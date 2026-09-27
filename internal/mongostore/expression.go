package mongostore

import (
	"context"
	"errors"
	"math"
	"strings"
	"unicode/utf8"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/value"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const ExpressionMedia = "application/vnd.weir.mongodb-update.v1+bson"

func (a *Adapter) prepareExpression(d *pb.Document) *pb.Failure {
	if d == nil || d.MediaType != ExpressionMedia {
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "unsupported MongoDB expression profile")
	}
	invalid := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid or excessive MongoDB expression")
	if len(d.Data) > protocol.MaxExpression {
		return invalid
	}
	// Decode enforces byte, depth and node budgets incrementally, before each
	// allocation. The bounded value tree is discarded; original BSON is sent.
	doc, err := Decode(d.Data)
	if err != nil || len(doc.Fields) == 0 || !expressionValues(doc) {
		return invalid
	}
	paths := make(map[string]bool)
	for _, operator := range doc.Fields {
		if operator.Name != "$set" && operator.Name != "$unset" && operator.Name != "$inc" || operator.Value.Kind != value.Object {
			return invalid
		}
		for _, field := range operator.Value.Fields {
			if !expressionPath(field.Name) || len(paths) >= 128 {
				return invalid
			}
			if paths[field.Name] {
				return invalid
			}
			paths[field.Name] = true
			if operator.Name == "$inc" && !incrementOperand(field.Value) {
				return invalid
			}
		}
	}
	for path := range paths {
		parts := strings.Split(path, ".")
		for i := 1; i < len(parts); i++ {
			if paths[strings.Join(parts[:i], ".")] {
				return invalid
			}
		}
	}
	return nil
}

func expressionPath(path string) bool {
	if len(path) == 0 || len(path) > 1024 {
		return false
	}
	parts := strings.Split(path, ".")
	if len(parts) > 32 || parts[0] == "_id" {
		return false
	}
	for _, part := range parts {
		if part == "" || strings.ContainsAny(part, "$\x00") {
			return false
		}
		// Array indexes and numeric object keys are intentionally outside this
		// profile: existing container type must not change path interpretation.
		if part[0] >= '0' && part[0] <= '9' {
			return false
		}
	}
	return true
}

func expressionValues(v value.Value) bool {
	if v.Kind == value.Extended && v.Type == "mongodb.bson.regex.v1" {
		raw := bson.RawValue{Type: bson.TypeRegex, Value: v.Data}
		pattern, options := raw.Regex()
		if !utf8.ValidString(pattern) || !utf8.ValidString(options) {
			return false
		}
	}
	if v.Kind == value.Object {
		seen := make(map[string]bool, len(v.Fields))
		for _, f := range v.Fields {
			if seen[f.Name] || !expressionValues(f.Value) {
				return false
			}
			seen[f.Name] = true
		}
	}
	for _, child := range v.Items {
		if !expressionValues(child) {
			return false
		}
	}
	return true
}

func incrementOperand(v value.Value) bool {
	switch v.Kind {
	case value.Int32, value.Int64:
		return true
	case value.Float64:
		return !math.IsNaN(v.Float) && !math.IsInf(v.Float, 0)
	case value.Extended:
		if v.Type == "mongodb.bson.decimal128.v1" {
			raw := bson.RawValue{Type: bson.TypeDecimal128, Value: v.Data}
			decimal := raw.Decimal128()
			return !decimal.IsNaN() && decimal.IsInf() == 0
		}
	}
	return false
}

func (a *Adapter) executeExpression(ctx context.Context, p *execution.Plan) ([]*pb.BulkResult, execution.Feedback) {
	n := p.Backend.(*plan)
	filter := bson.D{{Key: "_id", Value: n.id}}
	update := bson.D{{Key: "q", Value: filter}, {Key: "u", Value: n.document}, {Key: "multi", Value: false}, {Key: "upsert", Value: false}}
	concern := bson.D{{Key: "w", Value: "majority"}}
	command := bson.D{{Key: "update", Value: a.config.Collection}, {Key: "updates", Value: bson.A{update}}, {Key: "ordered", Value: true}, {Key: "writeConcern", Value: concern}}
	// RunCommand has no ordinary retry policy. The only authenticated profile
	// selects explicit SCRAM-SHA-256, whose pinned Reauth always fails.
	raw, err := a.client.Database(a.config.Database).RunCommand(ctx, command).Raw()
	if len(raw) == 0 {
		var commandError mongo.CommandError
		if errors.As(err, &commandError) {
			raw = commandError.Raw
		}
	}
	result, sample := expressionReply(raw)
	if feedback(ctx, err) == execution.Congested {
		sample = execution.Congested
	}
	if len(raw) == 0 {
		result = protocol.Mutation(pb.MutationOutcome_UNKNOWN, backendFailure(ctx, err))
		sample = feedback(ctx, err)
	}
	variant := &pb.BulkResult_Mutation{Mutation: result}
	reply := &pb.BulkResult{Index: p.Operation.Index, Result: variant}
	return []*pb.BulkResult{reply}, sample
}

func expressionReply(raw bson.Raw) (*pb.MutationResult, execution.Feedback) {
	unknown := protocol.Mutation(pb.MutationOutcome_UNKNOWN, protocol.Fail(pb.FailureCode_UNAVAILABLE, "update acknowledgement unavailable or incomplete"))
	nodes := maxNodes
	if len(raw) > 64<<10 || !validScanBSON(raw, 0, &nodes) {
		return unknown, execution.Neutral
	}
	fields, err := scanFields(raw)
	if err != nil || fields["writeConcernError"].Type != 0 || fields["upserted"].Type != 0 {
		return unknown, execution.Neutral
	}
	if fields["code"].Type != 0 || fields["writeErrors"].Type != 0 {
		for _, key := range []string{"n", "nModified"} {
			if count, exists := fields[key]; exists {
				n, valid := expressionCount(count)
				if !valid || n != 0 {
					return unknown, execution.Neutral
				}
			}
		}
	}
	if !scanOK(fields["ok"]) {
		// Unknown command failures may follow execution; only known definitive
		// rejections narrow UNKNOWN. Never trust an HTTP/wire success alone.
		if !expressionZero(fields["ok"]) {
			return unknown, execution.Neutral
		}
		return expressionRejection(fields["code"], unknown)
	}
	if writes, exists := fields["writeErrors"]; exists {
		array, ok := writes.ArrayOK()
		if !ok {
			return unknown, execution.Neutral
		}
		values, err := array.Values()
		if err != nil || len(values) != 1 {
			return unknown, execution.Neutral
		}
		item, ok := values[0].DocumentOK()
		if !ok {
			return unknown, execution.Neutral
		}
		failure, err := scanFields(item)
		index, ok := expressionCount(failure["index"])
		if err != nil || !ok || index != 0 {
			return unknown, execution.Neutral
		}
		return expressionRejection(failure["code"], unknown)
	}
	for key := range fields {
		switch key {
		case "ok", "n", "nModified", "operationTime", "$clusterTime", "electionId", "opTime":
		default:
			return unknown, execution.Neutral
		}
	}
	matched, ok := expressionCount(fields["n"])
	modified, valid := expressionCount(fields["nModified"])
	if !ok || !valid || matched < 0 || matched > 1 || modified < 0 || modified > matched {
		return unknown, execution.Neutral
	}
	if matched == 0 {
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "record missing")), execution.Neutral
	}
	// Acknowledged n=1,nModified=0 is APPLIED, including an empty operator.
	return protocol.Mutation(pb.MutationOutcome_APPLIED, nil), execution.Healthy
}

func expressionCount(raw bson.RawValue) (int64, bool) {
	switch raw.Type {
	case bson.TypeInt32:
		return int64(raw.Int32()), true
	case bson.TypeInt64:
		return raw.Int64(), true
	}
	return 0, false
}

func expressionRejection(raw bson.RawValue, unknown *pb.MutationResult) (*pb.MutationResult, execution.Feedback) {
	code, ok := expressionCount(raw)
	if !ok {
		return unknown, execution.Neutral
	}
	switch code {
	case 112:
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_CONFLICT, "native update conflict")), execution.Neutral
	case 2, 14, 28, 40, 66, 121, 11000:
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "native update rejected")), execution.Neutral
	}
	return unknown, execution.Neutral
}

func expressionZero(raw bson.RawValue) bool {
	switch raw.Type {
	case bson.TypeDouble:
		return raw.Double() == 0
	case bson.TypeInt32:
		return raw.Int32() == 0
	case bson.TypeInt64:
		return raw.Int64() == 0
	}
	return false
}
