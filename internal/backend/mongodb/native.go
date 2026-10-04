package mongodb

import (
	"context"
	"errors"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"google.golang.org/protobuf/proto"
)

const NativeCommandLimit = 4 << 20

const NativeResponseLimit = 4 << 20

func (a *Adapter) prepareNative(request *pb.NativeRequest) (*execution.Plan, *pb.Failure) {
	if f := protocol.ValidateNative(request); f != nil {
		return nil, f
	}
	parts, _ := protocol.ParseRelativeResource(request.Resource)
	if len(parts) != 2 || !validNamespace(parts) {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid MongoDB Native target")
	}
	if request.Request.ContentType != "application/bson" {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "MongoDB Native requires application/bson")
	}

	target := namespace{database: parts[0], collection: parts[1]}
	p := &execution.Plan{
		Backend:      target,
		Singleton:    true,
		Key:          request.Resource,
		Bytes:        proto.Size(request) + execution.EntryOverheadBytes,
		ResultBytes:  protocol.NativeChunk + execution.ResultOverheadBytes,
		WorkingBytes: scanPageBudget,
	}
	return p, nil
}

func (a *Adapter) nativeCommand(raw []byte, namespace namespace) *pb.Failure {
	invalid := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid or excessive ordered BSON command")
	nodes := 65536
	if len(raw) > NativeCommandLimit {
		return protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Native BSON command limit")
	}
	if !validScanBSON(raw, 0, &nodes) {
		return invalid
	}
	if !nativeNoCode(raw) {
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "Native JavaScript expressions unsupported")
	}
	fields, err := scanFields(raw)
	if err != nil || len(fields) == 0 {
		return invalid
	}
	elements, err := bson.Raw(raw).Elements()
	if err != nil {
		return invalid
	}
	command := elements[0].Key()
	if command != "count" && command != "findAndModify" {
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "unsupported Native Mongo command")
	}
	target, ok := elements[0].Value().StringValueOK()
	if !ok || target != namespace.collection {
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "Native command target differs from resource")
	}
	for key, value := range fields {
		if key == command {
			continue
		}
		allowed := false
		switch key {
		case "query":
			allowed = value.Type == bson.TypeEmbeddedDocument
		case "sort", "fields", "update":
			allowed = command == "findAndModify" && value.Type == bson.TypeEmbeddedDocument
		case "new", "upsert", "remove":
			allowed = command == "findAndModify" && value.Type == bson.TypeBoolean
		case "limit", "skip":
			allowed = command == "count" && (value.Type == bson.TypeInt32 || value.Type == bson.TypeInt64)
		case "hint":
			allowed = value.Type == bson.TypeString || value.Type == bson.TypeEmbeddedDocument
		}
		if !allowed {
			return protocol.Fail(pb.FailureCode_UNSUPPORTED, "unsupported Native Mongo option")
		}
	}
	// No aggregation pipeline, namespace-bearing stage, session or unacknowledged
	// write options exist in this profile. Query semantics remain MongoDB's.
	return nil
}

func (a *Adapter) executeNative(ctx context.Context, work *execution.Plan, emit execution.Emit) (*pb.NativeEnd, execution.Feedback) {
	raw := work.Command.GetNative().Request.Data
	target := work.Backend.(namespace)
	if f := a.nativeCommand(raw, target); f != nil {
		return protocol.NativeFailure(false, f), execution.Neutral
	}
	if ctx.Err() != nil {
		return protocol.NativeFailure(false, protocol.ContextFailure(ctx)), execution.Neutral
	}
	timeout := work.BackendTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	backendContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if failure, signal := a.qualifyTarget(backendContext, target); failure != nil {
		return protocol.NativeFailure(false, failure), signal
	}
	command := bson.Raw(raw)
	reply, err := a.client.Database(target.database).RunCommand(backendContext, command).Raw()
	failure := backendFailure(backendContext, err)
	signal := feedback(backendContext, err)
	// MongoDB retains one bounded raw reply. Once it is available, delivery uses
	// the caller's lifetime rather than charging output stalls to backend I/O.
	cancel()
	if err != nil && len(reply) == 0 {
		var native mongo.CommandError
		if errors.As(err, &native) {
			reply = native.Raw
		}
	}
	// Raw is the actual driver-retained wire response, including ok:0 and write
	// errors. Never reconstruct BSON from the Go error or normalize write effects.
	if len(reply) == 0 {
		return protocol.NativeFailure(true, failure), execution.Neutral
	}
	nodes := 65536
	fields, framingErr := scanFields(reply)
	envelopeOK := false
	commandOK := false
	switch fields["ok"].Type {
	case bson.TypeDouble:
		n := fields["ok"].Double()
		envelopeOK = n == 0 || n == 1
		commandOK = n == 1
	case bson.TypeInt32:
		n := fields["ok"].Int32()
		envelopeOK = n == 0 || n == 1
		commandOK = n == 1
	case bson.TypeInt64:
		n := fields["ok"].Int64()
		envelopeOK = n == 0 || n == 1
		commandOK = n == 1
	}
	if len(reply) > NativeResponseLimit || framingErr != nil || !envelopeOK || !validScanBSON(reply, 0, &nodes) {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "invalid or excessive Native BSON reply")), execution.Neutral
	}
	head := &pb.NativeHead{BodyContentType: "application/bson"}
	value := &pb.Event_Head{Head: head}
	event := &pb.Event{Value: value}
	if err := emit(work, event); err != nil {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "Native response delivery failed")), execution.Neutral
	}
	for len(reply) > 0 {
		n := min(len(reply), protocol.NativeChunk)
		value := &pb.Event_Chunk{Chunk: append([]byte(nil), reply[:n]...)}
		event := &pb.Event{Value: value}
		if err := emit(work, event); err != nil {
			return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "Native response delivery failed")), execution.Neutral
		}
		reply = reply[n:]
	}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	// Reuse known command-error congestion without interpreting native write effects.
	resultFeedback := execution.Neutral
	var commandFailure mongo.CommandError
	if errors.As(err, &commandFailure) && signal == execution.Congested {
		resultFeedback = execution.Congested
	} else if err == nil && commandOK && ctx.Err() == nil {
		// Record a complete command envelope without interpreting Native write
		// effects or asserting a mutation outcome.
		resultFeedback = execution.Completed
	}
	return end, resultFeedback
}

// Walk already validated bounded bytes, without materializing a query tree.
// This profile excludes server-side JavaScript rather than supplying a runtime.
func nativeNoCode(raw []byte) bool {
	rest := raw[4 : len(raw)-1]
	for len(rest) > 0 {
		element, tail, _ := bsoncore.ReadElement(rest)
		rest = tail
		key := element.Key()
		if key == "$where" || key == "$function" || key == "$accumulator" {
			return false
		}
		value := element.Value()
		switch value.Type {
		case bsoncore.TypeJavaScript, bsoncore.TypeCodeWithScope:
			return false
		case bsoncore.TypeEmbeddedDocument, bsoncore.TypeArray:
			if !nativeNoCode(value.Data) {
				return false
			}
		}
	}
	return true
}
