package mongodb

import (
	"math"
	"strings"
	"unicode/utf8"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/value"
	"go.mongodb.org/mongo-driver/v2/bson"
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
