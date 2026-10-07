package search

import (
	"context"
	"strings"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
	"github.com/batchstream/weir/internal/value"
)

const programAttempts = 5

// evaluateProgram runs Lua in the main process. A returned write carries the
// exact observed OCC condition; only a confirmed conflict permits reevaluation.
func evaluateProgram(ctx context.Context, native *plan, current *getReply) (*plan, *pb.MutationResult) {
	currentValue := value.Value{Kind: value.Missing}
	if *current.Found {
		var err error
		currentValue, err = value.DecodeJSON(current.Source)
		if err != nil {
			failure := protocol.Fail(pb.FailureCode_UNSUPPORTED, "stored JSON source cannot be transformed losslessly")
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
		}
	}
	if native.program.ObservedAt.IsZero() {
		native.program.ObservedAt = time.Now()
	}
	program := *native.program
	program.Current = currentValue
	transformed, err := luaengine.Evaluate(ctx, program)
	if err != nil {
		return nil, execution.LuaFailure(ctx, err)
	}
	next := *native
	switch transformed.Action {
	case "keep":
		return nil, protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
	case "reject":
		failure := protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, transformed.Message)
		return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
	case "delete":
		if !*current.Found {
			return nil, protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
		}
		next.action = "delete"
	case "replace":
		if !safeProgramSource(transformed.Value) {
			failure := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "Lua replacement contains unsupported Search fields")
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
		}
		source, err := value.EncodeJSON(transformed.Value)
		if err != nil || len(source) > protocol.MaxDocument {
			failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Lua replacement exceeds Search source limits")
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
		}
		next.source = source
		next.action = "create"
		if *current.Found {
			next.action = "index"
		}
	default:
		failure := protocol.Fail(pb.FailureCode_INTERNAL, "Lua evaluation returned an invalid action")
		return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
	}
	write := &next
	return write, nil
}

func safeProgramSource(v value.Value) bool {
	if v.Kind == value.Object {
		for _, field := range v.Fields {
			if field.Name == "" || len(field.Name) > 1024 || strings.ContainsAny(field.Name, ".\x00") {
				return false
			}
			switch field.Name {
			case "_id", "_index", "_routing", "_version", "_seq_no", "_primary_term":
				return false
			}
			if !safeProgramSource(field.Value) {
				return false
			}
		}
	}
	for _, item := range v.Items {
		if !safeProgramSource(item) {
			return false
		}
	}
	return v.Kind != value.Missing && v.Kind != value.Bytes && (v.Kind != value.Extended || value.IsJSONNumber(v))
}
