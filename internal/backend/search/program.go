package search

import (
	"bytes"
	"context"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/value"
)

// evaluateProgram runs Lua in the main process. A returned write carries the
// exact observed OCC condition; only a confirmed conflict permits reevaluation.
func (a *Adapter) evaluateProgram(ctx context.Context, work *execution.Plan, current *getReply) (*plan, *pb.MutationResult) {
	native := work.Backend.(*plan)
	currentValue := value.Value{Kind: value.Missing}
	if *current.Found {
		var err error
		currentValue, err = value.DecodeJSON(ctx, current.Source)
		if err != nil {
			if ctx.Err() != nil {
				return nil, execution.LuaFailure(ctx, err)
			}
			failure := protocol.Fail(pb.FailureCode_UNSUPPORTED, "stored JSON source cannot be transformed losslessly")
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
		}
	}
	if native.program.ObservedAt.IsZero() {
		native.program.ObservedAt = time.Now()
	}
	program := *native.program
	program.Current = currentValue
	if native.input != nil {
		input, err := value.DecodeJSON(ctx, native.input.Data)
		if err != nil {
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid JSON transform input"))
		}
		program.Input = input
	}
	transformed, err := a.lua.Evaluate(ctx, program)
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
		source, err := value.EncodeJSON(ctx, transformed.Value)
		if err != nil {
			if ctx.Err() != nil {
				return nil, execution.LuaFailure(ctx, err)
			}
			failure := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "Lua replacement cannot be encoded as JSON")
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
		}
		if *current.Found {
			previous, encodeErr := value.EncodeJSON(ctx, currentValue)
			if encodeErr != nil {
				return nil, execution.LuaFailure(ctx, encodeErr)
			}
			if bytes.Equal(source, previous) {
				return nil, protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
			}
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
	return &next, nil
}
