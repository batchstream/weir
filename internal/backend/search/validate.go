package search

import (
	"context"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/value"
)

func (a *Adapter) validateRecord(ctx context.Context, work *execution.Plan) *pb.Failure {
	if ctx.Err() != nil {
		return protocol.ContextFailure(ctx)
	}
	native := work.Backend.(*plan)
	if native.validated || work.Command.GetRead() != nil {
		return nil
	}
	mutation := work.Command.GetMutate()
	raw := native.source
	if transform := mutation.GetAtomicTransform(); transform != nil && transform.GetLua() != nil {
		if input := transform.GetLua().Input; input != nil {
			raw = input.Data
		}
	}
	if transform := mutation.GetAtomicTransform(); transform != nil {
		if program := transform.GetLua(); program != nil {
			if program.Input != nil {
				if validateJSON(ctx, raw) != nil {
					if ctx.Err() != nil {
						return protocol.ContextFailure(ctx)
					}
					return protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid JSON transform input")
				}
				if _, err := value.DecodeJSON(ctx, raw); err != nil {
					if ctx.Err() != nil {
						return protocol.ContextFailure(ctx)
					}
					return protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid JSON transform input")
				}
			}
			native.program.Source = string(program.Source)
		} else if failure := prepareExpression(ctx, transform.GetBackendExpression()); failure != nil {
			return failure
		}
	} else if len(raw) != 0 && (!object(raw) || validateJSON(ctx, raw) != nil) {
		if ctx.Err() != nil {
			return protocol.ContextFailure(ctx)
		}
		return protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid JSON source")
	}
	if ctx.Err() != nil {
		return protocol.ContextFailure(ctx)
	}
	native.validated = true
	return nil
}
