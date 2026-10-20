package search

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
	"github.com/batchstream/weir/internal/value"
	"github.com/iceisfun/golua/budget"
)

// evaluateProgram runs Lua in the main process. A returned write carries the
// exact observed OCC condition; only a confirmed conflict permits reevaluation.
func (a *Adapter) evaluateProgram(ctx context.Context, work *execution.Plan, current *getReply) (write *plan, result *pb.MutationResult) {
	var allocationErr error
	defer func() {
		if allocationErr != nil {
			write = nil
			failure := fmt.Errorf("%w: %w", execution.ErrExecutionMemory, allocationErr)
			result = execution.LuaFailure(ctx, failure)
		}
	}()
	defer budget.Recover(&allocationErr)
	native := work.Backend.(*plan)
	inputBytes := []byte(nil)
	if native.input != nil {
		inputBytes = native.input.Data
	}
	decodedWorking, err := programWorking(jsonDecodeWorking(current.Source), jsonDecodeWorking(inputBytes))
	if err != nil {
		return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
	}
	if err := reserveSearchScratch(ctx, decodedWorking); err != nil {
		return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
	}
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
	luaBytes, programScratch := 0, decodedWorking
	if memory := searchMemory(ctx); memory != nil && memory.owner != nil {
		program.Reserve = func(bytes int) error {
			working, err := programWorking(programScratch, bytes)
			if err != nil {
				return err
			}
			if err := reserveSearchScratch(ctx, working); err != nil {
				return err
			}
			luaBytes = bytes
			return nil
		}
	}
	if native.input != nil {
		input, err := value.DecodeJSON(ctx, native.input.Data)
		if err != nil {
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid JSON transform input"))
		}
		program.Input = input
	}
	a.metrics.initialize()
	started := time.Now()
	transformed, err := luaengine.Evaluate(ctx, program)
	a.metrics.observeLua(time.Since(started), err)
	if err != nil {
		return nil, execution.LuaFailure(ctx, err)
	}
	luaBytes = transformed.Allocations.UsedBytes()
	next := *native
	switch transformed.Action {
	case "keep":
		return nil, protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
	case "reject":
		if work.Memory != nil {
			retained, err := programWorking(len(transformed.Message), execution.ResultOverheadBytes)
			if err != nil {
				return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
			}
			if err := work.Memory.ReserveResult(retained); err != nil {
				return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
			}
		}
		failure := protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, transformed.Message)
		return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
	case "delete":
		if !*current.Found {
			return nil, protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
		}
		next.action = "delete"
	case "replace":
		valueBytes, err := transformed.ValueBytes(ctx, transformed.Value, true)
		if err != nil {
			return nil, execution.LuaFailure(ctx, err)
		}
		programScratch, err = programWorking(decodedWorking, valueBytes, valueBytes, valueBytes)
		if err != nil {
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
		}
		working, err := programWorking(programScratch, luaBytes)
		if err != nil {
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
		}
		if err := reserveSearchScratch(ctx, working); err != nil {
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
		}
		if err := luaengine.ReserveValueValidation(transformed.Allocations, transformed.Value); err != nil {
			return nil, execution.LuaFailure(ctx, err)
		}
		source, err := value.EncodeJSON(ctx, transformed.Value)
		if err != nil {
			if ctx.Err() != nil {
				return nil, execution.LuaFailure(ctx, err)
			}
			failure := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "Lua replacement cannot be encoded as JSON")
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
		}
		if *current.Found {
			if err := luaengine.ReserveValueValidation(transformed.Allocations, currentValue); err != nil {
				return nil, execution.LuaFailure(ctx, err)
			}
			previous, encodeErr := value.EncodeJSON(ctx, currentValue)
			if encodeErr != nil {
				return nil, execution.LuaFailure(ctx, encodeErr)
			}
			if bytes.Equal(source, previous) {
				return nil, protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
			}
		}
		if memory := searchMemory(ctx); memory != nil {
			sources, err := programWorking(memory.sources, cap(source))
			if err != nil {
				return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
			}
			memoryErr := memory.set(ctx, sources, memory.frame, max(0, memory.scratch-cap(source)))
			if memoryErr != nil {
				return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
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
	write = &next
	return write, nil
}
