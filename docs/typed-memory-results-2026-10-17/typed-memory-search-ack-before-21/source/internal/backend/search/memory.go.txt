package search

import (
	"context"
	"math"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/value"
)

type recordMemoryKey uint8

const recordMemoryContextKey recordMemoryKey = 0

// A batch retains pre-read sources and one framed request while its current
// exchange owns scratch. Final read documents transfer to their Ticket.
type recordMemory struct {
	owner   execution.Memory
	base    int
	sources int
	frame   int
	scratch int
}

func (m *recordMemory) set(sources, frame, scratch int) error {
	if sources < 0 || frame < 0 || scratch < 0 || sources > math.MaxInt-max(m.base, frame) || scratch > math.MaxInt-max(m.base, frame)-sources {
		return execution.ErrExecutionMemory
	}
	if m.owner != nil {
		if err := m.owner.SetWorking(max(m.base, frame) + sources + scratch); err != nil {
			return err
		}
	}
	m.sources, m.frame, m.scratch = sources, frame, scratch
	return nil
}

func searchMemory(ctx context.Context) *recordMemory {
	memory, _ := ctx.Value(recordMemoryContextKey).(*recordMemory)
	return memory
}

func reserveSearchScratch(ctx context.Context, bytes int) error {
	if memory := searchMemory(ctx); memory != nil {
		return memory.set(memory.sources, memory.frame, bytes)
	}
	return nil
}

func recordMemoryFailure() *pb.Failure {
	return protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, execution.ErrExecutionMemory.Error())
}

// Strings are charged by bytes; containers and scalar slots are counted without
// building decoder tokens first. The per-node allowance covers Value/Field
// slices, JSON validation maps, token strings and geometric slice growth.
func jsonDecodeWorking(raw []byte) int {
	if len(raw) > (math.MaxInt-8192)/3 {
		return math.MaxInt
	}
	working := 3*len(raw) + 8192
	quoted, escaped := false, false
	for _, character := range raw {
		if quoted {
			if escaped {
				escaped = false
			} else if character == '\\' {
				escaped = true
			} else if character == '"' {
				quoted = false
			}
			continue
		}
		if character == '"' {
			quoted = true
		}
		if character == '{' || character == '[' || character == ',' || character == ':' {
			if working > math.MaxInt-1024 {
				return math.MaxInt
			}
			working += 1024
		}
	}
	return working
}

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
	if err := reserveSearchScratch(ctx, jsonDecodeWorking(raw)); err != nil {
		return recordMemoryFailure()
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

func jsonValueBytes(current value.Value) int {
	bytes := 6*len(current.Text) + len(current.Data) + 32
	for _, field := range current.Fields {
		bytes += 6*len(field.Name) + jsonValueBytes(field.Value)
	}
	for _, item := range current.Items {
		bytes += jsonValueBytes(item)
	}
	return bytes
}
