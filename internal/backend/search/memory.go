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

func programWorking(parts ...int) (int, error) {
	working := 0
	for _, bytes := range parts {
		if bytes < 0 || bytes > math.MaxInt-working {
			return 0, execution.ErrExecutionMemory
		}
		working += bytes
	}
	return working, nil
}

// Empty index/ID strings are expanded by their worst JSON escaping below. The
// longest supported action/result and normal optional metadata cover both
// typeless Elasticsearch and OpenSearch replies. All per-item numeric evidence
// is nonnegative; elapsed times are decoded as unconstrained int64 values.
const bulkAckItemShape = `{"delete":{"_index":"","_id":"","_type":"_doc","status":404,"result":"not_found","_version":9223372036854775807,"_seq_no":9223372036854775807,"_primary_term":9223372036854775807,"forced_refresh":false,"_shards":{"total":9223372036854775807,"successful":9223372036854775807,"failed":9223372036854775807}}}`
const bulkAckEnvelopeShape = `{"errors":false,"took":-9223372036854775808,"ingest_took":-9223372036854775808,"items":[]}`

// jsonDecodeWorking contributes 3*wire + 1024 per structural node + 8192.
// A chunked body retains less than 2*wire (or the minimum 4096), while its
// old/new growth peak is less than 3*wire. Thus 5*wire covers both phases.
// The outer shape has 9 nodes. Its fixed costs are charged once by IOReserve,
// beside the existing 64 KiB normal header workspace.
const bulkAckEnvelopeWorking = 8192 + 4096 + 5*len(bulkAckEnvelopeShape) + 9*1024

func bulkAckWorking(id string) int {
	// Each item has 28 nodes and at most one array separator. The physical
	// index can be longer than the submitted alias; validIndex allows 255
	// bytes. The ID is already checked to at most 512 bytes. Six bytes cover
	// every escaped input byte without allocating a quoted string first.
	wire := len(bulkAckItemShape) + 1 + 6*(255+len(id))
	return 5*wire + 29*1024
}

// A batch retains pre-read sources and one framed request while its current
// exchange owns scratch. Final read documents transfer to their Ticket.
type recordMemory struct {
	owner   execution.Memory
	base    int
	reply   int
	sources int
	frame   int
	scratch int
}

func (m *recordMemory) set(ctx context.Context, sources, frame, scratch int) error {
	if m.base < 0 || m.reply < 0 || sources < 0 || frame < 0 || scratch < 0 {
		return execution.ErrExecutionMemory
	}
	held := max(m.base, frame)
	workspace := 0
	if m.reply == 64<<10 {
		// Pure reads can reuse their completed parser workspace for retained
		// sources. Keep the entire IO floor until all read exchanges finish;
		// a later exchange still approves sources plus its new scratch first.
		if sources > math.MaxInt-scratch {
			return execution.ErrExecutionMemory
		}
		workspace = max(m.reply, sources+scratch)
		if workspace > math.MaxInt-held {
			return execution.ErrExecutionMemory
		}
	} else {
		// Mutation sources cannot consume their future acknowledgement promise.
		workspace = max(m.reply, scratch)
		if sources > math.MaxInt-held || workspace > math.MaxInt-held-sources {
			return execution.ErrExecutionMemory
		}
		workspace += sources
	}
	if m.owner != nil {
		if err := m.owner.SetWorking(ctx, held+workspace); err != nil {
			return err
		}
	}
	m.sources, m.frame, m.scratch = sources, frame, max(m.reply, scratch)
	return nil
}

func searchMemory(ctx context.Context) *recordMemory {
	memory, _ := ctx.Value(recordMemoryContextKey).(*recordMemory)
	return memory
}

func reserveSearchScratch(ctx context.Context, bytes int) error {
	headerKey := nativeHeaderKey{}
	if header, _ := ctx.Value(headerKey).(*nativeHeaderMemory); header != nil {
		header.mu.Lock()
		if !header.completed && header.records != nil {
			err := header.setBody(bytes)
			header.mu.Unlock()
			return err
		}
		header.mu.Unlock()
	}
	if memory := searchMemory(ctx); memory != nil {
		return memory.set(ctx, memory.sources, memory.frame, bytes)
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
