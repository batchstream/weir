package search

import (
	"context"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/value"
	"google.golang.org/protobuf/proto"
)

func (a *Adapter) PrepareCommand(ctx context.Context, id uint64, input *pb.Command) (*execution.Plan, *pb.Failure) {
	if ctx.Err() != nil {
		return nil, protocol.ContextFailure(ctx)
	}
	if id == 0 {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid command index")
	}
	if err := protocol.ValidateCommand(input); err != nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, err.Error())
	}
	if input.GetNative() == nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "Native operation required")
	}
	work, failure := a.prepareNative(input.GetNative())
	if failure != nil {
		return nil, failure
	}
	if ctx.Err() != nil {
		return nil, protocol.ContextFailure(ctx)
	}
	work.Command = input
	work.ID = id
	work.Bytes = max(work.Bytes, 2*proto.Size(input)+4096)
	work.WorkingBytes = max(work.WorkingBytes, 24<<20)
	return work, nil
}

func (a *Adapter) PrepareRecord(ctx context.Context, record *execution.Record) (*execution.Plan, *pb.Failure) {
	if ctx.Err() != nil {
		return nil, protocol.ContextFailure(ctx)
	}
	if record == nil || record.Command() == nil || record.StoreName() != a.config.Store {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid or wrong-Store record")
	}
	work, failure := a.prepareRecord(ctx, record)
	if failure != nil {
		return nil, failure
	}
	work.ID = record.Index()
	// Known input/framing copies add up; response growth is approved before allocation.
	work.IOReserve = 64 << 10
	work.WorkingBytes = 2*record.Bytes() + execution.EntryOverheadBytes
	native := work.Backend.(*plan)
	if work.Command.GetMutate() != nil {
		// The Runtime approves the whole normal ACK peak before it dispatches
		// this mutation, allowing its existing queue to wait for initial IO.
		work.WorkingBytes += bulkAckWorking(native.id)
		work.IOReserve += bulkAckEnvelopeWorking
	}
	work.BatchKey = "records"
	if native.waitUntilVisible {
		work.BatchKey += "|visible"
	}
	return work, nil
}

func (a *Adapter) Execute(ctx context.Context, works []*execution.Plan, emit execution.Emit) {
	if len(works) == 0 {
		return
	}
	work := works[0]
	if work.Command.GetRead() != nil || work.Command.GetMutate() != nil {
		results := a.executeRecords(ctx, works)
		for _, entry := range works {
			if program := entry.Backend.(*plan).program; program != nil {
				program.Source = ""
				program.Input = value.Value{Kind: value.Missing}
			}
		}
		if work.Memory != nil {
			_ = work.Memory.SetWorking(ctx, 0)
		}
		for i, result := range results {
			_ = emit(works[i], result)
		}
		return
	}
	end := a.executeNative(ctx, work, emit)
	// Response headers, body buffers and the HTTP exchange are now released.
	// Keep only terminal bookkeeping while the admitted input stays charged.
	work.WorkingBytes = execution.EntryOverheadBytes
	value := &pb.Event_NativeEnd{NativeEnd: end}
	event := &pb.Event{Value: value}
	_ = emit(work, event)
}
