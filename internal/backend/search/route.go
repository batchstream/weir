package search

import (
	"context"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
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
	// Reserve initial parsing work from the scheduling window; larger
	// single records may grow beyond that window.
	work.WorkingBytes = 3 * a.options().ExchangeBytes
	native := work.Backend.(*plan)
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
