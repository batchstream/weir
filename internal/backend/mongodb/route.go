package mongodb

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
	if ctx.Err() != nil {
		return nil, protocol.ContextFailure(ctx)
	}
	if failure != nil {
		return nil, failure
	}
	work.Command = input
	work.ID = id
	work.Bytes = max(work.Bytes, 2*proto.Size(input)+4096)
	work.WorkingBytes = max(work.WorkingBytes, a.nativeReplyBytes()+2*len(input.GetNative().Request.Data)+8192)
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
	if ctx.Err() != nil {
		return nil, protocol.ContextFailure(ctx)
	}
	if failure != nil {
		return nil, failure
	}
	work.ID = record.Index()
	// One batch owns one driver reply, while known input/frame copies add up.
	work.IOReserve = a.nativeReplyBytes()
	work.WorkingBytes = 2*record.Bytes() + execution.EntryOverheadBytes
	native := work.Backend.(*plan)
	work.BatchKey = "records"
	if native.action == "read" || native.program != nil {
		work.BatchKey = native.target.String()
	}
	if native.program != nil {
		work.BatchKey += "/lua"
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
			_ = work.Memory.SetWorking(0)
		}
		for i, result := range results {
			_ = emit(works[i], result)
		}
		return
	}
	end := a.executeNative(ctx, work, emit)
	// The response working set has left executeNative; only terminal bookkeeping
	// remains while the caller receives NativeEnd. The admitted input stays charged.
	work.WorkingBytes = execution.EntryOverheadBytes
	value := &pb.Event_NativeEnd{NativeEnd: end}
	event := &pb.Event{Value: value}
	_ = emit(work, event)
}
