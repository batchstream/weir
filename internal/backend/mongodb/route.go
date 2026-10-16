package mongodb

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
	if ctx.Err() != nil {
		return nil, protocol.ContextFailure(ctx)
	}
	if failure != nil {
		return nil, failure
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
	if ctx.Err() != nil {
		return nil, protocol.ContextFailure(ctx)
	}
	if failure != nil {
		return nil, failure
	}
	work.ID = record.Index()
	operation := record.Command()
	if operation.GetRead() != nil {
		work.WorkingBytes = 6 * a.documentBytes()
	} else {
		work.WorkingBytes = 24 << 20
	}
	native := work.Backend.(*plan)
	work.BatchKey = "records"
	if native.action == "read" || native.program != nil {
		work.BatchKey = native.target.String()
	}
	if native.program != nil {
		work.BatchKey += "/lua"
		work.WorkingBytes = programWorkingBytes
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
	value := &pb.Event_NativeEnd{NativeEnd: end}
	event := &pb.Event{Value: value}
	_ = emit(work, event)
}
