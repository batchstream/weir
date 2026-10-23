package mongodb

import (
	"context"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func (a *Adapter) PrepareNative(ctx context.Context, input *pb.Command) (*execution.Plan, *pb.Failure) {
	ctx, release := a.bindLifetime(ctx)
	defer release()
	if ctx.Err() != nil {
		return nil, protocol.ContextFailure(ctx)
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
	return work, nil
}

func (a *Adapter) PrepareRecord(ctx context.Context, record *execution.Record) (*execution.Plan, *pb.Failure) {
	ctx, release := a.bindLifetime(ctx)
	defer release()
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

func (a *Adapter) ExecuteRecords(ctx context.Context, works []*execution.Plan) []*pb.Event {
	ctx, release := a.bindLifetime(ctx)
	defer release()
	return a.executeRecords(ctx, works)
}

func (a *Adapter) ExecuteNative(ctx context.Context, work *execution.Plan, emit execution.Emit) *pb.NativeEnd {
	ctx, release := a.bindLifetime(ctx)
	defer release()
	return a.executeNative(ctx, work, emit)
}
