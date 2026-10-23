package search

import (
	"context"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func (a *Adapter) PrepareNative(ctx context.Context, input *pb.Command) (*execution.Plan, *pb.Failure) {
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
	if failure != nil {
		return nil, failure
	}
	if ctx.Err() != nil {
		return nil, protocol.ContextFailure(ctx)
	}
	work.Command = input
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
	work.BatchKey = "records"
	if work.Backend.(*plan).waitUntilVisible {
		work.BatchKey += "|visible"
	}
	return work, nil
}

func (a *Adapter) ExecuteRecords(ctx context.Context, works []*execution.Plan) []*pb.Event {
	return a.executeRecords(ctx, works)
}

func (a *Adapter) ExecuteNative(ctx context.Context, work *execution.Plan, emit execution.Emit) *pb.NativeEnd {
	return a.executeNative(ctx, work, emit)
}
