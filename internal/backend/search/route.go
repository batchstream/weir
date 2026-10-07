package search

import (
	"context"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"google.golang.org/protobuf/proto"
)

func (a *Adapter) PrepareCommand(id uint64, input *pb.Command) (*execution.Plan, *pb.Failure) {
	if id == 0 {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid command index")
	}
	if err := protocol.ValidateCommand(input); err != nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, err.Error())
	}
	var failure *pb.Failure
	var work *execution.Plan
	switch {
	case input.GetScan() != nil:
		work, failure = a.prepareScan(input.GetScan())
	case input.GetNative() != nil:
		work, failure = a.prepareNative(input.GetNative())
	}
	if failure != nil {
		return nil, failure
	}
	if work == nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "missing operation")
	}
	work.Command = input
	work.ID = id
	work.Bytes = max(work.Bytes, 2*proto.Size(input)+4096)
	work.WorkingBytes = max(work.WorkingBytes, 24<<20)
	return work, nil
}

func (a *Adapter) PrepareRecord(record *execution.Record) (*execution.Plan, *pb.Failure) {
	if record == nil || record.Command() == nil || record.StoreName() != a.config.Store {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid or wrong-Store record")
	}
	work, failure := a.prepareRecord(record)
	if failure != nil {
		return nil, failure
	}
	work.ID = record.Index()
	// One body, one decoded source set and parsing scratch are bounded by
	// the complete response cap, independent of the declared single record size.
	work.WorkingBytes = 3 * batchBodyLimit
	native := work.Backend.(*plan)
	work.BatchKey = native.index
	return work, nil
}

func (a *Adapter) Execute(ctx context.Context, works []*execution.Plan, emit execution.Emit) bool {
	if len(works) == 0 {
		return false
	}
	work := works[0]
	if work.Command.GetRead() != nil || work.Command.GetMutate() != nil {
		results := a.executeRecords(ctx, works)
		for i, result := range results {
			_ = emit(works[i], result)
		}
		return false
	}
	if work.Command.GetScan() != nil {
		return a.streamScan(ctx, work, emit)
	}
	end := a.executeNative(ctx, work, emit)
	value := &pb.Event_NativeEnd{NativeEnd: end}
	event := &pb.Event{Value: value}
	_ = emit(work, event)
	return false
}

func (a *Adapter) streamScan(ctx context.Context, work *execution.Plan, emit execution.Emit) bool {
	timeout := work.BackendTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	fetchContext, cancel := context.WithTimeout(ctx, timeout)
	page := a.fetchScan(fetchContext, work)
	cancel()
	state := work.Backend.(*scanPlan)
	continuation, transferred := state.PublishPage(ctx, work, page, emit)
	if transferred {
		state.transferred = true
	}
	return continuation
}

func (a *Adapter) ClosePlan(ctx context.Context, work *execution.Plan) *pb.Failure {
	if work.Command.GetScan() != nil {
		return a.closeScan(ctx, work)
	}
	return nil
}
