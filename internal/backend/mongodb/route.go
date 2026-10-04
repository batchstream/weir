package mongodb

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
		native := input.GetNative()
		if len(native.GetMongodbCommand()) > NativeCommandLimit {
			return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "native input exceeds adapter bound")
		}
		work, failure = a.prepareNative(native)
	}
	if failure != nil {
		return nil, failure
	}
	if work == nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "missing operation")
	}
	work.CleanupRequired = input.GetScan() != nil
	work.Streaming = input.GetNative() != nil
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
	operation := record.Command()
	if operation.GetRead() != nil {
		work.WorkingBytes = a.readWorkingBytes()
	} else {
		work.WorkingBytes = 24 << 20
	}
	native := work.Backend.(*plan)
	work.BatchKey = native.target.String()
	if native.program != nil {
		work.BatchKey += "/lua"
		work.WorkingBytes = programWorkingBytes
	}
	return work, nil
}

func (a *Adapter) Execute(ctx context.Context, works []*execution.Plan, emit execution.Emit) execution.Feedback {
	if len(works) == 0 {
		return execution.Neutral
	}
	work := works[0]
	if work.Command.GetRead() != nil || work.Command.GetMutate() != nil {
		results, feedback := a.executeRecords(ctx, works)
		for i, result := range results {
			_ = emit(works[i], result)
		}
		return feedback
	}
	if work.Command.GetScan() != nil {
		return a.streamScan(ctx, work, emit)
	}
	end, feedback := a.executeNative(ctx, work, emit)
	value := &pb.Event_NativeEnd{NativeEnd: end}
	event := &pb.Event{Value: value}
	_ = emit(work, event)
	return feedback
}

func (a *Adapter) streamScan(ctx context.Context, work *execution.Plan, emit execution.Emit) execution.Feedback {
	work.Continue = false
	timeout := work.BackendTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	fetchContext, cancel := context.WithTimeout(ctx, timeout)
	page, feedback := a.fetchScan(fetchContext, work)
	cancel()
	state := work.Backend.(*scanPlan)
	for _, document := range page.Documents {
		value := &pb.Event_Document{Document: document}
		event := &pb.Event{Value: value}
		if err := emit(work, event); err != nil {
			page.Failure = protocol.Fail(pb.FailureCode_INTERNAL, "Scan result publication failed")
			if ctx.Err() != nil {
				page.Failure = protocol.ContextFailure(ctx)
			}
			break
		}
		state.count++
	}
	if page.Failure != nil || page.Exhausted || page.Complete {
		end := &pb.ScanEnd{DocumentCount: state.count, Failure: page.Failure}
		if page.Failure == nil {
			end.Exhausted = page.Exhausted
			end.NextContinuationToken = page.NextContinuationToken
		}
		value := &pb.Event_ScanEnd{ScanEnd: end}
		event := &pb.Event{Value: value}
		_ = emit(work, event)
	} else {
		work.Continue = true
	}
	return feedback
}

func (a *Adapter) ClosePlan(ctx context.Context, work *execution.Plan) *pb.Failure {
	if work.CleanupRequired {
		return a.closeScan(ctx, work)
	}
	return nil
}
