package mongodb

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"google.golang.org/protobuf/proto"
)

func (a *Adapter) PrepareCommand(id uint64, input *pb.Command) (*execution.Plan, *pb.Failure) {
	if id == 0 || proto.Size(input) > protocol.MaxFrame {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid ID or Command size")
	}
	call, failure := execution.NormalizeCommand(input, a.config.Store)
	if failure != nil {
		return nil, failure
	}
	var work *execution.Plan
	switch {
	case call.GetScan() != nil:
		work, failure = a.prepareScan(call.GetScan())
	case call.GetNative() != nil:
		native := call.GetNative()
		if len(native.Body) > NativeCommandLimit {
			return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "native input exceeds adapter bound")
		}
		work, failure = a.prepareNative(native.Open)
	}
	if failure != nil {
		return nil, failure
	}
	if work == nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "missing operation")
	}
	work.CleanupRequired = call.GetScan() != nil
	work.Streaming = call.GetNative() != nil
	work.Command = call
	work.ID = id
	work.Bytes = max(work.Bytes, 2*proto.Size(input)+4096)
	work.WorkingBytes = max(work.WorkingBytes, 24<<20)
	return work, nil
}

func (a *Adapter) PrepareRecord(record *execution.Record) (*execution.Plan, *pb.Failure) {
	if record == nil || record.Operation() == nil || record.StoreName() != a.config.Store {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid or wrong-Store record")
	}
	work, failure := a.prepareRecord(record)
	if failure != nil {
		return nil, failure
	}
	operation := record.Operation()
	work.ID = operation.Index
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
	if work.Operation != nil {
		results, feedback := a.executeRecords(ctx, works)
		for i, result := range results {
			output := &execution.Output{Result: result}
			_ = emit(works[i], output)
		}
		return feedback
	}
	if work.Command.GetScan() != nil {
		return a.streamScan(ctx, work, emit)
	}
	native := work.Command.GetNative()
	source := io.NopCloser(bytes.NewReader(native.Body))
	sink := &eventSink{work: work, emit: emit}
	exchange := &execution.NativeExchange{Source: source, Sink: sink}
	end, feedback := a.executeNative(ctx, work, exchange)
	_ = source.Close()
	value := &pb.Event_NativeEnd{NativeEnd: end}
	event := &pb.Event{Version: 1, Value: value}
	output := &execution.Output{Event: event}
	_ = emit(work, output)
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
		event := &pb.Event{Version: 1, Value: value}
		output := &execution.Output{Event: event}
		if err := emit(work, output); err != nil {
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
		event := &pb.Event{Version: 1, Value: value}
		output := &execution.Output{Event: event}
		_ = emit(work, output)
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

type eventSink struct {
	work *execution.Plan
	emit execution.Emit
}

func (s *eventSink) Interrupt() {}
func (s *eventSink) Head(head *pb.NativeHead) error {
	value := &pb.Event_Head{Head: head}
	event := &pb.Event{Version: 1, Value: value}
	output := &execution.Output{Event: event}
	return s.emit(s.work, output)
}
func (s *eventSink) Chunk(chunk []byte) error {
	value := &pb.Event_Chunk{Chunk: append([]byte(nil), chunk...)}
	event := &pb.Event{Version: 1, Value: value}
	output := &execution.Output{Event: event}
	return s.emit(s.work, output)
}
