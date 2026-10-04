package store

import (
	"fmt"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestReadWindowsFollowDeclaredResultMemory(t *testing.T) {
	adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 1), maxReadBytes: protocol.MaxDocument}
	limits := DefaultLimits()
	runtime := newRuntime(adapter, limits)
	requests := make([]*pb.ReadRequest, protocol.MaxRecordFrameItems)
	for i := range requests {
		request := &pb.ReadRequest{Resource: fmt.Sprintf("records/s:%d", i)}
		requests[i] = request
	}
	batch := &pb.ReadBatch{Requests: requests}
	operation := &pb.Command_Read{Read: batch}
	command := &pb.Command{Operation: operation}
	resultBytes := protocol.MaxDocument + execution.ResultOverheadBytes
	maximumItems := min(limits.BatchOperations, (limits.ResultBytes/limits.Concurrency)/resultBytes)
	windows := 0
	for offset := 0; offset < len(requests); {
		prepared, count, failure := runtime.PrepareWindow("test", command, offset)
		if failure != nil || count != min(maximumItems, len(requests)-offset) {
			t.Fatal("configured maximum read size did not bound the next window", offset, count, failure)
		}
		if prepared.resultBytes != count*resultBytes || prepared.resultBytes > limits.ResultBytes/limits.Concurrency || prepared.bytes > limits.BatchBytes {
			t.Fatal("window exceeded its input or output allowance", prepared)
		}
		for position, plan := range prepared.plans {
			if plan.ID != uint64(position+1) || plan.Operation.Read != requests[offset+position] {
				t.Fatal("window lost its local ordinal or input association", position, plan)
			}
		}
		offset += count
		windows++
	}
	if windows <= 1 || runtime.Snapshot().Retained != 0 {
		t.Fatal("preparation retained the complete logical call", windows, runtime.Snapshot())
	}
}

func TestCompletedWindowKeepsResultCreditsUntilAcknowledged(t *testing.T) {
	adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 2), maxReadBytes: protocol.MaxDocument}
	limits := DefaultLimits()
	limits.ResultBytes = protocol.MaxDocument + execution.ResultOverheadBytes
	runtime := newRuntime(adapter, limits)
	firstRequest := &pb.ReadRequest{Resource: "records/s:first"}
	secondRequest := &pb.ReadRequest{Resource: "records/s:second"}
	batch := &pb.ReadBatch{Requests: []*pb.ReadRequest{firstRequest, secondRequest}}
	operation := &pb.Command_Read{Read: batch}
	command := &pb.Command{Operation: operation}
	first, count, failure := runtime.PrepareWindow("test", command, 0)
	if failure != nil || count != 1 {
		t.Fatal("one read did not fit its reserved output allowance", count, failure)
	}
	second, count, failure := runtime.PrepareWindow("test", command, 1)
	if failure != nil || count != 1 {
		t.Fatal("second read could not prepare independently", count, failure)
	}
	owner := submitCrossBatch(t, runtime, t.Context(), first)
	runtime.execute(selectCrossBatch(runtime))
	if results := crossResults(t, owner); len(results) != 1 || results[0].Read.GetDocument() == nil {
		t.Fatal("admitted read did not publish its result", results)
	}
	if snapshot := runtime.Snapshot(); snapshot.Active != 0 || snapshot.Ready != 1 || snapshot.ResultBytes != limits.ResultBytes {
		t.Fatal("completion released result ownership before consumption", snapshot)
	}
	if _, failure, _ := runtime.SubmitBatch(t.Context(), second); failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("slow owner allowed an additional result allocation", failure)
	}
	owner.Ack()
	next := submitCrossBatch(t, runtime, t.Context(), second)
	runtime.execute(selectCrossBatch(runtime))
	if results := crossResults(t, next); len(results) != 1 || results[0].Read.GetDocument() == nil {
		t.Fatal("returned result credits did not resume admission", results)
	}
	next.Ack()
	waitReleased(t, runtime)
}

func TestMutationWindowsFollowPreparedInputBytes(t *testing.T) {
	adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 1)}
	limits := DefaultLimits()
	limits.BatchBytes = protocol.MaxDocument + 4096
	runtime := newRuntime(adapter, limits)
	document := &pb.Document{MediaType: "application/octet-stream", Data: make([]byte, 1536<<10)}
	action := &pb.MutateRequest_Put{Put: document}
	first := &pb.MutateRequest{Resource: "records/s:first", Action: action}
	second := &pb.MutateRequest{Resource: "records/s:second", Action: action}
	batch := &pb.MutationBatch{Requests: []*pb.MutateRequest{first, second}}
	operation := &pb.Command_Mutate{Mutate: batch}
	command := &pb.Command{Operation: operation}
	for offset := range 2 {
		prepared, count, failure := runtime.PrepareWindow("test", command, offset)
		if failure != nil || count != 1 || prepared.bytes > limits.BatchBytes || prepared.resultBytes != execution.ResultOverheadBytes {
			t.Fatal("large mutations did not split at the prepared input bound", offset, count, prepared, failure)
		}
	}
}
