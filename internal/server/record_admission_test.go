package server

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rejectingAdapter refuses to prepare one resource.
type rejectingAdapter struct {
	*recordAdapter
	rejected string
}

func (a *rejectingAdapter) PrepareRecord(ctx context.Context, record *execution.Record) (*execution.Plan, *pb.Failure) {
	if record.Command().GetMutate().GetResource() == a.rejected {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "resource rejected by the fixture")
	}
	return a.recordAdapter.PrepareRecord(ctx, record)
}

func putFrame(index uint64, resource string) *pb.ExecuteRequest {
	document := &pb.Document{ContentType: "application/json", Data: []byte(`{}`)}
	action := &pb.MutateRequest_Put{Put: document}
	mutation := &pb.MutateRequest{Resource: resource, Action: action}
	operation := &pb.Command_Mutate{Mutate: mutation}
	command := &pb.Command{Operation: operation}
	request := &pb.ExecuteRequest{StoreName: "records", Index: index, Command: command}
	return request
}

// recordResults sends requests on one stream and returns its results by index.
func recordResults(t *testing.T, client pb.StoreServiceClient, requests []*pb.ExecuteRequest) map[uint64]*pb.MutationResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := testutil.Execute(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range requests {
		if err := stream.Send(request); err != nil {
			t.Fatal(err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	results := make(map[uint64]*pb.MutationResult)
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			return results
		}
		if err != nil {
			t.Fatal("stream ended instead of reporting the record", err)
		}
		results[response.Index] = response.Event.GetMutationResult()
	}
}

func TestRecordStreamRejectedRecordFailsAloneAndTheStreamContinues(t *testing.T) {
	adapter := &rejectingAdapter{recordAdapter: newRecordAdapter("records"), rejected: "records/s:bad"}
	opts := routeAcceptanceNodeOptions{adapter: adapter}
	node := startRouteAcceptanceNode(t, opts)
	client := routeAcceptanceClient(t, node.address)
	requests := []*pb.ExecuteRequest{putFrame(1, "records/s:first"), putFrame(2, "records/s:bad"), putFrame(3, "records/s:last")}
	results := recordResults(t, client, requests)
	rejected := results[2]
	if len(results) != 3 || results[1].GetOutcome() != pb.MutationOutcome_APPLIED || results[3].GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("a rejected record affected its neighbours", results)
	}
	if rejected.GetOutcome() != pb.MutationOutcome_NOT_STARTED || rejected.GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT {
		t.Fatal("rejected record lost its own failure", rejected)
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
}

func TestRecordStreamRecordTimeoutExpiresOnlyItsQueuedRecord(t *testing.T) {
	gate := make(chan struct{})
	adapter := newRecordAdapter("records")
	adapter.block = gate
	limits := store.DefaultLimits()
	limits.BatchOperations, limits.MaxInFlight = 1, 1
	opts := routeAcceptanceNodeOptions{adapter: adapter, store: limits}
	node := startRouteAcceptanceNode(t, opts)
	client := routeAcceptanceClient(t, node.address)
	bounded := putFrame(2, "records/s:bounded")
	bounded.TimeoutMicros = protocol.TimeoutMicros(50 * time.Millisecond)
	requests := []*pb.ExecuteRequest{putFrame(1, "records/s:running"), bounded, putFrame(3, "records/s:waiting")}
	// The first record holds the only execution slot until the bounded one expires.
	time.AfterFunc(500*time.Millisecond, func() { close(gate) })
	results := recordResults(t, client, requests)
	expired := results[2]
	if expired.GetOutcome() != pb.MutationOutcome_NOT_STARTED || expired.GetFailure().GetCode() != pb.FailureCode_DEADLINE_EXCEEDED {
		t.Fatal("queued record outlived its timeout", expired)
	}
	if results[1].GetOutcome() != pb.MutationOutcome_APPLIED || results[3].GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("another record's timeout affected unbounded records", results)
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
}

func cancelFrame(index uint64) *pb.ExecuteRequest {
	request := &pb.ExecuteRequest{StoreName: "records", CancelIndex: index}
	return request
}

func TestRecordStreamCancelReleasesOnlyItsRecordEvenWithAFullWindow(t *testing.T) {
	gate := make(chan struct{})
	adapter := newRecordAdapter("records")
	adapter.block = gate
	limits := DefaultLimits()
	limits.Stall, limits.recordWindow = 5*time.Second, 2
	storeLimits := store.DefaultLimits()
	storeLimits.BatchOperations, storeLimits.MaxInFlight = 1, 1
	opts := routeAcceptanceNodeOptions{adapter: adapter, limits: limits, store: storeLimits}
	node := startRouteAcceptanceNode(t, opts)
	client := routeAcceptanceClient(t, node.address)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := testutil.Execute(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	send := func(request *pb.ExecuteRequest) {
		t.Helper()
		if err := stream.Send(request); err != nil {
			t.Fatal(err)
		}
	}
	receive := func() *pb.ExecuteResponse {
		t.Helper()
		response, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	// Record 1 holds the only execution slot and record 2 fills the window.
	send(putFrame(1, "records/s:running"))
	<-adapter.seen
	send(putFrame(2, "records/s:queued"))
	send(cancelFrame(2))
	queued := receive()
	if queued.Index != 2 || queued.Event.GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_STARTED || queued.Event.GetMutationResult().GetFailure().GetCode() != pb.FailureCode_CANCELLED {
		t.Fatal("canceled queued record was not released", queued)
	}
	send(cancelFrame(1))
	running := receive()
	if running.Index != 1 || running.Event.GetMutationResult().GetFailure().GetCode() != pb.FailureCode_CANCELLED {
		t.Fatal("canceled running record kept executing", running)
	}
	close(gate)
	send(putFrame(3, "records/s:after"))
	send(cancelFrame(1))
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	after := receive()
	if after.Index != 3 || after.Event.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("cancellation disturbed a later record", after)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatal("stream did not end cleanly", err)
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
}

func TestRecordStreamCancelMustNameAnEarlierRecord(t *testing.T) {
	opts := routeAcceptanceNodeOptions{adapter: newRecordAdapter("records")}
	node := startRouteAcceptanceNode(t, opts)
	client := routeAcceptanceClient(t, node.address)
	for name, requests := range map[string][]*pb.ExecuteRequest{
		"first":  {cancelFrame(1)},
		"unsent": {putFrame(1, "records/s:first"), cancelFrame(2)},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			stream, err := testutil.Execute(ctx, client)
			if err != nil {
				t.Fatal(err)
			}
			for _, request := range requests {
				if err := stream.Send(request); err != nil {
					t.Fatal(err)
				}
			}
			results := 0
			for {
				response, err := stream.Recv()
				if err == nil {
					results++
					if response.Index != 1 {
						t.Fatal("unexpected result", response)
					}
					continue
				}
				if status.Code(err) != codes.InvalidArgument || results != len(requests)-1 {
					t.Fatal("invalid cancel request accepted or erased admitted results", err, results)
				}
				break
			}
		})
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
}

func TestRecordStreamCancelIsReadWhileTheStoreQueueIsFull(t *testing.T) {
	gate := make(chan struct{})
	adapter := newRecordAdapter("records")
	adapter.block = gate
	limits := store.DefaultLimits()
	limits.BatchOperations, limits.MaxInFlight, limits.QueueOperations = 1, 1, 1
	opts := routeAcceptanceNodeOptions{adapter: adapter, store: limits}
	node := startRouteAcceptanceNode(t, opts)
	client := routeAcceptanceClient(t, node.address)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := testutil.Execute(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(putFrame(1, "records/s:running")); err != nil {
		t.Fatal(err)
	}
	<-adapter.seen
	// Record 2 fills the Store queue, record 3 waits for space and record 4
	// waits behind it to be queued.
	requests := []*pb.ExecuteRequest{putFrame(2, "records/s:queued"), putFrame(3, "records/s:waiting"), putFrame(4, "records/s:behind"), cancelFrame(4), cancelFrame(2)}
	for _, request := range requests {
		if err := stream.Send(request); err != nil {
			t.Fatal(err)
		}
	}
	canceled := make(map[uint64]bool)
	for range 2 {
		response, err := stream.Recv()
		failure := response.GetEvent().GetMutationResult().GetFailure()
		if err != nil || response.Index != 2 && response.Index != 4 || failure.GetCode() != pb.FailureCode_CANCELLED {
			t.Fatal("cancel request waited behind a record blocked on the Store queue", response, err)
		}
		canceled[response.Index] = true
	}
	if len(canceled) != 2 {
		t.Fatal("a canceled record reported twice", canceled)
	}
	close(gate)
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	results := make(map[uint64]pb.MutationOutcome)
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		results[response.Index] = response.Event.GetMutationResult().GetOutcome()
	}
	if len(results) != 2 || results[1] != pb.MutationOutcome_APPLIED || results[3] != pb.MutationOutcome_APPLIED {
		t.Fatal("cancellation disturbed other records", results)
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
}
