package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

func readFrame(index uint64, read *pb.ReadRequest) *pb.ExecuteRequest {
	command := &pb.Command{Operation: &pb.Command_Read{Read: read}}
	request := &pb.ExecuteRequest{StoreName: "records", Index: index, Command: command}
	return request
}
func TestRecordStreamLongInputWithLargeResults(t *testing.T) {
	adapter, local := peerLocal(t, "records")
	document := &pb.Document{ContentType: "application/octet-stream", Data: bytes.Repeat([]byte("x"), (2 << 20))}
	adapter.documents[testRequest().Resource] = document
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	stream, err := client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const count = 80
	first := readFrame(1, testRequest())
	if err := stream.Send(first); err != nil {
		t.Fatal(err)
	}
	for index := uint64(1); index <= count; index++ {
		response, err := stream.Recv()
		if err != nil || response.Index != index || len(response.Event.GetReadResult().GetDocument().GetData()) != (2<<20) {
			t.Fatal("lost indexed large result", index, response, err)
		}
		if index < count {
			request := readFrame(index+1, testRequest())
			if err := stream.Send(request); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatal("long stream did not complete", err)
	}
	waitPeerIdle(t, server)
}

func TestRecordStreamInvalidLaterFramePreservesPublishedResults(t *testing.T) {
	for _, scenario := range []string{"ordinal", "Store", "kind", "request"} {
		t.Run(scenario, func(t *testing.T) {
			adapter, local := peerLocal(t, "records")
			opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
			server, address := startPeerServer(t, opts)
			_, client := peerClient(t, address)
			stream, err := client.Execute(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			request := readFrame(1, testRequest())
			if err := stream.Send(request); err != nil {
				t.Fatal(err)
			}
			response, err := stream.Recv()
			if err != nil || response.Index != 1 || response.Event.GetReadResult().GetMissing() == nil {
				t.Fatal("first frame failed", response, err)
			}
			later := readFrame(2, testRequest())
			switch scenario {
			case "ordinal":
				later.Index = 3
			case "Store":
				later.StoreName = "other"
			case "kind":
				mutation := testMutation("later")
				later.Command.Operation = &pb.Command_Mutate{Mutate: mutation}
			case "request":
				later.Command.GetRead().Resource = "../bad"
			}
			if err := stream.Send(later); err != nil {
				t.Fatal(err)
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
			if response, err := stream.Recv(); response != nil || status.Code(err) != codes.InvalidArgument {
				t.Fatal("invalid later frame accepted", response, err)
			}
			if adapter.commands.Load() != 0 {
				t.Fatal("invalid later frame reached mutation execution")
			}
			waitPeerIdle(t, server)
		})
	}
}

func TestRecordStreamIndexedOperationFailureDoesNotStopLaterResults(t *testing.T) {
	adapter, local := peerLocal(t, "records")
	adapter.ackFailureKey = testRequest().Resource
	adapter.ackFailure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "acknowledgement unavailable")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	stream, err := client.Execute(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	items := []*pb.MutateRequest{testMutation("first"), testMutation("second")}
	items[1].Resource = "records/s:other"
	for i, mutation := range items {
		command := &pb.Command{Operation: &pb.Command_Mutate{Mutate: mutation}}
		request := &pb.ExecuteRequest{StoreName: "records", Index: uint64(i + 1), Command: command}
		if err := stream.Send(request); err != nil {
			t.Fatal(err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	for index := uint64(1); index <= 2; index++ {
		response, err := stream.Recv()
		if err != nil || response.Index != index {
			t.Fatal("indexed result missing", response, err)
		}
		result := response.Event.GetMutationResult()
		if result == nil || result.Outcome != pb.MutationOutcome_APPLIED || (result.Failure != nil) != (index == 1) {
			t.Fatalf("incorrect operation evidence at %d: %v", index, result)
		}
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	waitPeerIdle(t, server)
	for range 3 {
		families := testmetrics.Gather(t, local)
		applied := map[string]string{"operation": "mutate", "outcome": "applied"}
		if got := testmetrics.Sample(families, "weir_store_records_total", applied).GetCounter().GetValue(); got != 2 {
			t.Fatal("stream lost applied evidence", got)
		}
		mutate := map[string]string{"operation": "mutate"}
		if got := testmetrics.Sample(families, "weir_store_record_failures_total", mutate).GetCounter().GetValue(); got != 1 {
			t.Fatal("OK stream must still count APPLIED with Failure exactly once", got)
		}
	}
}

type orderedRecordAdapter struct {
	*peerAdapter
	first  <-chan struct{}
	second chan struct{}
}

func (adapter *orderedRecordAdapter) ExecuteRecords(ctx context.Context, plans []*execution.Plan) []*pb.Event {
	if plans[0].Key == "records/s:first" {
		select {
		case <-adapter.first:
		case <-ctx.Done():
			return nil
		}
	} else {
		close(adapter.second)
	}
	return adapter.peerAdapter.ExecuteRecords(ctx, plans)
}
func TestRecordStreamPublishesFIFOAfterOutOfOrderBackendCompletion(t *testing.T) {
	gate := make(chan struct{})
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}()
	adapter := &orderedRecordAdapter{peerAdapter: newPeerAdapter("records"), first: gate, second: make(chan struct{})}
	limits := store.DefaultLimits()
	limits.BatchOperations = 1

	opts := routeAcceptanceNodeOptions{adapter: adapter, store: limits}
	node := startRouteAcceptanceNode(t, opts)
	client := routeAcceptanceClient(t, node.address)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	stream, err := client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i, resource := range []string{"records/s:first", "records/s:second"} {
		read := &pb.ReadRequest{Resource: resource}
		request := readFrame(uint64(i+1), read)
		if err := stream.Send(request); err != nil {
			t.Fatal(err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-adapter.second:
	case <-ctx.Done():
		t.Fatal("receiver did not pipeline the second operation")
	}
	responses := make(chan *pb.ExecuteResponse, 2)
	done := make(chan error, 1)
	go func() {
		for {
			response, err := stream.Recv()
			if err != nil {
				done <- err
				return
			}
			responses <- response
		}
	}()
	select {
	case response := <-responses:
		t.Fatal("later completion bypassed first record", response)
	default:
	}
	close(gate)
	for index := uint64(1); index <= 2; index++ {
		select {
		case response := <-responses:
			if response.Index != index {
				t.Fatal("response reordered", response)
			}
		case <-ctx.Done():
			t.Fatal("response publication stalled")
		}
	}
	if err := <-done; err != io.EOF {
		t.Fatal(err)
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
}

func TestRecordStreamSingleRequestDoesNotExpireWhileBackendWorks(t *testing.T) {
	gate := make(chan struct{})
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}()
	adapter := newPeerAdapter("records")
	adapter.block = gate
	limits := DefaultLimits()
	limits.Stall = 150 * time.Millisecond
	opts := routeAcceptanceNodeOptions{adapter: adapter, limits: limits}
	node := startRouteAcceptanceNode(t, opts)
	client := routeAcceptanceClient(t, node.address)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	stream, err := client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request := readFrame(1, testRequest())
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	select {
	case <-adapter.seen:
	case <-ctx.Done():
		t.Fatal("record did not reach backend")
	}
	time.Sleep(2 * limits.Stall)
	close(gate)
	response, err := stream.Recv()
	if err != nil || response.Index != 1 {
		t.Fatal("input timeout mistook result wait for a stalled producer", response, err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	metric := &dto.Metric{}
	if err := node.server.metrics.watchdogs.WithLabelValues("input_or_result").Write(metric); err != nil {
		t.Fatal(err)
	}
	if metric.GetCounter().GetValue() != 0 {
		t.Fatal("input watchdog counted active backend work", metric)
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
}

func TestRecordStreamInvalidQueuedTailPublishesAdmittedPrefix(t *testing.T) {
	gate := make(chan struct{})
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}()
	adapter := newPeerAdapter("records")
	adapter.block = gate
	opts := routeAcceptanceNodeOptions{adapter: adapter}
	node := startRouteAcceptanceNode(t, opts)
	client := routeAcceptanceClient(t, node.address)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	stream, err := client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for index := uint64(1); index <= 3; index++ {
		read := testRequest()
		if index == 3 {
			read.Resource = "../invalid"
		}
		request := readFrame(index, read)
		if err := stream.Send(request); err != nil {
			t.Fatal(err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-adapter.seen:
	case <-ctx.Done():
		t.Fatal("valid prefix was not admitted")
	}
	close(gate)
	for index := uint64(1); index <= 2; index++ {
		response, err := stream.Recv()
		if err != nil || response.Index != index {
			t.Fatal("invalid tail erased an admitted prefix result", response, err)
		}
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatal("invalid tail accepted", err)
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
}

// blockedOutputStream supplies records forever while its first response
// never finishes sending.
type blockedOutputStream struct {
	grpc.ServerStream
	ctx      context.Context
	received atomic.Uint64
}

func (stream *blockedOutputStream) Context() context.Context { return stream.ctx }

func (stream *blockedOutputStream) Recv() (*pb.ExecuteRequest, error) {
	index := stream.received.Add(1)
	if index > 100 {
		return nil, io.EOF
	}
	return readFrame(index, testRequest()), nil
}

func (stream *blockedOutputStream) Send(*pb.ExecuteResponse) error {
	<-stream.ctx.Done()
	return status.FromContextError(stream.ctx.Err()).Err()
}

func TestRecordInputStopsAtCountBoundBehindSlowOutput(t *testing.T) {
	for _, window := range []int{3, RecordStreamItems, 96} {
		t.Run(fmt.Sprint(window), func(t *testing.T) {
			_, local := peerLocal(t, "records")
			transport := DefaultLimits()
			transport.MaxPendingRecords = window
			opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}, limits: transport}
			server, _ := startPeerServer(t, opts)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			stream := &blockedOutputStream{ctx: ctx}
			done := make(chan error, 1)
			go func() { done <- server.Execute(stream) }()
			deadline := time.Now().Add(time.Second)
			for stream.received.Load() < uint64(window) {
				if time.Now().After(deadline) {
					t.Fatal("pipeline did not fill its bounded count", stream.received.Load())
				}
				time.Sleep(time.Millisecond)
			}
			time.Sleep(50 * time.Millisecond)
			if received := stream.received.Load(); received != uint64(window) {
				t.Fatal("slow output did not pause request input", received)
			}
			cancel()
			if err := <-done; status.Code(err) != codes.Canceled {
				t.Fatal("canceled pipeline did not finish", err)
			}
		})
	}
}

func TestRecordStreamCancellationKeepsOtherRPCOnSharedConnection(t *testing.T) {
	gate := make(chan struct{})
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}()
	adapter := newPeerAdapter("records")
	adapter.block = gate
	opts := routeAcceptanceNodeOptions{adapter: adapter}
	node := startRouteAcceptanceNode(t, opts)
	connection, client := routeAcceptanceConnection(t, node.address)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	first, err := client.Execute(firstCtx)
	if err != nil {
		t.Fatal(err)
	}
	request := readFrame(1, testRequest())
	if err := first.Send(request); err != nil {
		t.Fatal(err)
	}
	select {
	case <-adapter.seen:
	case <-ctx.Done():
		t.Fatal("first RPC did not start")
	}
	second, err := client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Send(request); err != nil {
		t.Fatal(err)
	}
	if err := second.CloseSend(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-adapter.seen:
	case <-ctx.Done():
		t.Fatal("peer RPC did not start")
	}
	stopFirst()
	if _, err := first.Recv(); status.Code(err) != codes.Canceled {
		t.Fatal("first RPC did not cancel", err)
	}
	close(gate)
	response, err := second.Recv()
	if err != nil || response.Index != 1 {
		t.Fatal("one canceled receiver aborted the peer RPC", response, err)
	}
	if _, err := second.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
	if connection.GetState() != connectivity.Ready {
		t.Fatal("completed or canceled stream closed a healthy shared connection", connection.GetState())
	}
}
