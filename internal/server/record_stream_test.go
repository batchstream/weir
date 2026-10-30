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
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

func readFrame(index uint64, read *pb.ReadRequest) *pb.ExecuteRequest {
	command := &pb.Command{Operation: &pb.Command_Read{Read: read}}
	request := &pb.ExecuteRequest{StoreName: "records", Index: index, Command: command}
	return request
}
func TestRecordStreamLongInputWithLargeResults(t *testing.T) {
	adapter, local := localStore(t, "records")
	document := &pb.Document{ContentType: "application/octet-stream", Data: bytes.Repeat([]byte("x"), (2 << 20))}
	adapter.documents[testRequest().Resource] = document
	opts := testServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, address := startTestServer(t, opts)
	_, client := testClient(t, address)
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
	waitIdle(t, server)
}

func TestRecordStreamInvalidLaterFramePreservesPublishedResults(t *testing.T) {
	for _, scenario := range []string{"ordinal", "Store", "kind", "request"} {
		t.Run(scenario, func(t *testing.T) {
			adapter, local := localStore(t, "records")
			opts := testServerOptions{stores: map[string]*store.Runtime{"records": local}}
			server, address := startTestServer(t, opts)
			_, client := testClient(t, address)
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
			waitIdle(t, server)
		})
	}
}

func TestRecordStreamIndexedOperationFailureDoesNotStopLaterResults(t *testing.T) {
	adapter, local := localStore(t, "records")
	adapter.ackFailureKey = testRequest().Resource
	adapter.ackFailure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "acknowledgement unavailable")
	opts := testServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, address := startTestServer(t, opts)
	_, client := testClient(t, address)
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
	waitIdle(t, server)
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
	*recordAdapter
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
	return adapter.recordAdapter.ExecuteRecords(ctx, plans)
}
func TestRecordStreamPublishesEachResultWhenItsRecordCompletes(t *testing.T) {
	gate := make(chan struct{})
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}()
	adapter := &orderedRecordAdapter{recordAdapter: newRecordAdapter("records"), first: gate, second: make(chan struct{})}
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
	// The second record's result must not wait behind the blocked first one.
	for _, index := range []uint64{2, 1} {
		select {
		case response := <-responses:
			if response.Index != index {
				t.Fatal("result waited for an earlier record", response)
			}
		case <-ctx.Done():
			t.Fatal("response publication stalled")
		}
		if index == 2 {
			close(gate)
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
	adapter := newRecordAdapter("records")
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
	adapter := newRecordAdapter("records")
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
	// Results arrive in completion order.
	seen := make(map[uint64]bool)
	for range 2 {
		response, err := stream.Recv()
		if err != nil || response.Index < 1 || response.Index > 2 || seen[response.Index] {
			t.Fatal("invalid tail erased an admitted prefix result", response, err)
		}
		seen[response.Index] = true
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
			_, local := localStore(t, "records")
			transport := DefaultLimits()
			transport.recordWindow = window
			opts := testServerOptions{stores: map[string]*store.Runtime{"records": local}, limits: transport}
			server, _ := startTestServer(t, opts)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			stream := &blockedOutputStream{ctx: ctx}
			done := make(chan error, 1)
			go func() { done <- server.Execute(stream) }()
			// The server reads one record past its window, which waits for a
			// credit, so a cancel request behind a full window is still read.
			bound := uint64(window) + 1
			deadline := time.Now().Add(time.Second)
			for stream.received.Load() < bound {
				if time.Now().After(deadline) {
					t.Fatal("pipeline did not fill its bounded count", stream.received.Load())
				}
				time.Sleep(time.Millisecond)
			}
			time.Sleep(50 * time.Millisecond)
			if received := stream.received.Load(); received != bound {
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
	adapter := newRecordAdapter("records")
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
		t.Fatal("concurrent RPC did not start")
	}
	stopFirst()
	if _, err := first.Recv(); status.Code(err) != codes.Canceled {
		t.Fatal("first RPC did not cancel", err)
	}
	close(gate)
	response, err := second.Recv()
	if err != nil || response.Index != 1 {
		t.Fatal("one canceled receiver aborted the concurrent RPC", response, err)
	}
	if _, err := second.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
	if connection.GetState() != connectivity.Ready {
		t.Fatal("completed or canceled stream closed a healthy shared connection", connection.GetState())
	}
}

func TestRecordStreamOversizedResultFailsOnlyItsRecord(t *testing.T) {
	adapter, local := localStore(t, "records")
	large := &pb.Document{ContentType: "application/octet-stream", Data: make([]byte, MaxMessageBytes)}
	adapter.documents["data/s:large"] = large
	small := &pb.Document{ContentType: "application/octet-stream", Data: []byte("x")}
	adapter.documents[testRequest().Resource] = small
	opts := testServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, address := startTestServer(t, opts)
	_, client := testClient(t, address)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	largeRead := &pb.ReadRequest{Resource: "data/s:large"}
	reads := []*pb.ReadRequest{largeRead, testRequest()}
	results, err := testutil.ReadRecords(ctx, client, "records", reads)
	if err != nil {
		t.Fatal("oversized result ended the stream", err)
	}
	if results[0].GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || !bytes.Equal(results[1].GetDocument().GetData(), small.Data) {
		t.Fatal("oversized result was not confined to its own record", results[0].GetFailure(), results[1])
	}
	waitIdle(t, server)
}

func TestExecuteRejectsUnknownFields(t *testing.T) {
	_, local := localStore(t, "records")
	opts := testServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, address := startTestServer(t, opts)
	_, client := testClient(t, address)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request := readFrame(1, testRequest())
	tag := protowire.AppendTag(nil, 99, protowire.VarintType)
	unknown := protowire.AppendVarint(tag, 1)
	request.ProtoReflect().SetUnknown(unknown)
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatal("request with unknown fields was accepted", err)
	}
	waitIdle(t, server)
}
