package server

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
)

func readFrame(index uint64, read *pb.ReadRequest) *pb.ExecuteRequest {
	command := &pb.Command{Operation: &pb.Command_Read{Read: read}}
	request := &pb.ExecuteRequest{StoreName: "records", Index: index, Command: command}
	return request
}
func TestRecordStreamLongInputAndLargeResultsStayBounded(t *testing.T) {
	adapter, local := peerLocal(t, "records")
	document := &pb.Document{ContentType: "application/octet-stream", Data: bytes.Repeat([]byte("x"), protocol.MaxDocument)}
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
		if err != nil || response.Index != index || len(response.Event.GetReadResult().GetDocument().GetData()) != protocol.MaxDocument {
			t.Fatal("lost indexed large result", index, response, err)
		}
		snapshot := local.Snapshot()
		if snapshot.ResultBytes > store.DefaultLimits().ResultBytes || snapshot.Retained > 2 || snapshot.Publishers != 0 {
			t.Fatal("logical stream accumulated result owners", snapshot)
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

type ownedOutputStream struct {
	grpc.ServerStream
	ctx     context.Context
	codec   *responseCodec
	encoded chan mem.BufferSlice
}

func (stream *ownedOutputStream) Context() context.Context   { return stream.ctx }
func (*ownedOutputStream) Recv() (*pb.ExecuteRequest, error) { return nil, io.EOF }
func (stream *ownedOutputStream) Send(response *pb.ExecuteResponse) error {
	encoded, err := stream.codec.Marshal(response)
	if err != nil {
		return err
	}
	stream.encoded <- encoded
	return nil
}

func TestRecordPublicationWaitsForFinalEncodedOwnership(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, _ := startPeerServer(t, opts)
	request := readFrame(1, testRequest())
	record, err := execution.NewRecord("records", 1, request.Command)
	if err != nil {
		t.Fatal(err)
	}
	prepared, failure := local.PrepareRecord(record)
	if failure != nil {
		t.Fatal(failure)
	}
	ticket, failure, _ := local.Submit(t.Context(), prepared, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	codec := &responseCodec{admission: server.admission}
	stream := &ownedOutputStream{ctx: t.Context(), codec: codec, encoded: make(chan mem.BufferSlice, 1)}
	completed := make(chan error, 1)
	go func() { completed <- server.publishRecord(stream, ticket, 1) }()
	encoded := <-stream.encoded
	defer func() { encoded.Free() }()
	encoded.Ref()
	encoded.Free()
	snapshot := local.Snapshot()
	if snapshot.Retained != 1 || snapshot.ResultBytes == 0 || server.admission.wireBytes.Load() == 0 {
		t.Fatal("window released before final transport owner", snapshot)
	}
	select {
	case err := <-completed:
		t.Fatal("publication returned while transport still owns bytes", err)
	default:
	}
	encoded.Free()
	encoded = nil
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	waitPeerIdle(t, server)
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
}

func TestRecordPublicationCancellationReleasesSourceButKeepsEncodedCredits(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, _ := startPeerServer(t, opts)
	request := readFrame(1, testRequest())
	record, err := execution.NewRecord("records", 1, request.Command)
	if err != nil {
		t.Fatal(err)
	}
	prepared, failure := local.PrepareRecord(record)
	if failure != nil {
		t.Fatal(failure)
	}
	ticket, failure, _ := local.Submit(t.Context(), prepared, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	codec := &responseCodec{admission: server.admission}
	stream := &ownedOutputStream{ctx: ctx, codec: codec, encoded: make(chan mem.BufferSlice, 1)}
	completed := make(chan error, 1)
	go func() { completed <- server.publishRecord(stream, ticket, 1) }()
	encoded := <-stream.encoded
	defer func() { encoded.Free() }()
	cancel()
	if err := <-completed; status.Code(err) != codes.Canceled {
		t.Fatal("output cancellation lost its status", err)
	}
	if snapshot := local.Snapshot(); snapshot.Retained != 0 || snapshot.ResultBytes != 0 {
		t.Fatal("canceled stream retained source documents", snapshot)
	}
	if server.admission.wireBytes.Load() == 0 {
		t.Fatal("cancellation returned still-owned encoded credits")
	}
	server.admission.responses.Range(func(key, value any) bool {
		t.Error("canceled publication retained a source message", key)
		return true
	})
	encoded.Free()
	encoded = nil
	waitPeerIdle(t, server)
}

func TestOutputWatchdogTransfersToEncodedBufferAfterSend(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, _ := startPeerServer(t, opts)
	server.limits.Stall = 50 * time.Millisecond
	codec := &responseCodec{admission: server.admission}
	stream := &ownedOutputStream{ctx: t.Context(), codec: codec, encoded: make(chan mem.BufferSlice)}
	read := protocol.Missing()
	value := &pb.Event_ReadResult{ReadResult: read}
	event := &pb.Event{Value: value}
	completed := make(chan error, 1)
	go func() { completed <- server.sendEvent(stream, event, 1) }()
	encoded := <-stream.encoded
	defer func() { encoded.Free() }()
	var owner *responseBufferOwner
	server.admission.responses.Range(func(key, value any) bool {
		owner = value.(*responseBufferOwner)
		return false
	})
	if owner == nil {
		t.Fatal("mock transport did not retain encoded ownership")
	}
	// No OutPayload stats notification has started the buffer watchdog yet.
	// A completed Send must not leave its earlier watchdog running.
	time.Sleep(2 * server.limits.Stall)
	metric := &dto.Metric{}
	counter := server.metrics.watchdogs.WithLabelValues("output")
	if err := counter.Write(metric); err != nil {
		t.Fatal(err)
	}
	if metric.GetCounter().GetValue() != 0 {
		t.Fatal("Send watchdog remained active after Send returned", metric)
	}
	owner.watch(t.Context(), server)
	deadline := time.Now().Add(time.Second)
	for {
		if err := counter.Write(metric); err != nil {
			t.Fatal(err)
		}
		if metric.GetCounter().GetValue() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("encoded ownership watchdog did not run")
		}
		time.Sleep(time.Millisecond)
	}
	encoded.Free()
	encoded = nil
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	if metric.GetCounter().GetValue() != 1 {
		t.Fatal("one stalled encoded owner produced duplicate expirations", metric)
	}
	waitPeerIdle(t, server)
}

type orderedRecordAdapter struct {
	*peerAdapter
	first  <-chan struct{}
	second chan struct{}
}

func (adapter *orderedRecordAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) bool {
	if plans[0].Key == "records/s:first" {
		select {
		case <-adapter.first:
		case <-ctx.Done():
			return false
		}
	} else {
		close(adapter.second)
	}
	return adapter.peerAdapter.Execute(ctx, plans, emit)
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
	limits.Concurrency = 2
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
	deadline := time.Now().Add(time.Second)
	for node.runtime.Snapshot().Retained != 2 {
		if time.Now().After(deadline) {
			t.Fatal("valid prefix was not admitted", node.runtime.Snapshot())
		}
		time.Sleep(time.Millisecond)
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

type ownedRecordStream struct {
	ownedOutputStream
	received atomic.Uint64
}

func (stream *ownedRecordStream) Recv() (*pb.ExecuteRequest, error) {
	index := stream.received.Add(1)
	if index > 100 {
		return nil, io.EOF
	}
	return readFrame(index, testRequest()), nil
}
func TestRecordInputStopsAtCountBoundBehindSlowOutput(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, _ := startPeerServer(t, opts)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	codec := &responseCodec{admission: server.admission}
	output := ownedOutputStream{ctx: ctx, codec: codec, encoded: make(chan mem.BufferSlice, 1)}
	stream := &ownedRecordStream{ownedOutputStream: output}
	done := make(chan error, 1)
	go func() { done <- server.Execute(stream) }()
	encoded := <-stream.encoded
	defer func() { encoded.Free() }()
	deadline := time.Now().Add(time.Second)
	for local.Snapshot().Retained < RecordStreamItems {
		if time.Now().After(deadline) {
			t.Fatal("pipeline did not fill its bounded count", local.Snapshot())
		}
		time.Sleep(time.Millisecond)
	}
	if snapshot := local.Snapshot(); snapshot.Retained != RecordStreamItems || snapshot.ResultBytes >= store.DefaultLimits().ResultBytes || snapshot.Publishers != 0 || stream.received.Load() != RecordStreamItems {
		t.Fatal("slow output did not pause request input independently of Store bytes", snapshot, stream.received.Load())
	}
	cancel()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatal("canceled pipeline did not finish", err)
	}
	encoded.Free()
	encoded = nil
	waitPeerIdle(t, server)
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
	client := routeAcceptanceClient(t, node.address)
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
	var connection *limitedConn
	node.server.connections.Range(func(_, value any) bool { connection = value.(*limitedConn); return false })
	if connection == nil {
		t.Fatal("shared connection missing")
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
	// A canceled gRPC DATA buffer may be orphaned while its native reference
	// count remains positive. Collection proves that encoded storage is no longer
	// reachable; its credit must not be returned merely because the RPC ended.
	runtime.GC()
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
	if connection.closed.Load() {
		t.Fatal("completed or canceled stream closed a healthy shared connection")
	}
}
