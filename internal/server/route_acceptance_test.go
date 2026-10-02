package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
)

// This fixture implements the new execution boundary independently of the
// production adapters. Tests use real loopback TCP and the real Store scheduler.
type routeAcceptanceAdapter struct {
	mu       sync.Mutex
	values   map[string][]byte
	commands atomic.Int64
	executed atomic.Int64
	closed   atomic.Int64
	maxBatch atomic.Int64
	block    <-chan struct{}
	seen     chan []*execution.Plan
	bytes    int
	waitKey  string
	keyReady chan struct{}
	keyExit  <-chan struct{}
}

func newRouteAcceptanceAdapter(recordBytes int) *routeAcceptanceAdapter {
	a := &routeAcceptanceAdapter{values: make(map[string][]byte), bytes: recordBytes, seen: make(chan []*execution.Plan, 32)}
	return a
}

func (a *routeAcceptanceAdapter) PrepareCall(id uint64, call *pb.Call) (*execution.Plan, *pb.Failure) {
	key := ""
	resultBytes := 512
	if request := call.GetRead(); request != nil {
		key = request.Resource
		resultBytes += a.bytes
	} else if request := call.GetMutate(); request != nil {
		key = request.Resource
	} else {
		failure := &pb.Failure{Code: pb.FailureCode_UNSUPPORTED, Message: "acceptance fixture only accepts records"}
		return nil, failure
	}
	p := &execution.Plan{ID: id, Call: call, Key: key, BatchKey: "records", Bytes: proto.Size(call) + 512, ResultBytes: resultBytes, WorkingBytes: resultBytes}
	op := &pb.Operation{Index: id}
	if request := call.GetRead(); request != nil {
		variant := &pb.Operation_Read{Read: request}
		op.Operation = variant
	} else {
		variant := &pb.Operation_Mutate{Mutate: call.GetMutate()}
		op.Operation = variant
	}
	p.Operation = op
	return p, nil
}

func (a *routeAcceptanceAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) execution.Feedback {
	for old := a.maxBatch.Load(); int64(len(plans)) > old && !a.maxBatch.CompareAndSwap(old, int64(len(plans))); old = a.maxBatch.Load() {
	}
	select {
	case a.seen <- plans:
	default:
	}
	if a.block != nil {
		select {
		case <-a.block:
		case <-ctx.Done():
			return execution.Neutral
		}
	}
	for _, p := range plans {
		if p.Key == a.waitKey && a.keyExit != nil {
			select {
			case a.keyReady <- struct{}{}:
			default:
			}
			select {
			case <-a.keyExit:
			case <-ctx.Done():
				return execution.Neutral
			}
		}
		result := &pb.Result{Index: p.Operation.Index}
		if p.Context != nil && p.Context.Err() != nil {
			failure := &pb.Failure{Code: pb.FailureCode_CANCELLED, Message: "fixture caller canceled"}
			mutation := &pb.MutationResult{Outcome: pb.MutationOutcome_NOT_STARTED, Failure: failure}
			variant := &pb.Result_Mutation{Mutation: mutation}
			result.Result = variant
		} else if request := p.Call.GetRead(); request != nil {
			var data []byte
			if strings.HasPrefix(request.Resource, "records/s:large") {
				number, _ := strconv.Atoi(strings.TrimPrefix(request.Resource, "records/s:large"))
				data = bytes.Repeat([]byte{byte(1 + number%251)}, a.bytes)
			} else {
				a.mu.Lock()
				data = bytes.Clone(a.values[request.Resource])
				a.mu.Unlock()
			}
			read := &pb.ReadResult{}
			if data == nil {
				empty := &pb.Empty{}
				variant := &pb.ReadResult_Missing{Missing: empty}
				read.Result = variant
			} else {
				document := &pb.Document{MediaType: "application/octet-stream", Data: data}
				variant := &pb.ReadResult_Document{Document: document}
				read.Result = variant
			}
			variant := &pb.Result_Read{Read: read}
			result.Result = variant
		} else {
			request := p.Call.GetMutate()
			a.mu.Lock()
			a.values[request.Resource] = bytes.Clone(request.GetPut().GetData())
			a.mu.Unlock()
			a.commands.Add(1)
			mutation := &pb.MutationResult{Outcome: pb.MutationOutcome_APPLIED}
			variant := &pb.Result_Mutation{Mutation: mutation}
			result.Result = variant
		}
		variant := &pb.Event_Result{Result: result}
		event := &pb.Event{Version: 1, Value: variant}
		if err := emit(p, event); err != nil {
			continue
		}
		a.executed.Add(1)
	}
	return execution.Healthy
}

func (a *routeAcceptanceAdapter) Close() error {
	a.closed.Add(1)
	return nil
}

func (a *routeAcceptanceAdapter) ClosePlan(context.Context, *execution.Plan) *pb.Failure {
	return nil
}

type routeAcceptanceNode struct {
	server  *Server
	runtime *store.Runtime
	adapter *routeAcceptanceAdapter
	address string
}

type routeAcceptanceNodeOptions struct {
	adapter execution.Adapter
	limits  Limits
	store   store.Limits
}

func startRouteAcceptanceNode(t testing.TB, opts routeAcceptanceNodeOptions) *routeAcceptanceNode {
	t.Helper()
	if opts.limits.Sessions == 0 {
		opts.limits = DefaultLimits()
		opts.limits.Stall = 5 * time.Second
	}
	node := &routeAcceptanceNode{}
	node.adapter, _ = opts.adapter.(*routeAcceptanceAdapter)
	if opts.store.Concurrency == 0 {
		opts.store = store.DefaultLimits()
	}
	var err error
	node.runtime, err = store.New(opts.adapter, opts.store)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := NewAdmission(opts.limits)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{Stores: map[string]*store.Runtime{"records": node.runtime}, Limits: opts.limits, Admission: admission}
	node.server, err = New(config)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	node.address = listener.Addr().String()
	done := make(chan error, 1)
	go func() { done <- node.server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := node.server.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		_ = listener.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
		if node.runtime != nil {
			if err := node.runtime.Close(ctx); err != nil {
				t.Error(err)
			}
			if node.adapter != nil && node.adapter.closed.Load() != 1 {
				t.Error("adapter ownership was not released exactly once", node.adapter.closed.Load())
			}
		}
	})
	return node
}

func routeAcceptanceClient(t testing.TB, address string) pb.WeirClient {
	t.Helper()
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(),
		grpc.WithStaticStreamWindowSize(65535), grpc.WithStaticConnWindowSize(65535),
		grpc.WithReadBufferSize(16 << 10), grpc.WithWriteBufferSize(16 << 10),
		grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0), grpc.MaxCallRecvMsgSize(128<<10), grpc.MaxCallSendMsgSize(10<<20)),
	}
	connection, err := grpc.NewClient("passthrough:///"+address, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return pb.NewWeirClient(connection)
}

func routeAcceptanceServer(t testing.TB, adapter *routeAcceptanceAdapter) ([]*routeAcceptanceNode, pb.WeirClient) {
	t.Helper()
	opts := routeAcceptanceNodeOptions{adapter: adapter}
	node := startRouteAcceptanceNode(t, opts)
	return []*routeAcceptanceNode{node}, routeAcceptanceClient(t, node.address)
}

func routeAcceptanceRead(id uint64, key string) *pb.Request {
	read := &pb.ReadRequest{Resource: key}
	variant := &pb.Call_Read{Read: read}
	call := &pb.Call{Version: 1, Operation: variant}
	raw, err := proto.Marshal(call)
	if err != nil {
		panic(err)
	}
	request := &pb.Request{Id: id, Destination: "records", Payload: raw}
	return request
}

func routeAcceptancePut(id uint64, key, value string) *pb.Request {
	document := &pb.Document{MediaType: "application/octet-stream", Data: []byte(value)}
	action := &pb.MutateRequest_Put{Put: document}
	mutate := &pb.MutateRequest{Resource: key, Action: action}
	variant := &pb.Call_Mutate{Mutate: mutate}
	call := &pb.Call{Version: 1, Operation: variant}
	raw, err := proto.Marshal(call)
	if err != nil {
		panic(err)
	}
	request := &pb.Request{Id: id, Destination: "records", Payload: raw}
	return request
}

// The decoder releases each completed ID, so even the test consumer cannot
// accidentally turn the memory acceptance run into a collect-all benchmark.
type routeAcceptanceDecoder struct {
	partial map[uint64]*bytes.Buffer
	count   int
	bytes   uint64
	frames  int
}

func newRouteAcceptanceDecoder() *routeAcceptanceDecoder {
	d := &routeAcceptanceDecoder{partial: make(map[uint64]*bytes.Buffer)}
	return d
}

func (d *routeAcceptanceDecoder) consume(response *pb.Response) (*pb.Event, error) {
	if response.Id == 0 || len(response.Payload) > 64<<10 || response.End && len(response.Payload) != 0 {
		return nil, errors.New("invalid response envelope")
	}
	d.frames++
	buffer := d.partial[response.Id]
	if buffer == nil {
		if len(d.partial) >= 8 {
			return nil, errors.New("server exceeded eight active response IDs")
		}
		buffer = &bytes.Buffer{}
		d.partial[response.Id] = buffer
	}
	if buffer.Len()+len(response.Payload) > (2<<20)+4096 {
		return nil, errors.New("response exceeded fixture record bound")
	}
	_, _ = buffer.Write(response.Payload)
	if !response.End {
		return nil, nil
	}
	event := &pb.Event{}
	if err := protodelim.UnmarshalFrom(buffer, event); err != nil {
		return nil, fmt.Errorf("incomplete Event: %w", err)
	}
	if buffer.Len() != 0 || event.Version != 1 || event.GetResult().GetIndex() != response.Id {
		return nil, errors.New("response Event cardinality/version/ID mismatch")
	}
	d.bytes += uint64(len(event.GetResult().GetRead().GetDocument().GetData()))
	d.count++
	delete(d.partial, response.Id)
	return event, nil
}

func assertRouteAcceptanceIdle(t testing.TB, nodes []*routeAcceptanceNode) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		idle := true
		for _, node := range nodes {
			idle = idle && len(node.server.slots) == 0
			if node.runtime != nil {
				s := node.runtime.Snapshot()
				idle = idle && s.Pending == 0 && s.Active == 0 && s.Retained == 0 && s.ResultBytes == 0 && s.WorkingBytes == 0 && s.Publishers == 0
			}
		}
		if idle {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, node := range nodes {
		if node.runtime != nil {
			t.Log("runtime after terminal:", node.runtime.Snapshot())
		}
	}
	t.Fatal("Route terminal did not release sessions, tasks and bytes")
}

func TestRouteAcceptanceLargeResponseAndHalfClose(t *testing.T) {
	adapter := newRouteAcceptanceAdapter(2 << 20)
	nodes, client := routeAcceptanceServer(t, adapter)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream, err := client.Route(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sent := make(chan error, 1)
	go func() {
		for id := uint64(1); id <= 16; id++ {
			request := routeAcceptanceRead(id, fmt.Sprintf("records/s:large%d", id))
			if err := stream.Send(request); err != nil {
				sent <- err
				return
			}
		}
		sent <- stream.CloseSend()
	}()
	decoder := newRouteAcceptanceDecoder()
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		event, err := decoder.consume(response)
		if err != nil {
			t.Fatal(err)
		}
		if event != nil {
			data := event.GetResult().GetRead().GetDocument().GetData()
			id := event.GetResult().Index
			if len(data) != 2<<20 || !bytes.Equal(data, bytes.Repeat([]byte{byte(1 + id%251)}, len(data))) {
				t.Fatal("large record was corrupted or truncated", id, len(data))
			}
		}
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	if decoder.count != 16 || decoder.bytes != 32<<20 || len(decoder.partial) != 0 || decoder.frames <= decoder.count*2 {
		t.Fatal("half-close failed to drain fragmented responses", decoder.count, decoder.bytes, decoder.frames)
	}
	assertRouteAcceptanceIdle(t, nodes)
}

func TestRouteAcceptanceProtocolFailureDoesNotRollback(t *testing.T) {
	for _, invalid := range []string{"zero_id", "repeated_id", "decreasing_id", "destination"} {
		t.Run(invalid, func(t *testing.T) {
			adapter := newRouteAcceptanceAdapter(1024)
			nodes, client := routeAcceptanceServer(t, adapter)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := client.Route(ctx)
			if err != nil {
				t.Fatal(err)
			}
			first := routeAcceptancePut(4, "records/s:key", "committed")
			if err := stream.Send(first); err != nil {
				t.Fatal(err)
			}
			decoder := newRouteAcceptanceDecoder()
			for decoder.count == 0 {
				response, err := stream.Recv()
				if err != nil {
					t.Fatal(err)
				}
				event, err := decoder.consume(response)
				if err != nil {
					t.Fatal(err)
				}
				if event != nil && event.GetResult().GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
					t.Fatal("first mutation did not commit", event)
				}
			}
			second := routeAcceptanceRead(9, "records/s:key")
			switch invalid {
			case "zero_id":
				second.Id = 0
			case "repeated_id":
				second.Id = 4
			case "decreasing_id":
				second.Id = 3
			case "destination":
				second.Destination = "other"
			}
			_ = stream.Send(second)
			_ = stream.CloseSend()
			_, err = stream.Recv()
			if status.Code(err) != codes.InvalidArgument {
				t.Fatal("later protocol violation did not terminate RPC", err)
			}
			adapter.mu.Lock()
			value := string(adapter.values["records/s:key"])
			adapter.mu.Unlock()
			if adapter.commands.Load() != 1 || value != "committed" {
				t.Fatal("protocol failure changed preceding committed write", adapter.commands.Load(), value)
			}
			assertRouteAcceptanceIdle(t, nodes)
		})
	}
}

func routeAcceptanceDrain(stream pb.Weir_RouteClient) ([]uint64, error) {
	decoder := newRouteAcceptanceDecoder()
	var ids []uint64
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if len(decoder.partial) != 0 {
				return nil, errors.New("incomplete response at gRPC EOF")
			}
			return ids, nil
		}
		if err != nil {
			return nil, err
		}
		event, err := decoder.consume(response)
		if err != nil {
			return nil, err
		}
		if event != nil {
			ids = append(ids, event.GetResult().Index)
		}
	}
}

func TestRouteAcceptanceIndependentCompletionAndSameRecordOrder(t *testing.T) {
	adapter := newRouteAcceptanceAdapter(1024)
	release := make(chan struct{})
	adapter.waitKey = "records/s:locked"
	adapter.keyReady = make(chan struct{}, 2)
	adapter.keyExit = release
	limits := store.DefaultLimits()
	limits.BatchOperations = 1
	limits.Collect = 0
	opts := routeAcceptanceNodeOptions{adapter: adapter, store: limits}
	node := startRouteAcceptanceNode(t, opts)
	client := routeAcceptanceClient(t, node.address)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Route(ctx)
	if err != nil {
		t.Fatal(err)
	}
	requests := []*pb.Request{routeAcceptancePut(1, adapter.waitKey, "first"), routeAcceptanceRead(2, "records/s:independent"), routeAcceptancePut(3, adapter.waitKey, "second")}
	for _, request := range requests {
		if err := stream.Send(request); err != nil {
			t.Fatal(err)
		}
	}
	_ = stream.CloseSend()
	select {
	case <-adapter.keyReady:
	case <-ctx.Done():
		t.Fatal("first record operation did not reach backend")
	}
	decoder := newRouteAcceptanceDecoder()
	fast := make(chan error, 1)
	go func() {
		for {
			response, err := stream.Recv()
			if err != nil {
				fast <- err
				return
			}
			event, err := decoder.consume(response)
			if err != nil {
				fast <- err
				return
			}
			if event != nil {
				if event.GetResult().Index != 2 {
					fast <- errors.New("independent request did not complete before stalled record")
				} else {
					fast <- nil
				}
				return
			}
		}
	}()
	select {
	case err := <-fast:
		if err != nil {
			close(release)
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		cancel()
		<-fast
		t.Fatal("stalled record serialized an independent request")
	}
	var sameRecordIDs []uint64
beforeRelease:
	for {
		select {
		case plans := <-adapter.seen:
			for _, plan := range plans {
				if plan.Key == adapter.waitKey {
					sameRecordIDs = append(sameRecordIDs, plan.ID)
				}
			}
		default:
			break beforeRelease
		}
	}
	if len(sameRecordIDs) != 1 || sameRecordIDs[0] != 1 {
		close(release)
		t.Fatal("later same-record operation entered backend before first completed", sameRecordIDs)
	}
	close(release)
	var ids []uint64
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			if len(decoder.partial) != 0 {
				t.Fatal("same-record response ended incompletely")
			}
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		event, err := decoder.consume(response)
		if err != nil {
			t.Fatal(err)
		}
		if event != nil {
			if event.GetResult().GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal("same-record mutation did not complete APPLIED", event)
			}
			ids = append(ids, event.GetResult().Index)
		}
	}
	if len(ids) != 2 || !((ids[0] == 1 && ids[1] == 3) || (ids[0] == 3 && ids[1] == 1)) {
		t.Fatal("same-record operations did not each return one complete result", ids)
	}
afterRelease:
	for {
		select {
		case plans := <-adapter.seen:
			for _, plan := range plans {
				if plan.Key == adapter.waitKey {
					sameRecordIDs = append(sameRecordIDs, plan.ID)
				}
			}
		default:
			break afterRelease
		}
	}
	if len(sameRecordIDs) != 2 || sameRecordIDs[0] != 1 || sameRecordIDs[1] != 3 {
		t.Fatal("same-record backend execution did not preserve input order", sameRecordIDs)
	}
	adapter.mu.Lock()
	value := string(adapter.values[adapter.waitKey])
	adapter.mu.Unlock()
	if value != "second" {
		t.Fatal("same-record write order changed final value", value)
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
}

func TestRouteAcceptanceCrossRPCBatchCancellationIsolation(t *testing.T) {
	adapter := newRouteAcceptanceAdapter(1024)
	release := make(chan struct{})
	adapter.block = release
	limits := store.DefaultLimits()
	limits.Collect = 10 * time.Millisecond
	opts := routeAcceptanceNodeOptions{adapter: adapter, store: limits}
	node := startRouteAcceptanceNode(t, opts)
	client := routeAcceptanceClient(t, node.address)
	firstCtx, cancelFirst := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelFirst()
	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelSecond()
	first, err := client.Route(firstCtx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.Route(secondCtx)
	if err != nil {
		t.Fatal(err)
	}
	firstRequest := routeAcceptancePut(1, "records/s:firstRPC", "one")
	secondRequest := routeAcceptancePut(1, "records/s:secondRPC", "two")
	if err := first.Send(firstRequest); err != nil {
		t.Fatal(err)
	}
	if err := second.Send(secondRequest); err != nil {
		t.Fatal(err)
	}
	_ = first.CloseSend()
	_ = second.CloseSend()
	select {
	case batch := <-adapter.seen:
		if len(batch) != 2 || batch[0].Key == batch[1].Key {
			close(release)
			t.Fatal("compatible independent RPCs did not share one backend execution", len(batch))
		}
	case <-secondCtx.Done():
		close(release)
		t.Fatal("cross-RPC batch never reached adapter")
	}
	cancelFirst()
	if _, err := first.Recv(); status.Code(err) != codes.Canceled {
		close(release)
		t.Fatal("first RPC did not cancel", err)
	}
	close(release)
	ids, err := routeAcceptanceDrain(second)
	if err != nil || len(ids) != 1 || ids[0] != 1 {
		t.Fatal("canceling one member canceled its active batch peer", ids, err)
	}
	adapter.mu.Lock()
	value := string(adapter.values[secondRequest.GetDestination()+"/s:secondRPC"])
	adapter.mu.Unlock()
	if value != "two" {
		t.Fatal("noncanceled peer mutation was not applied", value)
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
}
