package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type routeAcceptanceNode struct {
	server  *Server
	runtime *store.Runtime
	address string
}

type routeAcceptanceNodeOptions struct {
	adapter execution.Adapter
	limits  Limits
	store   store.Limits
}

func startRouteAcceptanceNode(t testing.TB, opts routeAcceptanceNodeOptions) *routeAcceptanceNode {
	t.Helper()
	if opts.limits.Stall == 0 {
		opts.limits = DefaultLimits()
		opts.limits.Stall = 5 * time.Second
	}
	if opts.store.BatchOperations == 0 {
		opts.store = store.DefaultLimits()
	}
	local, err := store.New(opts.adapter, opts.store)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := NewAdmission(opts.limits)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{Stores: map[string]*store.Runtime{"records": local}, Limits: opts.limits, Admission: admission}
	srv, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	node := &routeAcceptanceNode{server: srv, runtime: local, address: listener.Addr().String()}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		_ = listener.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
		if err := local.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return node
}

func routeAcceptanceClient(t testing.TB, address string) pb.StoreServiceClient {
	t.Helper()
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(protocol.MaxExecuteResponseBytes), grpc.MaxCallSendMsgSize(protocol.MaxExecuteRequestBytes))}
	connection, err := grpc.NewClient("passthrough:///"+address, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return pb.NewStoreServiceClient(connection)
}

func assertRouteAcceptanceIdle(t testing.TB, nodes []*routeAcceptanceNode) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		idle := true
		for _, node := range nodes {
			snapshot := node.runtime.Snapshot()
			idle = idle && node.server.Snapshot().ActiveRPCs == 0 && snapshot.Pending == 0 && snapshot.Active == 0 && snapshot.Retained == 0 && snapshot.ResultBytes == 0 && snapshot.WorkingBytes == 0 && snapshot.Publishers == 0 && node.server.admission.wireBytes.Load() == 0
		}
		if idle {
			return
		}
		// Orphaned transport buffers are reclaimed after they become unreachable.
		// Cancellation and the collection of their storage can occur separately.
		for _, node := range nodes {
			if node.server.admission.wireBytes.Load() != 0 {
				runtime.GC()
				break
			}
		}
		if time.Now().After(deadline) {
			var snapshots []store.Snapshot
			var active, wire []int64
			for _, node := range nodes {
				snapshots = append(snapshots, node.runtime.Snapshot())
				active = append(active, node.server.Snapshot().ActiveRPCs)
				wire = append(wire, node.server.admission.wireBytes.Load())
			}
			t.Fatal("execution resources did not drain", snapshots, active, wire)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReadStreamAccepts513Records(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	requests := make([]*pb.ReadRequest, 513)
	for i := range requests {
		requests[i] = &pb.ReadRequest{Resource: fmt.Sprintf("data/s:%d", i)}
	}
	results, err := testutil.ReadRecords(t.Context(), client, "records", requests)
	if err != nil || len(results) != len(requests) {
		t.Fatal("long input lost records", len(results), err)
	}
	waitPeerIdle(t, server)
}

func TestContinuousReadsAcrossConcurrentSessions(t *testing.T) {
	adapter := newPeerAdapter("records")
	gate := make(chan struct{})
	adapter.block = gate
	limits := DefaultLimits()
	const sessions = 64
	opts := routeAcceptanceNodeOptions{adapter: adapter, limits: limits}
	node := startRouteAcceptanceNode(t, opts)
	client := routeAcceptanceClient(t, node.address)
	read := &pb.ReadRequest{Resource: "records/s:item"}
	requests := make([]*pb.ReadRequest, 32)
	for i := range requests {
		requests[i] = read
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	failures := make(chan error, sessions)
	var workers sync.WaitGroup
	for range sessions {
		workers.Go(func() {
			for range 4 {
				response, err := testutil.ReadRecords(ctx, client, "records", requests)
				if err != nil {
					failures <- err
					return
				}
				if len(response) != len(requests) {
					failures <- fmt.Errorf("read result count changed: %d", len(response))
					return
				}
				for index, result := range response {
					if result.GetMissing() == nil || result.GetFailure() != nil {
						failures <- fmt.Errorf("read %d did not return a successful missing result: %v", index, result)
						return
					}
				}
			}
		})
	}
	// Hold the first results until all concurrent RPCs have returned a result. Each worker
	// then verifies full record windows and repeated release and reacquisition.
	for node.server.Snapshot().ActiveRPCs != int64(sessions) {
		select {
		case <-ctx.Done():
			close(gate)
			workers.Wait()
			t.Fatal("concurrent reads did not occupy every session", ctx.Err(), node.server.Snapshot(), node.runtime.Snapshot())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(gate)
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Error("legitimate continuous session boundary rejected", err)
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
}

func TestMutationStreamOrdersDuplicateKeys(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	requests := []*pb.MutateRequest{testMutation("first"), testMutation("second"), testMutation("last")}
	results, err := testutil.MutateRecords(t.Context(), client, "records", requests)
	if err != nil || len(results) != 3 {
		t.Fatal(err)
	}
	for _, result := range results {
		if result.Outcome != pb.MutationOutcome_APPLIED {
			t.Fatal("lost write confirmation", result)
		}
	}
	read, err := routeRead(client, t.Context(), testRequest())
	if err != nil || !bytes.Equal(read.GetDocument().GetData(), []byte("last")) {
		t.Fatal("duplicate writes executed out of order", read, err)
	}
	waitPeerIdle(t, server)
}

func TestReadStreamLargeRelativeMetadataUsesBoundedWindows(t *testing.T) {
	adapter, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	srv, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	read := &pb.ReadRequest{Resource: strings.Repeat("a/", 1500) + "a"}
	items := make([]*pb.ReadRequest, 1200)
	for i := range items {
		items[i] = read
	}
	results, err := testutil.ReadRecords(t.Context(), client, "records", items)
	if err != nil || len(results) != len(items) {
		t.Fatal("bounded metadata windows lost records", len(results), err)
	}
	adapter.mu.Lock()
	batches := append([]int(nil), adapter.batchSizes...)
	adapter.mu.Unlock()
	for _, size := range batches {
		if size > store.DefaultLimits().BatchOperations {
			t.Fatal("metadata bypassed the database window", size)
		}
	}
	waitPeerIdle(t, srv)
	if snapshot := local.Snapshot(); snapshot.Active != 0 || snapshot.Pending != 0 || snapshot.Retained != 0 || snapshot.ResultBytes != 0 || snapshot.WorkingBytes != 0 {
		t.Fatal("rejected retained metadata reserved Store resources", snapshot)
	}
}

func TestLongReadStreamReleasesResultCreditsIncrementally(t *testing.T) {
	adapter, local := peerLocal(t, "records")
	document := &pb.Document{ContentType: "application/octet-stream", Data: bytes.Repeat([]byte("x"), 1<<20)}
	adapter.documents[testRequest().Resource] = document
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	srv, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	requests := make([]*pb.ReadRequest, 40)
	for i := range requests {
		requests[i] = testRequest()
	}
	response, err := testutil.ReadRecords(t.Context(), client, "records", requests)
	if err != nil {
		t.Fatal("one oversize member budget discarded the complete response", err)
	}
	succeeded, failed := 0, 0
	for _, result := range response {
		if result.GetDocument() != nil {
			succeeded++
		} else if result.GetFailure().GetCode() == pb.FailureCode_RESOURCE_EXHAUSTED {
			failed++
		} else {
			t.Fatal("invalid budget exhaustion result", result)
		}
	}
	if succeeded != 40 || failed != 0 {
		t.Fatal("stream windows failed to release result credits", succeeded, failed)
	}
	waitPeerIdle(t, srv)
	if local.Snapshot().ResultBytes != 0 {
		t.Fatal("actual copied read bytes were not released")
	}
}

func TestConcurrentClientRPCsKeepResponseOwnershipAndBatchBounds(t *testing.T) {
	const count = 512
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
	for i := range count {
		key := fmt.Sprintf("data/s:%d", i)
		document := &pb.Document{ContentType: "application/octet-stream", Data: []byte(key)}
		adapter.documents[key] = document
	}
	limits := store.DefaultLimits()

	transport := DefaultLimits()

	opts := routeAcceptanceNodeOptions{adapter: adapter, store: limits, limits: transport}
	node := startRouteAcceptanceNode(t, opts)
	clients := make([]pb.StoreServiceClient, 4)
	for i := range clients {
		clients[i] = routeAcceptanceClient(t, node.address)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	var workers sync.WaitGroup
	failures := make(chan error, count+1)
	workers.Go(func() {
		read := &pb.ReadRequest{Resource: "data/s:blocker"}
		request := &pb.ExecuteRequest{StoreName: "records",
			Index:   1,
			Command: &pb.Command{Operation: &pb.Command_Read{Read: read}}}

		_, err := testutil.ReadRecords(ctx, clients[0], request.StoreName, []*pb.ReadRequest{read})
		if err != nil {
			failures <- err
		}
	})
	select {
	case <-adapter.seen:
	case <-ctx.Done():
		t.Fatal("initial backend execution did not start")
	}
	for i := range count {
		workers.Go(func() {
			key := fmt.Sprintf("data/s:%d", i)
			read := &pb.ReadRequest{Resource: key}
			request := &pb.ExecuteRequest{StoreName: "records",
				Index:   1,
				Command: &pb.Command{Operation: &pb.Command_Read{Read: read}}}

			response, err := testutil.ReadRecords(ctx, clients[i%len(clients)], request.StoreName, []*pb.ReadRequest{read})
			if err != nil {
				failures <- err
				return
			}
			if len(response) != 1 || string(response[0].GetDocument().GetData()) != key {
				failures <- fmt.Errorf("RPC %d received another caller's response: %v", i, response)
			}
		})
	}
	for node.runtime.Snapshot().Retained != count+1 {
		select {
		case <-ctx.Done():
			close(gate)
			workers.Wait()
			t.Fatal("single-record RPCs did not reach queue", node.runtime.Snapshot())
		case <-time.After(time.Millisecond):
		}
	}
	close(gate)
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	adapter.mu.Lock()
	sizes := append([]int(nil), adapter.batchSizes...)
	adapter.mu.Unlock()
	total := 0
	for _, size := range sizes {
		if size < 1 || size > limits.BatchOperations {
			t.Fatal("physical batch exceeded configured operation bound", sizes)
		}
		total += size
	}
	if total != count+1 {
		t.Fatal("concurrent callers lost or duplicated execution", total)
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
}
