package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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
	if opts.limits.Sessions == 0 {
		opts.limits = DefaultLimits()
		opts.limits.Stall = 5 * time.Second
	}
	if opts.store.Concurrency == 0 {
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
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(protocol.MaxBatchResponseBytes), grpc.MaxCallSendMsgSize(protocol.MaxBatchRequestBytes))}
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
		if time.Now().After(deadline) {
			t.Fatal("batch resources did not drain")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReadBatchKeepsPhysicalBatchAndAccepts513Records(t *testing.T) {
	for _, count := range []int{32, 513} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			adapter, local := peerLocal(t, "records")
			opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
			srv, address := startPeerServer(t, opts)
			_, client := peerClient(t, address)
			request := &pb.ReadBatchRequest{StoreName: "records", Requests: make([]*pb.ReadRequest, count)}
			for i := range request.Requests {
				request.Requests[i] = &pb.ReadRequest{Resource: fmt.Sprintf("data/s:%d", i)}
			}
			response, err := client.Read(t.Context(), request)
			if err != nil || len(response.Results) != count {
				t.Fatal("batch lost records", err)
			}
			adapter.mu.Lock()
			batches := append([]int(nil), adapter.batchSizes...)
			adapter.mu.Unlock()
			if len(batches) != (count+31)/32 || batches[0] != 32 {
				t.Fatal("batch was dribbled into individual records", batches)
			}
			waitPeerIdle(t, srv)
			if snapshot := local.Snapshot(); snapshot.Publishers != 0 || snapshot.Retained != 0 || snapshot.ResultBytes != 0 {
				t.Fatal("unary batch retained per-record publishers or results", snapshot)
			}
		})
	}
}

func TestContinuousBatchReadsAtSessionLimit(t *testing.T) {
	adapter := newPeerAdapter("records")
	limits := DefaultLimits()
	limits.Sessions = 64
	opts := routeAcceptanceNodeOptions{adapter: adapter, limits: limits}
	node := startRouteAcceptanceNode(t, opts)
	client := routeAcceptanceClient(t, node.address)
	read := &pb.ReadRequest{Resource: "records/s:item"}
	request := &pb.ReadBatchRequest{StoreName: "records", Requests: make([]*pb.ReadRequest, 32)}
	for i := range request.Requests {
		request.Requests[i] = read
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	failures := make(chan error, limits.Sessions)
	var workers sync.WaitGroup
	for range limits.Sessions {
		workers.Go(func() {
			for range 32 {
				response, err := client.Read(ctx, request)
				if err != nil {
					failures <- err
					return
				}
				if len(response.Results) != len(request.Requests) {
					failures <- fmt.Errorf("batch result count changed: %d", len(response.Results))
					return
				}
			}
		})
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Error("legitimate continuous session boundary rejected", err)
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
}

func TestMutationBatchValidatesEntireInputAndOrdersDuplicateKeys(t *testing.T) {
	adapter, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	srv, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	bad := testMutation("bad")
	bad.Resource = "weir://elsewhere/data/s:key"
	request := &pb.MutateBatchRequest{StoreName: "records", Requests: []*pb.MutateRequest{testMutation("before"), bad}}
	if _, err := client.Mutate(t.Context(), request); status.Code(err) != codes.InvalidArgument || adapter.commands.Load() != 0 {
		t.Fatal("invalid trailing member executed earlier mutation", err, adapter.commands.Load())
	}
	request.Requests = []*pb.MutateRequest{testMutation("first"), testMutation("second"), testMutation("last")}
	response, err := client.Mutate(t.Context(), request)
	if err != nil || len(response.Results) != 3 {
		t.Fatal(err)
	}
	for _, result := range response.Results {
		if result.Outcome != pb.MutationOutcome_APPLIED {
			t.Fatal("mutation lost confirmation", result)
		}
	}
	read, err := routeRead(client, t.Context(), testRequest())
	if err != nil || !bytes.Equal(read.GetDocument().GetData(), []byte("last")) {
		t.Fatal("duplicate mutation keys executed out of input order", read, err)
	}
	waitPeerIdle(t, srv)
}

func TestReadBatchRejectsRetainedPathMetadataBeforeBackendWork(t *testing.T) {
	adapter, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	srv, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	read := &pb.ReadRequest{Resource: strings.Repeat("a/", 1500) + "a"}
	items := make([]*pb.ReadRequest, 1200)
	for i := range items {
		items[i] = read
	}
	request := &pb.ReadBatchRequest{StoreName: "records", Requests: items}
	if err := protocol.ValidateReadBatchRequest(request); err != nil {
		t.Fatal("metadata fixture violates the public wire contract", err)
	}
	if _, err := client.Read(t.Context(), request); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("native RPC lost the preparation metadata error", err)
	}
	adapter.mu.Lock()
	count := len(adapter.batchSizes)
	adapter.mu.Unlock()
	if count != 0 {
		t.Fatal("oversized retained metadata reached backend execution", count)
	}
	waitPeerIdle(t, srv)
	if snapshot := local.Snapshot(); snapshot.Active != 0 || snapshot.Pending != 0 || snapshot.Retained != 0 || snapshot.ResultBytes != 0 || snapshot.WorkingBytes != 0 {
		t.Fatal("rejected retained metadata reserved Store resources", snapshot)
	}
}

func TestBatchLargeDocumentsUseActualSharedResponseBudget(t *testing.T) {
	adapter, local := peerLocal(t, "records")
	document := &pb.Document{MediaType: "application/octet-stream", Data: bytes.Repeat([]byte("x"), 1<<20)}
	adapter.documents[testRequest().Resource] = document
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	srv, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	request := &pb.ReadBatchRequest{StoreName: "records", Requests: make([]*pb.ReadRequest, 40)}
	for i := range request.Requests {
		request.Requests[i] = testRequest()
	}
	response, err := client.Read(t.Context(), request)
	if err != nil {
		t.Fatal("one oversize member budget discarded the complete response", err)
	}
	succeeded, failed := 0, 0
	for _, result := range response.Results {
		if result.GetDocument() != nil {
			succeeded++
		} else if result.GetFailure().GetCode() == pb.FailureCode_RESOURCE_EXHAUSTED {
			failed++
		} else {
			t.Fatal("invalid budget exhaustion result", result)
		}
	}
	if succeeded != 31 || failed != 9 {
		t.Fatal("large reads were reserved per maximum or failed to bound actual bytes", succeeded, failed)
	}
	waitPeerIdle(t, srv)
	if local.Snapshot().ResultBytes != 0 {
		t.Fatal("actual copied read bytes were not released")
	}
}

func TestIndependentClientRPCsCoalesceWithoutCrossingResponseOwners(t *testing.T) {
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
		document := &pb.Document{MediaType: "application/octet-stream", Data: []byte(key)}
		adapter.documents[key] = document
	}
	limits := store.DefaultLimits()
	limits.Concurrency = 1
	limits.BackendTimeout = 5 * time.Second
	transport := DefaultLimits()
	transport.Sessions = count + 1
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
		request := &pb.ReadBatchRequest{StoreName: "records", Requests: []*pb.ReadRequest{read}}
		_, err := clients[0].Read(ctx, request)
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
			request := &pb.ReadBatchRequest{StoreName: "records", Requests: []*pb.ReadRequest{read}}
			response, err := clients[i%len(clients)].Read(ctx, request)
			if err != nil {
				failures <- err
				return
			}
			if len(response.Results) != 1 || string(response.Results[0].GetDocument().GetData()) != key {
				failures <- fmt.Errorf("RPC %d received another caller's response: %v", i, response)
			}
		})
	}
	for node.runtime.Snapshot().Pending != count {
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
	if len(sizes) != 1+count/limits.BatchOperations || sizes[0] != 1 {
		t.Fatal("independent RPCs were not coalesced into bounded adapter executions", sizes)
	}
	for _, size := range sizes[1:] {
		if size != limits.BatchOperations {
			t.Fatal("single-record RPCs did not fill physical batches", sizes)
		}
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
}
