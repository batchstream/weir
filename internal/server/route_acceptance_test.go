package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
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
