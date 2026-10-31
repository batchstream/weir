package server

import (
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
)

func TestTapAdmitsOnlyServedMethodsWhileAccepting(t *testing.T) {
	_, local := localStore(t, "records")
	opts := testServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, _ := startTestServer(t, opts)
	execute := &tap.Info{FullMethodName: pb.StoreService_Execute_FullMethodName}
	unknown := &tap.Info{FullMethodName: "/weir.v1.StoreService/Other"}
	if _, err := server.admitRPC(t.Context(), unknown); status.Code(err) != codes.Unimplemented {
		t.Fatal("listener admitted an unserved method", err)
	}
	accepted, err := server.admitRPC(t.Context(), execute)
	if err != nil {
		t.Fatal(err)
	}
	if !endRPC(accepted) || accepted.Err() != context.Canceled || endRPC(accepted) {
		t.Fatal("admitted RPC cannot be ended exactly once", accepted.Err())
	}
	server.admission.SetOverloaded(true)
	if _, err := server.admitRPC(t.Context(), execute); status.Code(err) != codes.Unavailable {
		t.Fatal("overloaded process admitted an RPC", err)
	}
	server.admission.SetOverloaded(false)
	server.admission.BeginDrain()
	if _, err := server.admitRPC(t.Context(), execute); status.Code(err) != codes.Unavailable {
		t.Fatal("draining process admitted an RPC", err)
	}
}

func TestConnectionAgeEndsConnectionsWithoutCuttingStreams(t *testing.T) {
	adapter, local := localStore(t, "records")
	block := make(chan struct{})
	adapter.block = block
	limits := DefaultLimits()
	limits.Stall = 5 * time.Second
	limits.MaxConnectionAge = 200 * time.Millisecond
	opts := testServerOptions{stores: map[string]*store.Runtime{"records": local}, limits: limits}
	_, address := startTestServer(t, opts)
	conn, client := testClient(t, address)
	held := make(chan error, 1)
	go func() {
		_, err := routeRead(client, t.Context(), testRequest())
		held <- err
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	select {
	case <-adapter.seen:
	case <-ctx.Done():
		t.Fatal("held read did not start")
	}
	// GOAWAY moves the client off its aged connection while the held stream
	// keeps running, so the next call reconnects and re-resolves.
	for state := conn.GetState(); state == connectivity.Ready; state = conn.GetState() {
		if !conn.WaitForStateChange(ctx, state) {
			t.Fatal("connection outlived its maximum age")
		}
	}
	close(block)
	if err := <-held; err != nil {
		t.Fatal("an aged connection cut its running stream", err)
	}
	if _, err := routeRead(client, t.Context(), testRequest()); err != nil {
		t.Fatal("call after GOAWAY did not reconnect", err)
	}
}
