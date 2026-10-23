package server

import (
	"context"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc/codes"
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
	if _, err := server.admitRPC(t.Context(), execute); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("overloaded process admitted an RPC", err)
	}
	server.admission.SetOverloaded(false)
	server.admission.BeginDrain()
	if _, err := server.admitRPC(t.Context(), execute); status.Code(err) != codes.Unavailable {
		t.Fatal("draining process admitted an RPC", err)
	}
}
