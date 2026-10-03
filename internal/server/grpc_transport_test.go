package server

import (
	"context"
	"github.com/batchstream/weir-protocol/api/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/tap"
)

func TestTapRejectsExpiredAndReclaimsUndispatchedRPC(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, _ := startPeerServer(t, opts)
	info := &tap.Info{FullMethodName: pb.StoreService_Read_FullMethodName}
	for range 50 {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := server.admitRPC(ctx, info); err == nil {
			t.Fatal("expired tap admitted")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	accepted, err := server.admitRPC(ctx, info)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	waitPeerIdle(t, server)
	if accepted.Err() == nil {
		t.Fatal("RPC cancellation missing")
	}
}

func TestDispatchedRPCAdmissionRemainsUntilStatsEnd(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, _ := startPeerServer(t, opts)
	ctx, cancel := context.WithCancel(t.Context())
	info := &tap.Info{FullMethodName: pb.StoreService_Read_FullMethodName}
	accepted, err := server.admitRPC(ctx, info)
	if err != nil {
		t.Fatal(err)
	}
	tags := &stats.RPCTagInfo{FullMethodName: info.FullMethodName}
	handler := transportStats{}
	handler.TagRPC(accepted, tags)
	cancel()
	state := accepted.Value(rpcKey).(*rpcState)
	state.cancelBeforeDispatch(context.Canceled)
	if len(server.slots) != 1 {
		t.Fatal("active handler admission released on caller cancellation")
	}
	end := &stats.End{Error: context.Canceled, EndTime: time.Now()}
	handler.HandleRPC(accepted, end)
	waitPeerIdle(t, server)
}

func TestNativeBatchDecodeLimitFailsBeforeBackendWork(t *testing.T) {
	adapter, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	count := protocol.MaxBatchResponseBytes/protocol.ResultOverhead + 1
	read := &pb.ReadRequest{Resource: "records/s:key"}
	requests := make([]*pb.ReadRequest, count)
	for i := range requests {
		requests[i] = read
	}
	request := &pb.ReadBatchRequest{StoreName: "records", Requests: requests}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	response, err := client.Read(ctx, request)
	// Native gRPC wraps codec errors as Internal before the unary interceptor.
	if response != nil || status.Code(err) != codes.Internal || !strings.Contains(status.Convert(err).Message(), "request metadata exceeds decode budget") {
		t.Fatal("native decode status changed", response, err)
	}
	if len(adapter.seen) != 0 || adapter.commands.Load() != 0 || local.Snapshot().Retained != 0 {
		t.Fatal("over-budget raw request reached backend")
	}
	waitPeerIdle(t, server)
}
