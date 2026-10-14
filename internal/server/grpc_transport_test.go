package server

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
)

func TestTapRejectsExpiredAndReclaimsUndispatchedRPC(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, _ := startPeerServer(t, opts)
	info := &tap.Info{FullMethodName: pb.StoreService_Execute_FullMethodName}
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

func TestDispatchedRPCAdmissionRemainsWhileHandlerWorks(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, _ := startPeerServer(t, opts)
	ctx, cancel := context.WithCancel(t.Context())
	info := &tap.Info{FullMethodName: pb.StoreService_Execute_FullMethodName}
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
	if server.admission.activeRPCs.Load() != 1 {
		t.Fatal("active handler admission released on caller cancellation")
	}
	end := &stats.End{Error: context.Canceled, EndTime: time.Now()}
	handler.HandleRPC(accepted, end)
	waitPeerIdle(t, server)
}

type admissionStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (stream *admissionStream) Context() context.Context { return stream.ctx }

func TestRecordFrameDecodeLimitFailsBeforeBackendWork(t *testing.T) {
	adapter, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	read := &pb.ReadRequest{Resource: "records/s:key"}
	command := &pb.Command{Operation: &pb.Command_Read{Read: read}}
	request := &pb.ExecuteRequest{StoreName: "records", Index: 1, Command: command}
	request.ProtoReflect().SetUnknown([]byte{0x1a, 0})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	stream, err := client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	// Native gRPC wraps codec errors as Internal before the stream handler.
	if response != nil || status.Code(err) != codes.Internal || !strings.Contains(status.Convert(err).Message(), "invalid execution protobuf framing") {
		t.Fatal("native decode status changed", response, err)
	}
	if len(adapter.seen) != 0 || adapter.commands.Load() != 0 || local.Snapshot().Retained != 0 {
		t.Fatal("over-budget raw request reached backend")
	}
	waitPeerIdle(t, server)
}
