package server

import (
	"context"
	"io"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type admissionStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (stream *admissionStream) Context() context.Context { return stream.ctx }

type receiverStatusStream struct {
	admissionStream
	request *pb.ExecuteRequest
	block   bool
	entered chan struct{}
}

func (stream *receiverStatusStream) Recv() (*pb.ExecuteRequest, error) {
	if stream.request != nil {
		request := stream.request
		stream.request = nil
		return request, nil
	}
	if stream.block {
		close(stream.entered)
		<-stream.ctx.Done()
		return nil, status.FromContextError(stream.ctx.Err()).Err()
	}
	return nil, io.EOF
}

func (stream *receiverStatusStream) Send(*pb.ExecuteResponse) error {
	<-stream.entered
	return status.Error(codes.Internal, "output failed")
}

func TestCompletedRecordReceiverPreservesNativeTerminalStatus(t *testing.T) {
	_, runtime := localStore(t, "records")
	opts := testServerOptions{stores: map[string]*store.Runtime{"records": runtime}}
	server, _ := startTestServer(t, opts)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	key := rpcCancelKey{}
	ctx = context.WithValue(ctx, key, cancel)
	base := admissionStream{ctx: ctx}
	stream := &receiverStatusStream{admissionStream: base, request: readFrame(2, testRequest())}
	if err := server.Execute(stream); status.Code(err) != codes.InvalidArgument || ctx.Err() != nil {
		t.Fatal("receiver status raced with native RPC cancellation", err, ctx.Err())
	}
}

func TestFailedRecordOutputStillJoinsBlockedNativeReceiver(t *testing.T) {
	_, runtime := localStore(t, "records")
	opts := testServerOptions{stores: map[string]*store.Runtime{"records": runtime}}
	server, _ := startTestServer(t, opts)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	key := rpcCancelKey{}
	ctx = context.WithValue(ctx, key, cancel)
	base := admissionStream{ctx: ctx}
	stream := &receiverStatusStream{admissionStream: base, request: readFrame(1, testRequest()), block: true, entered: make(chan struct{})}
	if err := server.Execute(stream); status.Code(err) != codes.Internal || ctx.Err() != context.Canceled {
		t.Fatal("failed output did not cancel/join an outstanding native Recv", err, ctx.Err())
	}
}
