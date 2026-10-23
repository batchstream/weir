package server

import (
	"context"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
)

type rpcCancelKey struct{}

// admitRPC rejects unserved methods, draining and overload before gRPC reads
// any request data. The returned context becomes the parent of the stream's
// context, so canceling it also ends a blocked Recv or flow-controlled Send.
func (s *Server) admitRPC(ctx context.Context, info *tap.Info) (context.Context, error) {
	if info.FullMethodName != pb.StoreService_Execute_FullMethodName {
		s.admission.rejections.WithLabelValues("method").Inc()
		return nil, status.Error(codes.Unimplemented, "method is not served")
	}
	if err := s.admission.check(); err != nil {
		return nil, err
	}
	rpc, cancel := context.WithCancel(ctx)
	key := rpcCancelKey{}
	tagged := context.WithValue(rpc, key, cancel)
	return tagged, nil
}

// endRPC cancels the RPC whose context is ctx and reports whether it was still running.
func endRPC(ctx context.Context) bool {
	key := rpcCancelKey{}
	cancel, ok := ctx.Value(key).(context.CancelFunc)
	if !ok || ctx.Err() != nil {
		return false
	}
	cancel()
	return true
}

// cancelRPC ends a stalled RPC.
func (s *Server) cancelRPC(ctx context.Context, phase string) {
	if endRPC(ctx) {
		s.metrics.watchdogs.WithLabelValues(phase).Inc()
	}
}

// observeStream counts Execute, the only method admitRPC lets through.
func (s *Server) observeStream(service any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	s.admission.activeRPCs.Add(1)
	defer s.admission.activeRPCs.Add(-1)
	err := handler(service, stream)
	s.metrics.rpcs.WithLabelValues("execute", statusLabel(err)).Inc()
	return err
}
