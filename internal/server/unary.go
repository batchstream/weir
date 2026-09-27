package server

import (
	"context"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) unary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	state, ok := ctx.Value(deliveryKey).(*delivery)
	if !ok {
		return nil, status.Error(codes.Internal, "missing HTTP/2 delivery lifetime")
	}
	result, err := handler(ctx, req)
	state.beginResponse()
	return result, err
}
func (s *Server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResult, error) {
	variant := &pb.BulkOperation_Read{Read: req}
	op := &pb.BulkOperation{Operation: variant}
	result, err := s.single(ctx, op)
	if err != nil {
		return nil, err
	}
	return result.GetRead(), nil
}
func (s *Server) Mutate(ctx context.Context, req *pb.MutateRequest) (*pb.MutationResult, error) {
	variant := &pb.BulkOperation_Mutate{Mutate: req}
	op := &pb.BulkOperation{Operation: variant}
	result, err := s.single(ctx, op)
	if err != nil {
		return nil, err
	}
	return result.GetMutation(), nil
}
func (s *Server) single(ctx context.Context, op *pb.BulkOperation) (*pb.BulkResult, error) {
	state, ok := ctx.Value(deliveryKey).(*delivery)
	if !ok {
		return nil, status.Error(codes.Internal, "missing HTTP/2 delivery lifetime")
	}
	service, name, failure := s.resolve(protocol.Resource(op), false)
	if failure == nil {
		failure = protocol.Validate(op, name)
		if failure != nil {
			s.admission.rejections.WithLabelValues("operation").Inc()
		}
	}
	if failure != nil {
		return protocol.ResultError(op, pb.MutationOutcome_NOT_STARTED, failure), nil
	}
	if service.RemoteWeir != nil {
		args := singleRelay{ctx: ctx, remote: service.RemoteWeir, operation: op, delivery: state}
		return s.remoteSingle(args)
	}
	runtime := service.LocalStore
	plan, f := runtime.Prepare(op)
	if f != nil {
		return protocol.ResultError(op, pb.MutationOutcome_NOT_STARTED, f), nil
	}
	ticket, f, _ := runtime.Submit(ctx, plan, nil)
	if f != nil {
		return protocol.ResultError(op, pb.MutationOutcome_NOT_STARTED, f), nil
	}
	result, err := ticket.Wait(ctx)
	if err != nil {
		return nil, status.FromContextError(err).Err()
	}
	state.retain(ticket)
	return result, nil
}
