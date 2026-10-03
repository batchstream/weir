package server

import (
	"context"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func failureStatus(failure *pb.Failure) error {
	code := codes.InvalidArgument
	switch failure.Code {
	case pb.FailureCode_RESOURCE_EXHAUSTED:
		code = codes.ResourceExhausted
	case pb.FailureCode_UNAVAILABLE:
		code = codes.Unavailable
	case pb.FailureCode_INTERNAL:
		code = codes.Internal
	case pb.FailureCode_UNSUPPORTED:
		code = codes.Unimplemented
	case pb.FailureCode_CANCELLED:
		code = codes.Canceled
	case pb.FailureCode_DEADLINE_EXCEEDED:
		code = codes.DeadlineExceeded
	}
	return status.Error(code, failure.Message)
}

func (s *Server) Read(ctx context.Context, request *pb.ReadBatchRequest) (*pb.ReadBatchResponse, error) {
	byteLimit := protocol.MaxBatchRequestBytes
	if runtime := s.stores[request.GetStoreName()]; runtime != nil {
		byteLimit = runtime.PendingByteLimit()
	}
	records, failure := execution.NewReadRecords(request, byteLimit)
	if failure != nil {
		return nil, failureStatus(failure)
	}
	runtime, err := s.hostedStore(request.StoreName)
	if err != nil {
		return nil, err
	}
	prepared, failure := runtime.PrepareBatch(records)
	if failure != nil {
		return nil, failureStatus(failure)
	}
	ticket, err := s.submitBatch(ctx, runtime, prepared)
	if err != nil {
		return nil, err
	}
	state, ok := ctx.Value(rpcKey).(*rpcState)
	if !ok {
		defer ticket.Ack()
	} else {
		state.retain(ticket)
	}
	results, err := ticket.WaitBatch(ctx)
	if err != nil {
		return nil, status.FromContextError(err).Err()
	}
	response := &pb.ReadBatchResponse{Results: make([]*pb.ReadResult, len(results))}
	for i, result := range results {
		response.Results[i] = result.GetRead()
	}
	if err := protocol.ValidateReadBatchResponse(response, len(request.Requests)); err != nil {
		return nil, status.Error(codes.Internal, "invalid read batch result: "+err.Error())
	}
	return response, nil
}

func (s *Server) Mutate(ctx context.Context, request *pb.MutateBatchRequest) (*pb.MutateBatchResponse, error) {
	byteLimit := protocol.MaxBatchRequestBytes
	if runtime := s.stores[request.GetStoreName()]; runtime != nil {
		byteLimit = runtime.PendingByteLimit()
	}
	records, failure := execution.NewMutationRecords(request, byteLimit)
	if failure != nil {
		return nil, failureStatus(failure)
	}
	runtime, err := s.hostedStore(request.StoreName)
	if err != nil {
		return nil, err
	}
	prepared, failure := runtime.PrepareBatch(records)
	if failure != nil {
		return nil, failureStatus(failure)
	}
	ticket, err := s.submitBatch(ctx, runtime, prepared)
	if err != nil {
		return nil, err
	}
	state, ok := ctx.Value(rpcKey).(*rpcState)
	if !ok {
		defer ticket.Ack()
	} else {
		state.retain(ticket)
	}
	results, err := ticket.WaitBatch(ctx)
	if err != nil {
		return nil, status.FromContextError(err).Err()
	}
	response := &pb.MutateBatchResponse{Results: make([]*pb.MutationResult, len(results))}
	for i, result := range results {
		response.Results[i] = result.GetMutation()
	}
	if err := protocol.ValidateMutateBatchResponse(response, len(request.Requests)); err != nil {
		return nil, status.Error(codes.Internal, "invalid mutation batch result: "+err.Error())
	}
	return response, nil
}
