package server

import (
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
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
