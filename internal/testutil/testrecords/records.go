// Package testrecords prepares backend fixtures through public request validation.
package testrecords

import (
	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func New(store string, input *pb.ExecuteRequest) (*execution.Record, *pb.Failure) {
	if input == nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "missing record fixture")
	}
	request := &pb.ExecuteRequest{StoreName: store, Index: input.Index, Command: input.Command}
	if err := protocol.ValidateExecuteRequest(request); err != nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, err.Error())
	}
	record, err := execution.NewRecord(store, input.Index, input.Command)
	if err != nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, err.Error())
	}
	return record, nil
}

func Prepare(adapter execution.Adapter, store string, request *pb.ExecuteRequest) (*execution.Plan, *pb.Failure) {
	record, failure := New(store, request)
	if failure != nil {
		return nil, failure
	}
	return adapter.PrepareRecord(record)
}
