// Package testrecords translates backend fixtures into real public batches.
package testrecords

import (
	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

// New validates a fixture request through the production record constructor.
func New(store string, operation *execution.Operation) (*execution.Record, *pb.Failure) {
	if operation == nil || operation.Index == 0 || (operation.Read == nil) == (operation.Mutate == nil) {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid record fixture")
	}
	var err error
	if operation.Read != nil {
		err = protocol.ValidateReadRequest(operation.Read)
	} else {
		err = protocol.ValidateMutationRequest(operation.Mutate)
	}
	if err != nil || !protocol.ValidStoreName(store) {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid record fixture request")
	}
	record, err := execution.NewRecord(store, operation)
	if err != nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, err.Error())
	}
	return record, nil
}

func Prepare(adapter execution.Adapter, store string, operation *execution.Operation) (*execution.Plan, *pb.Failure) {
	record, failure := New(store, operation)
	if failure != nil {
		return nil, failure
	}
	return adapter.PrepareRecord(record)
}
