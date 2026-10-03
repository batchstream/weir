// Package testrecords translates backend fixtures into real public batches.
package testrecords

import (
	"strings"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"google.golang.org/protobuf/proto"
)

// New accepts existing backend fixture URIs and correlation indices. It runs
// the production constructor, assigning the requested index by choosing that
// position in a valid public batch rather than fabricating a validated Record.
func New(store string, operation *pb.Operation) (*execution.Record, *pb.Failure) {
	if operation == nil || operation.Index > uint64(protocol.MaxBatchResponseBytes/protocol.ResultOverhead) {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid record fixture")
	}
	count := max(1, int(operation.Index))
	var records []*execution.Record
	var failure *pb.Failure
	if input := operation.GetRead(); input != nil {
		read := proto.Clone(input).(*pb.ReadRequest)
		read.Resource = strings.TrimPrefix(read.Resource, "weir://"+store+"/")
		items := make([]*pb.ReadRequest, count)
		for i := range items {
			items[i] = read
		}
		request := &pb.ReadBatchRequest{StoreName: store, Requests: items}
		records, failure = execution.NewReadRecords(request, protocol.MaxBatchRequestBytes)
	} else if input := operation.GetMutate(); input != nil {
		mutation := proto.Clone(input).(*pb.MutateRequest)
		mutation.Resource = strings.TrimPrefix(mutation.Resource, "weir://"+store+"/")
		items := make([]*pb.MutateRequest, count)
		for i := range items {
			items[i] = mutation
		}
		request := &pb.MutateBatchRequest{StoreName: store, Requests: items}
		records, failure = execution.NewMutationRecords(request, protocol.MaxBatchRequestBytes)
	} else {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "missing record fixture")
	}
	if failure != nil {
		return nil, failure
	}
	return records[count-1], nil
}

func Prepare(adapter execution.Adapter, store string, operation *pb.Operation) (*execution.Plan, *pb.Failure) {
	record, failure := New(store, operation)
	if failure != nil {
		return nil, failure
	}
	return adapter.PrepareRecord(record)
}
