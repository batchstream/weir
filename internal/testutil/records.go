package testutil

import (
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"google.golang.org/protobuf/proto"
)

// RecordFixture submits one relative request through its public batch RPC.
type RecordFixture struct {
	StoreName string
	Operation *execution.Operation
}

func RecordRequest(store string, message proto.Message) RecordFixture {
	operation := &execution.Operation{Index: 1}
	switch request := message.(type) {
	case *pb.ReadRequest:
		operation.Read = request
	case *pb.MutateRequest:
		operation.Mutate = request
	default:
		panic("record fixture requires a read or mutation request")
	}
	fixture := RecordFixture{StoreName: store, Operation: operation}
	return fixture
}
