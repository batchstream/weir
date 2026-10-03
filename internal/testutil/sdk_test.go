package testutil

import (
	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"testing"
)

func TestReadAndMutateFixturesValidateTypedBatchRequests(t *testing.T) {
	action := &pb.MutateRequest_AtomicTransform{}
	mutation := &pb.MutateRequest{Resource: "records/s:key", Action: action}
	request := &pb.MutateBatchRequest{StoreName: "store", Requests: []*pb.MutateRequest{mutation}}
	if err := protocol.ValidateMutateBatchRequest(request); err == nil {
		t.Fatal("missing transform accepted")
	}
}
