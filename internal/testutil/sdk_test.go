package testutil

import (
	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"testing"
)

func TestMutationFixturesValidateTypedRequests(t *testing.T) {
	action := &pb.MutateRequest_AtomicTransform{}
	mutation := &pb.MutateRequest{Resource: "records/s:key", Action: action}
	if err := protocol.ValidateMutationRequest(mutation); err == nil {
		t.Fatal("missing transform accepted")
	}
}
