package testutil

import (
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestSDKCommandRejectsMissingTransform(t *testing.T) {
	action := &pb.MutateRequest_AtomicTransform{}
	mutation := &pb.MutateRequest{Resource: "records/s:key", Action: action}
	operation := &pb.Command_Mutate{Mutate: mutation}
	command := &pb.Command{Version: 1, Operation: operation}
	result, err := SDKCommand(command)
	if err == nil || result != nil {
		t.Fatal("missing transform was accepted by the SDK fixture bridge", result, err)
	}
}
