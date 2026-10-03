package testutil

import (
	"strings"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/protobuf/proto"
)

// RecordFixture builds one Store-relative request for adapter conformance tests.
type RecordFixture struct {
	StoreName string
	Operation *pb.Operation
}

func RecordCommand(message proto.Message) RecordFixture {
	operation := &pb.Operation{Index: 1}
	var resource *string
	switch request := proto.Clone(message).(type) {
	case *pb.ReadRequest:
		resource = &request.Resource
		operation.Operation = &pb.Operation_Read{Read: request}
	case *pb.MutateRequest:
		resource = &request.Resource
		operation.Operation = &pb.Operation_Mutate{Mutate: request}
	default:
		panic("record fixture requires a read or mutation request")
	}
	store, target, _ := strings.Cut(strings.TrimPrefix(*resource, "weir://"), "/")
	*resource = target
	fixture := RecordFixture{StoreName: store, Operation: operation}
	return fixture
}
