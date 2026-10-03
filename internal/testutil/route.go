package testutil

import (
	"strings"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/protobuf/proto"
)

// RecordCommand builds Execute fixtures from the canonical resources also used by
// private backend conformance tests. The wire target is always Store-relative.
// This helper is only for repository test harnesses; it is not a client API.
func RecordCommand(message proto.Message) RecordFixture {
	var resource string
	call := &pb.Command{Version: 1}
	switch request := proto.Clone(message).(type) {
	case *pb.ReadRequest:
		resource = request.Resource
		variant := &pb.Command_Read{Read: request}
		call.Operation = variant
	case *pb.MutateRequest:
		resource = request.Resource
		variant := &pb.Command_Mutate{Mutate: request}
		call.Operation = variant
	default:
		panic("RecordCommand requires a record fixture")
	}
	destination, target, _ := strings.Cut(strings.TrimPrefix(resource, "weir://"), "/")
	if read := call.GetRead(); read != nil {
		read.Resource = target
	} else {
		call.GetMutate().Resource = target
	}
	opts := RecordFixture{StoreName: destination, Command: call}
	return opts
}
