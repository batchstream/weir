package testutil

import (
	"strings"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/routeclient"
	"google.golang.org/protobuf/proto"
)

// RecordCall builds Route fixtures from the canonical resources also used by
// private backend conformance tests. The wire target is always Store-relative.
// This helper is only for repository test harnesses; it is not a client API.
func RecordCall(message proto.Message) routeclient.RecordOptions {
	var resource string
	call := &pb.Call{Version: 1}
	switch request := proto.Clone(message).(type) {
	case *pb.ReadRequest:
		resource = request.Resource
		variant := &pb.Call_Read{Read: request}
		call.Operation = variant
	case *pb.MutateRequest:
		resource = request.Resource
		variant := &pb.Call_Mutate{Mutate: request}
		call.Operation = variant
	default:
		panic("RecordCall requires a record fixture")
	}
	destination, target, _ := strings.Cut(strings.TrimPrefix(resource, "weir://"), "/")
	if read := call.GetRead(); read != nil {
		read.Resource = target
	} else {
		call.GetMutate().Resource = target
	}
	opts := routeclient.RecordOptions{Destination: destination, Call: call}
	return opts
}
