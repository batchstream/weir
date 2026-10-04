package execution

import (
	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/protobuf/proto"
)

// Entry and terminal metadata reservations bound decoded bookkeeping beside
// request and result documents. They belong to the server admission model.
const EntryOverheadBytes = 512
const ResultOverheadBytes = 512

// Operation associates one validated request with its position in the owning
// RPC. It is an internal execution value and has no serialized representation.
type Operation struct {
	Index  uint64
	Read   *pb.ReadRequest
	Mutate *pb.MutateRequest
}

func (operation *Operation) Resource() string {
	if operation.Read != nil {
		return operation.Read.Resource
	}
	return operation.Mutate.Resource
}

// RequestBytes includes retained positional metadata beside the public request.
func (operation *Operation) RequestBytes() int {
	if operation.Read != nil {
		return proto.Size(operation.Read) + 16
	}
	return proto.Size(operation.Mutate) + 16
}

// Result retains one positional terminal for its owning RPC. The public Read or
// Mutate handler publishes the corresponding result without an internal envelope.
type Result struct {
	Index    uint64
	Read     *pb.ReadResult
	Mutation *pb.MutationResult
}

func FailedResult(operation *Operation, outcome pb.MutationOutcome, failure *pb.Failure) *Result {
	result := &Result{Index: operation.Index}
	if operation.Read != nil {
		result.Read = protocol.ReadFailure(failure)
	} else {
		result.Mutation = protocol.Mutation(outcome, failure)
	}
	return result
}
