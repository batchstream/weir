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

// BackendBatchBytes bounds one native database exchange, independently of the
// number of records in a logical client stream.
const BackendBatchBytes = 32 << 20

// Operation associates a validated request with its position in the current
// Store window. The public stream ordinal is assigned at publication.
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

// Result retains one positional terminal for the current Store window. Execute
// publishes its typed result with the corresponding public stream ordinal.
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
