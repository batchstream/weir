package execution

import (
	"context"
	"errors"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// Entry and terminal metadata reservations bound server bookkeeping.
const EntryOverheadBytes = 512
const ResultOverheadBytes = 512

// BackendBatchBytes bounds a native database exchange independently of stream length.
const BackendBatchBytes = 32 << 20

func FailedEvent(command *pb.Command, outcome pb.MutationOutcome, failure *pb.Failure) *pb.Event {
	event := &pb.Event{}
	if command.GetRead() != nil {
		event.Value = &pb.Event_ReadResult{ReadResult: protocol.ReadFailure(failure)}
	} else {
		event.Value = &pb.Event_MutationResult{MutationResult: protocol.Mutation(outcome, failure)}
	}
	return event
}

// LuaFailure reports an evaluation failure before any backend write.
func LuaFailure(ctx context.Context, err error) *pb.MutationResult {
	if ctx.Err() != nil {
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.ContextFailure(ctx))
	}
	code := pb.FailureCode_INVALID_ARGUMENT
	message := "Lua program evaluation failed"
	if errors.Is(err, context.DeadlineExceeded) {
		code = pb.FailureCode_DEADLINE_EXCEEDED
		message = "Lua program execution limit exceeded"
	}
	failure := protocol.Fail(code, message)
	return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
}
