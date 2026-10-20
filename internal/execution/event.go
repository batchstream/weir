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

// gRPC's native buffer invokes its owner only above its 1024-byte pooling
// threshold. Smaller protobufs still allocate this much tracked capacity.
const MinimumWireBytes = 1025
const RecordStreamItems = 32

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

// ReservationFailure preserves cancellation evidence from a context-aware
// reservation without changing the caller's separately established outcome.
func ReservationFailure(ctx context.Context, err error) *pb.Failure {
	if ctx.Err() != nil {
		return protocol.ContextFailure(ctx)
	}
	if errors.Is(err, context.Canceled) {
		return protocol.Fail(pb.FailureCode_CANCELLED, "allocation canceled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return protocol.Fail(pb.FailureCode_DEADLINE_EXCEEDED, "allocation deadline exceeded")
	}
	return protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, ErrExecutionMemory.Error())
}

// LuaFailure reports an evaluation failure before any backend write.
func LuaFailure(ctx context.Context, err error) *pb.MutationResult {
	if ctx.Err() != nil {
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.ContextFailure(ctx))
	}
	code := pb.FailureCode_INVALID_ARGUMENT
	message := "Lua program evaluation failed"
	if errors.Is(err, ErrExecutionMemory) {
		code = pb.FailureCode_RESOURCE_EXHAUSTED
		message = ErrExecutionMemory.Error()
	} else if errors.Is(err, context.DeadlineExceeded) {
		code = pb.FailureCode_DEADLINE_EXCEEDED
		message = "Lua program execution limit exceeded"
	}
	failure := protocol.Fail(code, message)
	return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
}
