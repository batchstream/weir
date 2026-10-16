package store

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type nativeReadCapacityAdapter struct {
	lifecycleAdapter
	reads atomic.Int32
	held  int
	extra int
}

func (a *nativeReadCapacityAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) {
	work := plans[0]
	if work.Command.GetNative() == nil {
		event := execution.FailedEvent(work.Command, pb.MutationOutcome_APPLIED, nil)
		_ = emit(work, event)
		return
	}
	if err := work.NativeIO.GrowIO(a.held + (512 << 10)); err != nil {
		return
	}
	work.WorkingBytes = a.held
	if err := work.NativeIO.EndIO(work.WorkingBytes, 0); err != nil {
		return
	}
	head := &pb.NativeHead{BodyContentType: "application/http"}
	value := &pb.Event_Head{Head: head}
	event := &pb.Event{Value: value}
	if err := emit(work, event); err != nil {
		return
	}
	err := work.NativeIO.BeginIO(ctx, a.extra)
	code := pb.FailureCode_UNAVAILABLE
	if errors.Is(err, execution.ErrExecutionMemory) {
		code = pb.FailureCode_RESOURCE_EXHAUSTED
	}
	if err == nil {
		a.reads.Add(1)
		_ = work.NativeIO.EndIO(work.WorkingBytes, 0)
	}
	work.WorkingBytes = execution.EntryOverheadBytes
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	if err != nil {
		failure := protocol.Fail(code, "Native response cannot acquire body-read memory")
		end = protocol.NativeFailure(true, failure)
	}
	terminal := &pb.Event_NativeEnd{NativeEnd: end}
	event = &pb.Event{Value: terminal}
	_ = emit(work, event)
}

func TestNativeFastConsumersFailIncompleteWhenBodyWorkspaceCannotConverge(t *testing.T) {
	memory := fullResidentBorrowMemory(t)
	baseline := memory.Snapshot()
	// Legal short handoffs cannot fill R with arbitrary headers any more. A
	// body reader that genuinely needs more than the protected workspace must
	// still fail without a circular wait or acquiring an IO slot.
	adapter := &nativeReadCapacityAdapter{held: protocol.NativeChunk + execution.EntryOverheadBytes, extra: int(memory.reserve) + 1}
	limits := DefaultLimits()
	runtime, err := New(adapter, limits, memory)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	var sessions []*Session
	var heads []*Emission
	for i := range 2 {
		session := runtime.NewSession()
		defer session.Close()
		sessions = append(sessions, session)
		work := plan(uint64(i+1), "search", false)
		work.Command, work.WorkingBytes = nativeCall(), 4<<20
		if _, failure, _ := runtime.Submit(ctx, work, session); failure != nil {
			t.Fatal(failure)
		}
		select {
		case head := <-session.Events:
			if head.Event.GetHead() == nil {
				t.Fatal("Search-like exchange did not retain its response", head.Event)
			}
			heads = append(heads, head)
		case <-ctx.Done():
			t.Fatal("headers did not arrive")
		}
	}
	before := memory.Snapshot()
	if before.Borrowed != 2 || before.BudgetBytes-before.UsedBytes >= uint64(adapter.extra) || before.UsedBytes > before.BudgetBytes {
		t.Fatal("test did not fill actual output ownership", before)
	}
	// Both consumers are immediately ready. Their body reads cannot be the
	// source of the memory they need to start, and no backend IO is converging.
	for _, head := range heads {
		head.Release()
	}
	typed := plan(3, "typed", true)
	ticket, failure, _ := runtime.Submit(ctx, typed, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	if event, err := ticket.Wait(ctx); err != nil || event.GetReadResult() == nil {
		t.Fatal("full Native working memory blocked unrelated typed IO", event, err)
	}
	ticket.Ack()
	progress, stop := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer stop()
	incomplete := 0
	for _, session := range sessions {
		select {
		case emission := <-session.Events:
			end := emission.Event.GetNativeEnd()
			if end.GetCompletion() == pb.NativeCompletion_RESPONSE_INCOMPLETE && end.GetFailure().GetCode() == pb.FailureCode_RESOURCE_EXHAUSTED {
				incomplete++
			} else if end.GetCompletion() != pb.NativeCompletion_RESPONSE_COMPLETE {
				t.Fatal("body workspace failure lost already-attempted evidence", end)
			}
			emission.Release()
		case <-progress.Done():
			t.Fatal("fast Native consumers cyclically waited for body-read quota with no converging IO", before, runtime.Snapshot(), memory.Snapshot())
		}
	}
	if incomplete == 0 || incomplete+int(adapter.reads.Load()) != 2 {
		t.Fatal("full workspace did not yield incomplete evidence or a bounded continuation", incomplete, adapter.reads.Load())
	}
	for _, session := range sessions {
		session.Close()
	}
	waitReleased(t, runtime)
	if snapshot := memory.Snapshot(); snapshot.Count != baseline.Count || snapshot.UsedBytes != baseline.UsedBytes {
		t.Fatal("failed continuation retained working credit", snapshot)
	}
}
