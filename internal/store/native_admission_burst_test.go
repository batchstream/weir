package store

import (
	"context"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type nativeAdmissionBurstAdapter struct {
	lifecycleAdapter
	entered chan struct{}
	release <-chan struct{}
}

func (a *nativeAdmissionBurstAdapter) Execute(ctx context.Context, works []*execution.Plan, emit execution.Emit) {
	work := works[0]
	if work.Command.GetNative() == nil {
		event := execution.FailedEvent(work.Command, pb.MutationOutcome_APPLIED, nil)
		_ = emit(work, event)
		return
	}
	if work.Key != "short" {
		a.entered <- struct{}{}
		select {
		case <-a.release:
		case <-ctx.Done():
			return
		}
	}
	work.WorkingBytes = 1024
	head := &pb.NativeHead{BodyContentType: "application/bson"}
	value := &pb.Event_Head{Head: head}
	event := &pb.Event{Value: value}
	if err := emit(work, event); err != nil {
		return
	}
	work.WorkingBytes = execution.EntryOverheadBytes
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	terminal := &pb.Event_NativeEnd{NativeEnd: end}
	event = &pb.Event{Value: terminal}
	_ = emit(work, event)
}

func TestNativeTransientAdmissionWaitsForOtherStoreIOWithoutLostWake(t *testing.T) {
	const initialWorking = 48_000_000
	work := plan(1, "initial", false)
	work.Command, work.WorkingBytes = nativeCall(), initialWorking
	input := work.Bytes + max(work.ResultBytes, 2*protocol.NativeChunk+execution.ResultOverheadBytes)
	memory := NewExecutionMemory()
	memory.known, memory.trusted = true, true
	memory.budget = 2*uint64(initialWorking+input) + 64<<10
	release := make(chan struct{})
	adapter := &nativeAdmissionBurstAdapter{entered: make(chan struct{}, 2), release: release}
	limits := DefaultLimits()
	first, err := New(adapter, limits, memory)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(adapter, limits, memory)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	t.Cleanup(func() {
		cancel()
		closing, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := first.Close(closing); err != nil {
			t.Error(err)
		}
		if err := second.Close(closing); err != nil {
			t.Error(err)
		}
	})
	var initialSessions []*Session
	for i := range 2 {
		session := first.NewSession()
		defer session.Close()
		initialSessions = append(initialSessions, session)
		work.ID = uint64(i + 1)
		if _, failure, _ := first.Submit(ctx, work, session); failure != nil {
			t.Fatal(failure)
		}
		if i == 0 {
			select {
			case <-adapter.entered:
			case <-ctx.Done():
				t.Fatal("initial backend IO did not begin")
			}
		}
	}
	before := memory.Snapshot()
	if before.Starting != 1 || before.BudgetBytes-before.UsedBytes >= initialWorking || before.InputUsedBytes+uint64(work.Bytes+nativeQueuedWindow) > before.InputReservedBytes {
		t.Fatal("test did not isolate transient working pressure from input exhaustion", before)
	}
	session := second.NewSession()
	defer session.Close()
	short := plan(3, "short", false)
	short.Command, short.WorkingBytes = nativeCall(), initialWorking
	ticket, failure, _ := second.Submit(ctx, short, session)
	if ticket == nil || failure != nil || memory.Snapshot().Count != 3 || memory.Snapshot().BudgetRejections != 0 {
		t.Fatal("temporary initial IO reservations became an unowned wait or hard Native rejection", before, memory.Snapshot(), failure)
	}
	t.Logf("transient initial IO: before=%+v admitted=%+v", before, memory.Snapshot())
	for {
		second.mu.Lock()
		blocked := ticket.blocked
		second.mu.Unlock()
		if blocked {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("admitted input did not wait in the bounded second Store queue")
		case <-time.After(time.Millisecond):
		}
	}
	// No further Submit or Guard sample occurs: only the other Store's shared
	// memory signal can wake this already admitted, memory-blocked ticket.
	close(release)
	complete := false
	for {
		select {
		case emission := <-session.Events:
			event := emission.Event
			emission.Release()
			if event.GetNativeEnd() != nil {
				if event.GetNativeEnd().Completion != pb.NativeCompletion_RESPONSE_COMPLETE {
					t.Fatal("short Native did not return its confirmed complete result", event)
				}
				complete = true
			}
			if event == nil {
				if !complete {
					t.Fatal("completion marker arrived before NativeEnd")
				}
				ticket.Ack()
				goto completed
			}
		case <-ctx.Done():
			t.Fatal("short Native did not complete after real IO convergence", memory.Snapshot())
		}
	}
completed:
	session.Close()
	// The first Store's slow outputs are deliberately still alive here.
	if memory.Snapshot().BudgetRejections != 0 {
		t.Fatal("transient convergence was counted as a real budget rejection")
	}
	cancel()
	for _, initial := range initialSessions {
		initial.Close()
	}
	deadline := time.Now().Add(time.Second)
	for memory.Snapshot().Count != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 {
		t.Fatal("canceled burst retained Native lease ownership", snapshot)
	}
	t.Logf("all admitted inputs completed/canceled and released: %+v", memory.Snapshot())
}

type nativeSameBytesIOAdapter struct {
	lifecycleAdapter
	reading chan struct{}
	finish  <-chan struct{}
	extra   int
}

func (a *nativeSameBytesIOAdapter) Execute(ctx context.Context, works []*execution.Plan, emit execution.Emit) {
	work := works[0]
	head := &pb.NativeHead{BodyContentType: "application/http"}
	value := &pb.Event_Head{Head: head}
	event := &pb.Event{Value: value}
	if err := emit(work, event); err != nil {
		return
	}
	if err := work.NativeIO.BeginIO(ctx, a.extra); err != nil {
		return
	}
	close(a.reading)
	select {
	case <-a.finish:
	case <-ctx.Done():
		return
	}
	// The read's temporary allocation becomes response ownership without
	// changing total bytes. It still ends the last genuinely converging IO.
	work.WorkingBytes += a.extra
	if err := work.NativeIO.EndIO(work.WorkingBytes, 0); err != nil {
		return
	}
	<-ctx.Done()
}

func TestNativeSameByteIOConvergenceWakesOtherStoreWithoutNewSubmission(t *testing.T) {
	memory := testExecutionMemory(192 << 20)
	finish := make(chan struct{})
	adapter := &nativeSameBytesIOAdapter{reading: make(chan struct{}), finish: finish, extra: 2 << 20}
	limits := DefaultLimits()
	first, err := New(adapter, limits, memory)
	if err != nil {
		t.Fatal(err)
	}
	healthy := &singleExecutionAdapter{}
	second, err := New(healthy, limits, memory)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	t.Cleanup(func() {
		cancel()
		closing, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := first.Close(closing); err != nil {
			t.Error(err)
		}
		if err := second.Close(closing); err != nil {
			t.Error(err)
		}
	})
	session := first.NewSession()
	defer session.Close()
	work := plan(1, "held", false)
	work.Command = nativeCall()
	publication := max(work.ResultBytes, 2*protocol.NativeChunk+execution.ResultOverheadBytes) - nativeQueuedWindow
	work.WorkingBytes = int(memory.residentLimitLocked()-memory.inputTargetLocked()) - publication - adapter.extra
	if _, failure, _ := first.Submit(ctx, work, session); failure != nil {
		t.Fatal(failure)
	}
	select {
	case head := <-session.Events:
		if head.Event.GetHead() == nil {
			t.Fatal(head.Event)
		}
		head.Release()
	case <-ctx.Done():
		t.Fatal("held response did not begin")
	}
	select {
	case <-adapter.reading:
	case <-ctx.Done():
		t.Fatal("real body read did not acquire workspace")
	}
	before := memory.Snapshot()
	if before.UsedBytes != before.BudgetBytes-before.IOReserveBytes {
		t.Fatal("test did not fill the resident share with a real body read", before)
	}
	shortSession := second.NewSession()
	defer shortSession.Close()
	short := plan(2, "short", false)
	short.Command, short.WorkingBytes = nativeCall(), int(memory.reserve)+4096
	ticket, failure, _ := second.Submit(ctx, short, shortSession)
	if failure != nil {
		t.Fatal("IO spent promised input capacity", failure)
	}
	for {
		second.mu.Lock()
		blocked := ticket.blocked
		second.mu.Unlock()
		if blocked {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal("initial dispatch did not wait for real IO convergence")
		}
	}
	typed := plan(3, "typed", true)
	typedTicket, failure, _ := second.Submit(ctx, typed, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	if event, err := typedTicket.Wait(ctx); err != nil || event.GetReadResult() == nil {
		t.Fatal("memory wait hid eligible typed IO", event, err)
	}
	typedTicket.Ack()
	close(finish)
	select {
	case emission := <-shortSession.Events:
		end := emission.Event.GetNativeEnd()
		if end.GetCompletion() != pb.NativeCompletion_NATIVE_NOT_STARTED || end.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
			t.Fatal("same-byte convergence lost pre-attempt failure evidence", end)
		}
		emission.Release()
	case <-ctx.Done():
		t.Fatal("IO count reached zero without waking the other Store's bounded queue", memory.Snapshot())
	}
	after := memory.Snapshot()
	if after.UsedBytes != before.UsedBytes || after.BudgetRejections != 1 || memory.io != 0 {
		t.Fatal("convergence changed total ownership or failed to end IO", before, after)
	}
	t.Logf("same-byte real IO completion: before=%+v after=%+v", before, after)
	shortSession.Close()
	ticket.Ack()
}
