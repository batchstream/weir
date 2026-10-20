package store

import (
	"context"
	"fmt"
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

type recordGrowthAdmissionAdapter struct {
	lifecycleAdapter
	entered    chan struct{}
	grow       <-chan struct{}
	results    chan error
	total      int
	native     *nativeAdmissionBurstAdapter
	allocation context.Context
}

func (a *recordGrowthAdmissionAdapter) Execute(ctx context.Context, works []*execution.Plan, emit execution.Emit) {
	if works[0].Command.GetNative() != nil {
		a.native.Execute(ctx, works, emit)
		return
	}
	close(a.entered)
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.grow:
			allocation := ctx
			if a.allocation != nil {
				allocation = a.allocation
			}
			a.results <- works[0].Memory.SetWorking(allocation, a.total)
		}
	}
}

func TestNativeStartupPreservesAdmittedRecordGrowth(t *testing.T) {
	t.Run("same-runtime", func(t *testing.T) { testNativeStartupRecordGrowth(t, true, "") })
	t.Run("cross-runtime", func(t *testing.T) { testNativeStartupRecordGrowth(t, false, "") })
	t.Run("allocation-canceled", func(t *testing.T) { testNativeStartupRecordGrowth(t, false, "canceled") })
	t.Run("allocation-deadline", func(t *testing.T) { testNativeStartupRecordGrowth(t, false, "deadline") })
}

func testNativeStartupRecordGrowth(t *testing.T, same bool, cancellation string) {
	const recordInitial = 48_000_000 + 3066
	const nativeInitial = 48_000_000 + 8192
	const growth = 64 << 10
	const headroom = 8192
	memory := NewExecutionMemory()
	memory.known, memory.trusted = true, true
	native := plan(2, "initial", false)
	native.Command, native.WorkingBytes, native.IOReserve = nativeCall(), nativeInitial, defaultNativeIOReserve
	publication := max(native.ResultBytes, 2*protocol.NativeChunk+execution.ResultOverheadBytes) - nativeQueuedWindow
	memory.budget = uint64(DefaultLimits().QueueBytes + recordInitial + nativeInitial + publication + headroom)
	grow := make(chan struct{})
	recordAdapter := &recordGrowthAdmissionAdapter{entered: make(chan struct{}), grow: grow, results: make(chan error, 1), total: recordInitial + growth}
	release := make(chan struct{})
	nativeAdapter := &nativeAdmissionBurstAdapter{entered: make(chan struct{}, 1), release: release}
	recordAdapter.native = nativeAdapter
	limits := DefaultLimits()
	recordRuntime, err := New(recordAdapter, limits, memory)
	if err != nil {
		t.Fatal(err)
	}
	nativeRuntime := recordRuntime
	if !same {
		nativeRuntime, err = New(nativeAdapter, limits, memory)
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	allocation, cancelAllocation := context.WithTimeout(ctx, time.Second)
	if cancellation == "deadline" {
		cancelAllocation()
		allocation, cancelAllocation = context.WithTimeout(ctx, 100*time.Millisecond)
	}
	defer cancelAllocation()
	recordAdapter.allocation = allocation
	var recordTicket *Ticket
	t.Cleanup(func() {
		if recordTicket != nil {
			recordTicket.Abandon()
		}
		cancel()
		closing, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := recordRuntime.Close(closing); err != nil {
			t.Error(err)
		}
		if err := nativeRuntime.Close(closing); err != nil {
			t.Error(err)
		}
		if after := memory.Snapshot(); after.Count != 0 || after.UsedBytes != 0 || after.ResidentBytes != 0 || after.InputUsedBytes != 0 || after.WireBytes != 0 || after.NativeCount != 0 {
			t.Error("real runtime release retained ledger ownership", after)
		} else {
			t.Logf("real runtime release: %+v", after)
		}
	})
	record := plan(1, "record", false)
	record.WorkingBytes, record.IOReserve = 3066, 48_000_000
	ticket, failure, _ := recordRuntime.Submit(ctx, record, nil)
	recordTicket = ticket
	if failure != nil {
		t.Fatal(failure)
	}
	select {
	case <-recordAdapter.entered:
	case <-ctx.Done():
		t.Fatal("record did not begin")
	}
	session := nativeRuntime.NewSession()
	defer session.Close()
	if _, failure, _ := nativeRuntime.Submit(ctx, native, session); failure != nil {
		t.Fatal(failure)
	}
	select {
	case <-nativeAdapter.entered:
	case <-ctx.Done():
		t.Fatal("Native startup did not begin")
	}
	before := memory.Snapshot()
	if before.BudgetBytes-before.UsedBytes != headroom || before.Starting != 2 {
		t.Fatal("did not isolate speculative Native startup pressure", before)
	}
	grow <- struct{}{}
	for {
		memory.mu.Lock()
		waiting := len(memory.workingWaits) == 1
		memory.mu.Unlock()
		if waiting {
			break
		}
		select {
		case early := <-recordAdapter.results:
			t.Fatal("record growth failed instead of awaiting captured Native startup", early)
		case <-ctx.Done():
			t.Fatal("record growth did not acquire its bounded scheduling claim")
		case <-time.After(time.Millisecond):
		}
	}
	if snapshot := memory.Snapshot(); snapshot.UsedBytes != before.UsedBytes || snapshot.BudgetRejections != 0 {
		t.Fatal("waiting allocated credit or counted a rejection", before, snapshot)
	}
	if cancellation != "" {
		if cancellation == "canceled" {
			cancelAllocation()
		}
		select {
		case result := <-recordAdapter.results:
			if result != allocation.Err() || result == nil {
				t.Fatal("allocation context did not interrupt growth", result, allocation.Err())
			}
		case <-ctx.Done():
			t.Fatal("growth used the batch context instead of allocation context")
		}
		memory.mu.Lock()
		claims := len(memory.workingWaits)
		memory.mu.Unlock()
		if claims != 0 || memory.Snapshot().UsedBytes != before.UsedBytes {
			t.Fatal("canceled growth retained a claim or changed real ownership")
		}
	}
	close(release)
	for {
		select {
		case emission := <-session.Events:
			terminal := emission.Event.GetNativeEnd()
			emission.Release()
			if terminal != nil {
				if terminal.GetCompletion() != pb.NativeCompletion_RESPONSE_COMPLETE {
					t.Fatal("Native did not converge successfully", terminal)
				}
				goto converged
			}
		case <-ctx.Done():
			t.Fatal("Native did not release speculative startup ownership")
		}
	}
converged:
	if cancellation != "" {
		return
	}
	select {
	case result := <-recordAdapter.results:
		if result != nil {
			t.Fatal("admitted growth failed after real Native convergence", result, memory.Snapshot())
		}
	case <-ctx.Done():
		t.Fatal("record did not progress after convergence")
	}
	t.Logf("admitted record growth after Native convergence: %+v", memory.Snapshot())
}

type coalescedGrowthAdapter struct {
	lifecycleAdapter
	entered chan []*execution.Plan
	grow    <-chan struct{}
	results chan error
}

func (a *coalescedGrowthAdapter) Execute(ctx context.Context, works []*execution.Plan, emit execution.Emit) {
	a.entered <- works
	select {
	case <-a.grow:
	case <-ctx.Done():
		return
	}
	if len(works) != 2 {
		a.results <- execution.ErrExecutionMemory
		return
	}
	// Match real adapter accounting: the seed owns the entire batch while
	// the sibling's allocation context bounds this particular operation.
	err := works[0].Memory.SetWorking(works[1].Context, growthRecordInitial+64<<10)
	a.results <- err
	if err != nil {
		return
	}
	for _, work := range works {
		event := execution.FailedEvent(work.Command, pb.MutationOutcome_APPLIED, nil)
		_ = emit(work, event)
	}
}

func TestCoalescedSiblingGrowthSurvivesCanceledAccountingSeed(t *testing.T) {
	for _, waiting := range []bool{false, true} {
		t.Run(map[bool]string{false: "immediate", true: "Native-convergence"}[waiting], func(t *testing.T) {
			memory := NewExecutionMemory()
			memory.known, memory.trusted = true, true
			publication := 2*protocol.NativeChunk + execution.ResultOverheadBytes - nativeQueuedWindow
			memory.budget = uint64(DefaultLimits().QueueBytes + growthRecordInitial + growthNativeInitial + publication + 8192)
			grow := make(chan struct{})
			adapter := &coalescedGrowthAdapter{entered: make(chan []*execution.Plan, 1), grow: grow, results: make(chan error, 1)}
			limits := DefaultLimits()
			limits.BatchOperations, limits.BatchWait = 2, time.Hour
			records, err := New(adapter, limits, memory)
			if err != nil {
				t.Fatal(err)
			}
			release := make(chan struct{})
			nativeAdapter := &nativeAdmissionBurstAdapter{entered: make(chan struct{}, 1), release: release}
			nativeRuntime, err := New(nativeAdapter, DefaultLimits(), memory)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			var seed, sibling *Ticket
			t.Cleanup(func() {
				cancel()
				if seed != nil {
					seed.Abandon()
				}
				if sibling != nil {
					sibling.Abandon()
				}
				closing, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				if err := records.Close(closing); err != nil {
					t.Error(err)
				}
				if err := nativeRuntime.Close(closing); err != nil {
					t.Error(err)
				}
				after := memory.Snapshot()
				if after.Count != 0 || after.UsedBytes != 0 || after.ResidentBytes != 0 || after.InputReservedBytes != 0 || after.InputUsedBytes != 0 || after.WireBytes != 0 || after.NativeCount != 0 {
					t.Error("coalesced execution retained real ownership", after)
				}
			})
			for index := range 2 {
				work := plan(uint64(index+1), fmt.Sprint(index), false)
				work.WorkingBytes, work.IOReserve = 1533, 48_000_000
				ticket, failure, _ := records.Submit(ctx, work, nil)
				if failure != nil {
					t.Fatal(failure)
				}
				if index == 0 {
					seed = ticket
				} else {
					sibling = ticket
				}
			}
			select {
			case works := <-adapter.entered:
				if len(works) != 2 || works[0].Memory != seed || works[1].Memory != sibling {
					t.Fatal("test did not coalesce seed and live sibling")
				}
			case <-ctx.Done():
				t.Fatal("coalesced batch did not begin")
			}
			seed.Abandon()
			session := nativeRuntime.NewSession()
			defer session.Close()
			if waiting {
				work := plan(3, "initial", false)
				work.Command, work.WorkingBytes, work.IOReserve = nativeCall(), growthNativeInitial, defaultNativeIOReserve
				if _, failure, _ := nativeRuntime.Submit(ctx, work, session); failure != nil {
					t.Fatal(failure)
				}
				select {
				case <-nativeAdapter.entered:
				case <-ctx.Done():
					t.Fatal("competing Native did not begin")
				}
			}
			close(grow)
			if waiting {
				for {
					memory.mu.Lock()
					claimed := len(memory.workingWaits) == 1
					memory.mu.Unlock()
					if claimed {
						break
					}
					select {
					case err := <-adapter.results:
						t.Fatal("canceled seed prevented live sibling from waiting", err)
					case <-time.After(time.Millisecond):
					case <-ctx.Done():
						t.Fatal("sibling growth did not wait")
					}
				}
				close(release)
				for {
					select {
					case emission := <-session.Events:
						terminal := emission.Event.GetNativeEnd()
						emission.Release()
						if terminal != nil {
							goto convergedSibling
						}
					case <-ctx.Done():
						t.Fatal("Native did not converge")
					}
				}
			}
		convergedSibling:
			select {
			case err := <-adapter.results:
				if err != nil {
					t.Fatal("canceled seed rejected live sibling allocation", err)
				}
			case <-ctx.Done():
				t.Fatal("sibling growth did not complete")
			}
			event, err := sibling.Wait(ctx)
			if err != nil || event.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal("live sibling did not complete", event, err)
			}
			sibling.Ack()
		})
	}
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
