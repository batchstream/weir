package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
)

func TestNativeInputPromiseIsSharedBoundedAndReleasedAtLastAck(t *testing.T) {
	memory := testExecutionMemory(768 << 20)
	work := plan(1, "native", false)
	work.Bytes = 1 << 20
	var leases []*executionLease
	for {
		lease, failure := memory.admit(work)
		if failure != nil {
			if failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
				t.Fatal(failure)
			}
			break
		}
		leases = append(leases, lease)
	}
	snapshot := memory.Snapshot()
	input := uint64(work.Bytes + nativeQueuedWindow)
	if snapshot.InputReservedBytes != 32<<20 || snapshot.UsedBytes != snapshot.InputReservedBytes || snapshot.ResidentBytes != snapshot.InputReservedBytes || snapshot.InputUsedBytes != uint64(len(leases))*input || snapshot.InputReservedBytes-snapshot.InputUsedBytes >= input {
		t.Fatal("queued inputs did not draw from one finite, precharged promise", snapshot)
	}
	for index, lease := range leases {
		if err := lease.terminal(200); err != nil {
			t.Fatal("queued terminal required an unreserved publication chunk", err)
		}
		lease.release()
		snapshot = memory.Snapshot()
		if index < len(leases)-1 && (snapshot.UsedBytes != 32<<20 || snapshot.InputReservedBytes != 32<<20) {
			t.Fatal("one Ack removed another input's promised capacity", snapshot)
		}
	}
	if snapshot.UsedBytes != 0 || snapshot.InputReservedBytes != 0 || snapshot.InputUsedBytes != 0 || snapshot.ResidentBytes != 0 {
		t.Fatal("last Ack retained the shared input promise", snapshot)
	}
}

func TestNativeInputPromiseSurvivesConvergenceAndRevokesOnlyUnusedOnShrink(t *testing.T) {
	memory := testExecutionMemory(768 << 20)
	work := plan(1, "native", false)
	first, failure := memory.admit(work)
	if failure != nil {
		t.Fatal(failure)
	}
	defer first.release()
	publication := first.publication - nativeQueuedWindow
	if started, _ := first.start(int(memory.budget-memory.inputReserved)-publication, defaultNativeIOReserve); !started {
		t.Fatal("could not consume non-input workspace")
	}
	before := memory.Snapshot()
	second, failure := memory.admit(work)
	if failure != nil {
		t.Fatal("IO consumed the already promised input space", failure)
	}
	defer second.release()
	after := memory.Snapshot()
	if after.UsedBytes != before.UsedBytes || after.InputUsedBytes != 2*uint64(work.Bytes+nativeQueuedWindow) || after.UsedBytes > after.BudgetBytes {
		t.Fatal("drawing promised input minted extra memory", before, after)
	}
	sample := overload.Snapshot{RecoveryAllowanceKnown: true, RecoveryAllowance: 3 << 20}
	memory.ObserveMemory(sample)
	shrunk := memory.Snapshot()
	if shrunk.InputReservedBytes != after.InputUsedBytes || shrunk.InputUsedBytes != after.InputUsedBytes || shrunk.UsedBytes != after.UsedBytes-(after.InputReservedBytes-after.InputUsedBytes) {
		t.Fatal("shrink erased live inputs or actual IO", after, shrunk)
	}
	if _, failure := memory.admit(work); failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("revoked input capacity was reused", failure)
	}
	sample.Unknown, sample.RecoveryAllowanceKnown, sample.RecoveryAllowance = true, false, 8<<30
	memory.ObserveMemory(sample)
	if unknown := memory.Snapshot(); unknown.InputReservedBytes != shrunk.InputReservedBytes || unknown.UsedBytes != shrunk.UsedBytes {
		t.Fatal("unknown sample enlarged the input promise", unknown)
	}
}

func TestNativeInputPromiseSmallCapacityAndOversizedIdlePeak(t *testing.T) {
	for _, budget := range []uint64{0, 1 << 20, defaultNativeIOReserve + (16 << 10), 128 << 20} {
		memory := NewExecutionMemory()
		memory.known, memory.trusted, memory.budget = true, true, budget
		work := plan(1, "native", false)
		work.WorkingBytes = 4096
		normal := min(uint64(32<<20), (budget-min(budget, uint64(defaultNativeIOReserve)))/2)
		work.Bytes = int(normal) + 2048
		lease, failure := memory.admit(work)
		peak := uint64(work.Bytes+nativeQueuedWindow+work.WorkingBytes) + uint64(2*protocol.NativeChunk+execution.ResultOverheadBytes-nativeQueuedWindow)
		if peak > budget {
			if failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || memory.Snapshot().UsedBytes != 0 {
				t.Fatal("oversized input was accepted without its entire initial peak fitting", budget, failure, memory.Snapshot())
			}
			continue
		}
		if failure != nil {
			t.Fatal("a valid idle oversized input was limited by the automatic queue promise", budget, failure)
		}
		if snapshot := memory.Snapshot(); snapshot.InputReservedBytes != uint64(work.Bytes+nativeQueuedWindow) || snapshot.InputUsedBytes != snapshot.InputReservedBytes {
			t.Fatal("idle oversized input did not own its actual bytes", snapshot)
		}
		if _, failure := memory.admit(work); failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
			t.Fatal("oversized single-input exception multiplied across leases", failure)
		}
		if started, waiting := lease.start(work.WorkingBytes, defaultNativeIOReserve); !started || waiting {
			t.Fatal("admitted idle oversized input could not use its verified peak", memory.Snapshot())
		}
		lease.release()
		if snapshot := memory.Snapshot(); snapshot.UsedBytes != 0 || snapshot.InputUsedBytes != 0 || snapshot.InputReservedBytes != 0 {
			t.Fatal(snapshot)
		}
		work.WorkingBytes = int(budget)
		if _, failure := memory.admit(work); failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || memory.Snapshot().UsedBytes != 0 {
			t.Fatal("oversized raw input bypassed its future dispatch cost", budget, failure, memory.Snapshot())
		}
	}
}

func TestNativeInputPromiseConcurrentAdmissionCannotOverdraw(t *testing.T) {
	memory := testExecutionMemory(768 << 20)
	work := plan(1, "native", false)
	work.Bytes = 100 << 10
	var group sync.WaitGroup
	leases := make(chan *executionLease, nativeCountLimit)
	for range nativeCountLimit {
		group.Go(func() {
			lease, _ := memory.admit(work)
			if lease != nil {
				leases <- lease
			}
		})
	}
	group.Wait()
	close(leases)
	snapshot := memory.Snapshot()
	if snapshot.InputUsedBytes > snapshot.InputReservedBytes || snapshot.UsedBytes != snapshot.InputReservedBytes || snapshot.UsedBytes > snapshot.BudgetBytes || snapshot.Count >= nativeCountLimit {
		t.Fatal("concurrent Stores overran the one Node input quota", snapshot)
	}
	for lease := range leases {
		lease.release()
	}
	if snapshot := memory.Snapshot(); snapshot.UsedBytes != 0 || snapshot.InputReservedBytes != 0 || snapshot.InputUsedBytes != 0 {
		t.Fatal(snapshot)
	}
}

func TestNativeQueueSaturationDoesNotCreateUnadmittedWaiters(t *testing.T) {
	memory := testExecutionMemory(768 << 20)
	limits := DefaultLimits()
	limits.MaxInFlight, limits.QueueOperations = 1, 1
	adapter := &nativeInitialIOAdapter{entered: make(chan struct{}, 1)}
	runtime, err := New(adapter, limits, memory)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for index := range 2 {
		session := runtime.NewSession()
		defer session.Close()
		work := plan(uint64(index), "native", false)
		work.Command, work.WorkingBytes = nativeCall(), 48<<20
		if _, failure, _ := runtime.Submit(ctx, work, session); failure != nil {
			t.Fatal(failure)
		}
		if index == 0 {
			select {
			case <-adapter.entered:
			case <-ctx.Done():
				t.Fatal("backend did not begin")
			}
		}
	}
	before := memory.Snapshot()
	session := runtime.NewSession()
	defer session.Close()
	work := plan(3, "extra", false)
	work.Command = nativeCall()
	if ticket, failure, changed := runtime.Submit(ctx, work, session); ticket != nil || failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || changed != nil {
		t.Fatal("full Native input queue created an external uncharged waiter", failure, changed)
	}
	if after := memory.Snapshot(); after.Count != before.Count || after.InputUsedBytes != before.InputUsedBytes {
		t.Fatal("rejected input retained a lease", before, after)
	}
	typed := plan(4, "typed", true)
	if _, failure, changed := runtime.Submit(ctx, typed, nil); failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || changed != nil {
		t.Fatal("typed queue created an unowned input waiter", failure, changed)
	}
}
