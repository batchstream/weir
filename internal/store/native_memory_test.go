package store

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
)

func testNativeMemory(allowance uint64) *NativeMemory {
	memory := NewNativeMemory()
	snapshot := overload.Snapshot{RecoveryAllowance: allowance, RecoveryAllowanceKnown: true}
	memory.ObserveMemory(snapshot)
	return memory
}

func TestNativeMemoryConservesOneLeaseAcrossEveryOwner(t *testing.T) {
	memory := testNativeMemory(768 << 20)
	work := plan(1, "native", false)
	lease, failure := memory.admit(work)
	if failure != nil {
		t.Fatal(failure)
	}
	input := uint64(work.Bytes + max(work.ResultBytes, 2*protocol.NativeChunk+execution.ResultOverheadBytes))
	if snapshot := memory.Snapshot(); snapshot.Count != 1 || snapshot.Waiting != 1 || snapshot.UsedBytes != input || snapshot.BudgetBytes != 512<<20 {
		t.Fatal("queued input or event credit was not reserved exactly once", snapshot)
	}
	if started, waiting := lease.start(48<<20, defaultNativeIOReserve); !started || waiting {
		t.Fatal("initial backend working set did not reserve before dispatch")
	}
	if snapshot := memory.Snapshot(); snapshot.Starting != 1 || snapshot.UsedBytes != input+(48<<20) {
		t.Fatal("initial response working reservation was not charged", snapshot)
	}
	if err := lease.settle(8<<20, 1); err != nil {
		t.Fatal(err)
	}
	if !lease.beginRead(2 << 20) {
		t.Fatal("a settled output consumed the reserved IO workspace")
	}
	if snapshot := memory.Snapshot(); snapshot.Settled != 1 || snapshot.UsedBytes != input+(10<<20) {
		t.Fatal("body read lost its transient allocation window", snapshot)
	}
	if err := lease.settle(8<<20, protocol.NativeChunk); err != nil {
		t.Fatal(err)
	}
	lease.terminal(20)
	if snapshot := memory.Snapshot(); snapshot.Terminal != 1 || snapshot.UsedBytes != input {
		t.Fatal("terminal publication retained a cleaned response peak", snapshot)
	}
	lease.release()
	lease.release()
	if snapshot := memory.Snapshot(); snapshot.UsedBytes != 0 || snapshot.Count != 0 || memory.io != 0 {
		t.Fatal("final ownership did not release once", snapshot)
	}
}

func TestNativeMemoryUnknownZeroAndShrinkNeverInventFreeCredit(t *testing.T) {
	memory := NewNativeMemory()
	work := plan(1, "native", false)
	if _, failure := memory.admit(work); failure.GetCode() != pb.FailureCode_UNAVAILABLE {
		t.Fatal("initial unknown memory looked like actual overload", failure)
	}
	sample := overload.Snapshot{RecoveryAllowance: 768 << 20, RecoveryAllowanceKnown: true}
	memory.ObserveMemory(sample)
	lease, failure := memory.admit(work)
	if failure != nil {
		t.Fatal(failure)
	}
	if started, _ := lease.start(48<<20, defaultNativeIOReserve); !started {
		t.Fatal("initial trusted reservation failed")
	}
	if err := lease.settle(8<<20, 1); err != nil {
		t.Fatal(err)
	}
	before := memory.Snapshot()
	sample.Unknown, sample.RecoveryAllowanceKnown, sample.RecoveryAllowance = true, false, 8<<30
	memory.ObserveMemory(sample)
	if snapshot := memory.Snapshot(); snapshot.Known || snapshot.BudgetBytes != before.BudgetBytes || snapshot.UsedBytes != before.UsedBytes {
		t.Fatal("unknown capacity loosened the trusted bound or erased held memory", snapshot)
	}
	sample.Unknown, sample.RecoveryAllowanceKnown, sample.RecoveryAllowance = false, true, 0
	memory.ObserveMemory(sample)
	if snapshot := memory.Snapshot(); !snapshot.Known || snapshot.BudgetBytes != 0 || snapshot.UsedBytes != before.UsedBytes {
		t.Fatal("known zero was confused with unknown or actual resources disappeared", snapshot)
	}
	if _, failure := memory.admit(work); failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("a zero share admitted more work", failure)
	}
	if lease.grow(9<<20) == nil || lease.beginRead(1) {
		t.Fatal("shrunk capacity allowed more allocations")
	}
	if err := lease.settle(1, 1); err != nil {
		t.Fatal("shrunk capacity prevented relinquishing owned memory", err)
	}
	lease.terminal(20)
	lease.release()
	if memory.Snapshot().UsedBytes != 0 {
		t.Fatal("cleanup after capacity shrink retained its credit")
	}
}

func TestNativeMemoryCountIsSharedAndIndependentOfInputQueueLimit(t *testing.T) {
	memory := testNativeMemory(3 << 30)
	work := plan(1, "native", false)
	leases := make([]*nativeLease, 0, nativeCountLimit)
	for range nativeCountLimit {
		lease, failure := memory.admit(work)
		if failure != nil {
			t.Fatal(failure)
		}
		leases = append(leases, lease)
	}
	if _, failure := memory.admit(work); failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("Node count limit grew with Store/input queues", failure)
	}
	for _, lease := range leases {
		lease.release()
	}
	if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 || snapshot.BudgetRejections != 1 {
		t.Fatal("count saturation cleanup did not conserve ownership", snapshot)
	}
}

func TestNativeTerminalCannotGrowBeyondOwnedCapacity(t *testing.T) {
	memory := testNativeMemory(768 << 20)
	work := plan(1, "native", false)
	first, failure := memory.admit(work)
	if failure != nil {
		t.Fatal(failure)
	}
	other, failure := memory.admit(work)
	if failure != nil {
		t.Fatal(failure)
	}
	if started, _ := other.start(8<<20, defaultNativeIOReserve); !started {
		t.Fatal("other response did not reserve")
	}
	if err := other.settle(8<<20, 1); err != nil {
		t.Fatal(err)
	}
	memory.mu.Lock()
	memory.budget = memory.used
	memory.mu.Unlock()
	before := memory.Snapshot()
	if err := first.terminal(2 << 20); err != execution.ErrNativeMemory {
		t.Fatal("oversized terminal bypassed the Node bound", err)
	}
	if after := memory.Snapshot(); after.UsedBytes != before.UsedBytes || after.UsedBytes > after.BudgetBytes {
		t.Fatal("rejected terminal changed actual ownership", before, after)
	}
	if err := first.terminal(256); err != nil {
		t.Fatal("fixed failure could not use its already owned event window", err)
	}
	first.release()
	other.release()
	if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 {
		t.Fatal("rejected terminal leaked credit", snapshot)
	}
}

type nativeLargeHeldAdapter struct {
	singleExecutionAdapter
	attempts atomic.Int64
}

type nativeInitialIOAdapter struct {
	lifecycleAdapter
	entered chan struct{}
}

func (a *nativeInitialIOAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) {
	work := plans[0]
	if work.Command.GetNative() != nil {
		select {
		case a.entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return
	}
	event := execution.FailedEvent(work.Command, pb.MutationOutcome_APPLIED, nil)
	_ = emit(work, event)
}

func TestNativeMemoryBlockedDispatchDoesNotHideEligibleTypedAndDrainReleases(t *testing.T) {
	memory := NewNativeMemory()
	memory.known, memory.trusted = true, true
	memory.budget = memory.reserve + (1 << 20)
	adapter := &nativeInitialIOAdapter{entered: make(chan struct{}, 1)}
	limits := DefaultLimits()
	limits.MaxInFlight, limits.BatchOperations = 2, 1
	runtime, err := New(adapter, limits, memory)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for i := range 2 {
		session := runtime.NewSession()
		defer session.Close()
		work := plan(uint64(i), fmt.Sprint(i), false)
		work.Command, work.WorkingBytes = nativeCall(), defaultNativeIOReserve
		if _, failure, _ := runtime.Submit(ctx, work, session); failure != nil {
			t.Fatal(failure)
		}
		if i == 0 {
			select {
			case <-adapter.entered:
			case <-ctx.Done():
				t.Fatal("initial IO did not begin")
			}
		}
	}
	work := plan(3, "typed", true)
	ticket, failure, _ := runtime.Submit(ctx, work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	if event, err := ticket.Wait(ctx); err != nil || event.GetReadResult() == nil {
		t.Fatal("memory-ineligible Native hid an eligible typed request", event, err)
	}
	ticket.Ack()
	if snapshot := memory.Snapshot(); snapshot.Starting != 1 || snapshot.Waiting != 1 || snapshot.Count != 2 || snapshot.UsedBytes > snapshot.BudgetBytes {
		t.Fatal("waiting dispatch lost its bounded input or acquired IO", snapshot)
	}
	closing, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if err := runtime.Close(closing); err != nil {
		t.Fatal(err)
	}
	if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 || snapshot.ResidentBytes != 0 {
		t.Fatal("drain lost queued, IO, or terminal ownership", snapshot)
	}
}

func (a *nativeLargeHeldAdapter) Execute(ctx context.Context, works []*execution.Plan, emit execution.Emit) {
	work := works[0]
	if work.Command.GetNative() != nil {
		a.attempts.Add(1)
		work.WorkingBytes = 8 << 20
		if work.Key == "short" {
			work.WorkingBytes = 1024
		}
	}
	a.singleExecutionAdapter.Execute(ctx, works, emit)
}

func TestNodeNativeMemorySharesThirtyTwoLargeOutputsAcrossStores(t *testing.T) {
	// A 1 GiB process with the default 70% recovery line has a 477.9 MiB
	// Native budget. All Stores share it; the same capacity is never multiplied.
	memory := testNativeMemory((1 << 30) * 70 / 100)
	limits := DefaultLimits()
	limits.MaxInFlight = 1
	var runtimes []*Runtime
	var adapters []*nativeLargeHeldAdapter
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	for range 2 {
		adapter := &nativeLargeHeldAdapter{}
		runtime, err := New(adapter, limits, memory)
		if err != nil {
			t.Fatal(err)
		}
		runtimes = append(runtimes, runtime)
		adapters = append(adapters, adapter)
		t.Cleanup(func() {
			closing, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if err := runtime.Close(closing); err != nil {
				t.Error(err)
			}
		})
	}
	var sessions []*Session
	for index := range 32 {
		runtime := runtimes[index%2]
		session := runtime.NewSession()
		sessions = append(sessions, session)
		defer session.Close()
		work := plan(uint64(index+1), fmt.Sprint(index), false)
		work.Command, work.WorkingBytes = nativeCall(), 48<<20
		if _, failure, _ := runtime.Submit(ctx, work, session); failure != nil {
			t.Fatal(failure)
		}
	}
	for _, session := range sessions {
		select {
		case emission := <-session.Events:
			if emission.Event.GetHead() == nil {
				t.Fatal("a transient initial estimate killed an admitted stream", emission.Event)
			}
		case <-ctx.Done():
			t.Fatal("large slow outputs stranded an IO slot", memory.Snapshot())
		}
	}
	for _, runtime := range runtimes {
		work := plan(100, "typed", true)
		ticket, failure, _ := runtime.Submit(ctx, work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		if event, err := ticket.Wait(ctx); err != nil || event.GetReadResult() == nil {
			t.Fatal("a different Store's resident replies blocked typed IO", event, err)
		}
		ticket.Ack()
	}
	runtime := runtimes[0]
	session := runtime.NewSession()
	defer session.Close()
	work := plan(101, "short", false)
	work.Command, work.WorkingBytes = nativeCall(), 48<<20
	ticket, failure, _ := runtime.Submit(ctx, work, session)
	if failure != nil {
		t.Fatal(failure)
	}
	for {
		select {
		case emission := <-session.Events:
			emission.Release()
			if emission.Event == nil {
				ticket.Ack()
				goto complete
			}
		case <-ctx.Done():
			t.Fatal("short Native could not use its reserved backend work window", memory.Snapshot())
		}
	}
complete:
	for memory.Snapshot().Count != 32 {
		if ctx.Err() != nil {
			t.Fatal("short Native publisher retained ownership", memory.Snapshot())
		}
		time.Sleep(time.Millisecond)
	}
	if snapshot := memory.Snapshot(); snapshot.Settled != 32 || snapshot.Count != 32 || snapshot.UsedBytes > snapshot.BudgetBytes-snapshot.IOReserveBytes {
		t.Fatal("shared output credit exceeded its actual retained bound", snapshot)
	}
	for _, runtime := range runtimes {
		if testmetrics.Sum(testmetrics.Gather(t, runtime), "weir_store_active_executions") != 0 {
			t.Fatal("a consumer still appeared as backend IO")
		}
	}
	for _, session := range sessions {
		session.Close()
	}
	for _, runtime := range runtimes {
		waitReleased(t, runtime)
	}
	if snapshot := memory.Snapshot(); snapshot.UsedBytes != 0 || snapshot.Count != 0 {
		t.Fatal("shared lease remained after cancellation", snapshot)
	}
}

func TestNativeStartupCanBorrowReserveAfterSmallCapacityOutputsSettle(t *testing.T) {
	memory := testNativeMemory((256 << 20) * 70 / 100)
	work := plan(1, "native", false)
	var leases []*nativeLease
	for range 3 {
		lease, failure := memory.admit(work)
		if failure != nil {
			t.Fatal(failure)
		}
		started, _ := lease.start(48<<20, (48<<20)+2*protocol.NativeChunk+8192)
		if !started {
			t.Fatal("initial workspace unavailable", memory.Snapshot())
		}
		if err := lease.settle(8<<20, 1); err != nil {
			t.Fatal(err)
		}
		leases = append(leases, lease)
	}
	lease, failure := memory.admit(work)
	if failure != nil {
		t.Fatal(failure)
	}
	before := memory.Snapshot()
	started, waiting := lease.start(48<<20, (48<<20)+2*protocol.NativeChunk+8192)
	if !started || waiting {
		t.Fatal("R was permanently unavailable slack", before, memory.Snapshot())
	}
	if snapshot := memory.Snapshot(); snapshot.UsedBytes <= snapshot.BudgetBytes-snapshot.IOReserveBytes || snapshot.UsedBytes > snapshot.BudgetBytes {
		t.Fatal("startup did not actually borrow bounded R", snapshot)
	}
	if err := lease.settle(8<<20, 1); err != nil {
		t.Fatal(err)
	}
	leases = append(leases, lease)
	if snapshot := memory.Snapshot(); snapshot.Settled != 4 || snapshot.Starting != 0 || snapshot.ResidentBytes > snapshot.BudgetBytes-snapshot.IOReserveBytes {
		t.Fatal("small actual reply did not return startup reserve", snapshot)
	}
	for _, lease := range leases {
		lease.release()
	}
	if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 || snapshot.ResidentBytes != 0 {
		t.Fatal(snapshot)
	}
}
