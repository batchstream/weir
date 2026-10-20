package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	"github.com/batchstream/weir/internal/execution"
)

const growthRecordInitial = 48_000_000 + 3066
const growthNativeInitial = 48_000_000 + 8192

type growthLedger struct {
	memory *ExecutionMemory
	record *executionLease
	donor  *executionLease
}

func newGrowthLedger(t *testing.T) growthLedger {
	t.Helper()
	memory := NewExecutionMemory()
	memory.known, memory.trusted = true, true
	publication := 2*protocol.NativeChunk + execution.ResultOverheadBytes - nativeQueuedWindow
	memory.budget = uint64(DefaultLimits().QueueBytes + growthRecordInitial + growthNativeInitial + publication + 8192)
	recordWork := plan(1, "record", false)
	recordWork.WorkingBytes, recordWork.IOReserve = 3066, 48_000_000
	record, failure, _ := memory.admit(recordWork)
	if failure != nil {
		t.Fatal(failure)
	}
	if started, _ := record.start(growthRecordInitial, recordWork.IOReserve); !started {
		t.Fatal("record did not start")
	}
	native := plan(2, "native", false)
	native.Command, native.WorkingBytes, native.IOReserve = nativeCall(), growthNativeInitial, defaultNativeIOReserve
	donor, failure, _ := memory.admit(native)
	if failure != nil {
		t.Fatal(failure)
	}
	if started, _ := donor.start(native.WorkingBytes, native.IOReserve); !started {
		t.Fatal("Native did not start")
	}
	t.Cleanup(func() {
		memory.mu.Lock()
		leases := make([]*executionLease, 0, len(memory.leases))
		for lease := range memory.leases {
			leases = append(leases, lease)
		}
		memory.mu.Unlock()
		for _, lease := range leases {
			lease.release()
		}
		after := memory.Snapshot()
		if after.Count != 0 || after.UsedBytes != 0 || after.ResidentBytes != 0 || after.InputReservedBytes != 0 || after.InputUsedBytes != 0 || after.WireBytes != 0 || after.NativeCount != 0 || len(memory.workingWaits) != 0 {
			t.Error("real lease release retained growth ownership or scheduling claims", after)
		}
	})
	fixture := growthLedger{memory: memory, record: record, donor: donor}
	return fixture
}

func requestLedgerGrowth(ctx context.Context, lease *executionLease, target int, wait **workingWait) (<-chan struct{}, error) {
	live := func() error { return nil }
	return lease.requestWorking(ctx, target, wait, live)
}

func TestWorkingGrowthWaitExcludesIneligibleOwners(t *testing.T) {
	for _, kind := range []string{"self", "record", "settled", "unbounded", "impossible"} {
		t.Run(kind, func(t *testing.T) {
			fixture := newGrowthLedger(t)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			var requestContext context.Context = ctx
			target := growthRecordInitial + 64<<10
			switch kind {
			case "self":
				fixture.record.native = true
				fixture.donor.preparing = false
			case "record":
				fixture.donor.native = false
				// Restore classification before real release's Native count update.
				defer func() { fixture.donor.native = true }()
			case "settled":
				fixture.donor.preparing = false
			case "unbounded":
				requestContext = context.Background()
			case "impossible":
				target = int(fixture.memory.budget) + 1
			}
			var wait *workingWait
			defer func() { fixture.memory.cancelWorkingWait(wait) }()
			wake, err := requestLedgerGrowth(requestContext, fixture.record, target, &wait)
			if wake != nil || !errors.Is(err, execution.ErrExecutionMemory) || len(fixture.memory.workingWaits) != 0 {
				t.Fatal("ineligible ownership became a wait or a grant", kind, wake, err)
			}
			if kind == "self" {
				fixture.record.native = false
			}
		})
	}
}

func TestWorkingGrowthCapturedDonorMustActuallyReleaseCapacity(t *testing.T) {
	fixture := newGrowthLedger(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var wait *workingWait
	defer func() { fixture.memory.cancelWorkingWait(wait) }()
	target := growthRecordInitial + 2<<20
	wake, err := requestLedgerGrowth(ctx, fixture.record, target, &wait)
	if err != nil || wake == nil {
		t.Fatal("growth did not capture Native startup", err)
	}
	// Initial IO converges, but most bytes become actual retained response
	// ownership. Its consumer is not a donor that growth is allowed to await.
	if err := fixture.donor.settle(growthNativeInitial-1<<20, 0); err != nil {
		t.Fatal("donor could not transfer real response ownership", err)
	}
	wake, err = requestLedgerGrowth(ctx, fixture.record, target, &wait)
	if wake != nil || !errors.Is(err, execution.ErrExecutionMemory) {
		t.Fatal("growth waited for settled output instead of ending its finite cohort", err)
	}
}

func TestWorkingGrowthFIFOClaimsPreserveDonorProgressAndFreshStartOrder(t *testing.T) {
	fixture := newGrowthLedger(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	secondPlan := plan(3, "second-record", false)
	secondPlan.WorkingBytes = 0
	second, failure, _ := fixture.memory.admit(secondPlan)
	if failure != nil {
		t.Fatal(failure)
	}
	if started, _ := second.start(0, 0); !started {
		t.Fatal("second record did not start")
	}
	freshPlan := plan(4, "fresh", false)
	fresh, failure, _ := fixture.memory.admit(freshPlan)
	if failure != nil {
		t.Fatal(failure)
	}
	var firstWait, secondWait *workingWait
	defer func() {
		fixture.memory.cancelWorkingWait(firstWait)
		fixture.memory.cancelWorkingWait(secondWait)
	}()
	wake, err := requestLedgerGrowth(ctx, fixture.record, growthRecordInitial+64<<10, &firstWait)
	if err != nil || wake == nil {
		t.Fatal(err)
	}
	wake, err = requestLedgerGrowth(ctx, second, 64<<10, &secondWait)
	if err != nil || wake == nil || len(fixture.memory.workingWaits) != 2 {
		t.Fatal("second growth did not join FIFO", err)
	}
	if started, waiting := fresh.start(1024, 0); started || !waiting {
		t.Fatal("fresh start bypassed queued growth")
	}
	// Donors can still reserve their real initial-IO needs; priority must not
	// block the operation whose convergence the captured cohort depends on.
	if err := fixture.donor.grow(growthNativeInitial + 8192); err != nil {
		t.Fatal("growth precedence blocked donor GrowIO", err)
	}
	fixture.donor.finishWorking()
	wake, err = requestLedgerGrowth(ctx, second, 64<<10, &secondWait)
	if err != nil || wake == nil {
		t.Fatal("younger growth bypassed older scheduling claim", err)
	}
	wake, err = requestLedgerGrowth(ctx, fixture.record, growthRecordInitial+64<<10, &firstWait)
	if err != nil || wake != nil {
		t.Fatal("oldest growth did not receive real released capacity", err)
	}
	if started, waiting := fresh.start(1024, 0); started || !waiting {
		t.Fatal("fresh start bypassed remaining growth")
	}
	wake, err = requestLedgerGrowth(ctx, second, 64<<10, &secondWait)
	if err != nil || wake != nil {
		t.Fatal("next growth did not progress", err)
	}
	if started, _ := fresh.start(1024, 0); !started {
		t.Fatal("completed growth claims blocked fresh work")
	}
}

func TestWorkingGrowthRetiredCohortDoesNotCaptureQueuedFutureNative(t *testing.T) {
	fixture := newGrowthLedger(t)
	futurePlan := plan(3, "future", false)
	futurePlan.Command = nativeCall()
	future, failure, _ := fixture.memory.admit(futurePlan)
	if failure != nil {
		t.Fatal(failure)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var wait *workingWait
	defer func() { fixture.memory.cancelWorkingWait(wait) }()
	wake, err := requestLedgerGrowth(ctx, fixture.record, growthRecordInitial+2<<20, &wait)
	if wake == nil || err != nil || len(wait.donors) != 1 {
		t.Fatal("cohort did not isolate the already starting Native", err)
	}
	if _, included := wait.donors[future]; included {
		t.Fatal("queued future Native entered the captured cohort")
	}
	fixture.donor.release()
	// A real allowance shrink can leave the target impossible even after full
	// donor retirement. Neither its retired identity nor future work renews it.
	fixture.memory.budget = fixture.memory.used + 1024
	wake, err = requestLedgerGrowth(ctx, fixture.record, growthRecordInitial+2<<20, &wait)
	if wake != nil || !errors.Is(err, execution.ErrExecutionMemory) || !fixture.donor.released {
		t.Fatal("retired identity or queued future work extended growth wait", err)
	}
}

func TestWorkingGrowthCancellationAndDeadlinePreserveActualOwnership(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "canceled", true: "deadline"}[deadline], func(t *testing.T) {
			fixture := newGrowthLedger(t)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
			}
			defer cancel()
			var wait *workingWait
			defer func() { fixture.memory.cancelWorkingWait(wait) }()
			before := fixture.memory.Snapshot()
			wake, err := requestLedgerGrowth(ctx, fixture.record, growthRecordInitial+64<<10, &wait)
			if err != nil || wake == nil {
				t.Fatal("growth did not wait", err)
			}
			if !deadline {
				cancel()
			}
			<-ctx.Done()
			wake, err = requestLedgerGrowth(ctx, fixture.record, growthRecordInitial+64<<10, &wait)
			if wake != nil || !errors.Is(err, ctx.Err()) || fixture.memory.Snapshot().UsedBytes != before.UsedBytes {
				t.Fatal("cancellation granted or released unowned credit", err)
			}
		})
	}
}
