package store

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestExecutionMemoryObservationFinitePrivateNumericSnapshots(t *testing.T) {
	fixture := newGrowthLedger(t)
	memory := fixture.memory
	var received []executionMemoryObservation
	memory.observations.sink = func(observation executionMemoryObservation) {
		received = append(received, observation)
	}
	memory.mu.Lock()
	for range 3 {
		for reason := memoryAdmit; reason <= memoryNativeReadDeadline; reason++ {
			memory.observeRejectionLocked(reason, fixture.record, fixture.record.working+123)
		}
	}
	memory.mu.Unlock()
	if len(received) != 0 {
		t.Fatal("snapshot capture invoked the sink under the ledger lock")
	}
	memory.flushObservations()
	memory.flushObservations()
	if len(received) != executionMemoryObservationLimit {
		t.Fatal("observations were not bounded to eight per Node", len(received))
	}
	seen := make(map[executionMemoryReason]bool)
	for _, observation := range received {
		if seen[observation.Reason] {
			t.Fatal("reason was reported more than once", observation.Reason)
		}
		seen[observation.Reason] = true
		if observation.AccountedBytes != observation.UsedBytes || observation.Leases != 2 || observation.RecordLeases != 1 || observation.NativePreparing != 1 {
			t.Fatal("snapshot was not a coherent aggregate of actual owners", observation)
		}
	}
	// A value-only, numeric schema cannot accidentally retain or serialize a
	// future command/source, resource identifier, URL, credential or context.
	typeOf := reflect.TypeFor[executionMemoryObservation]()
	for index := range typeOf.NumField() {
		field := typeOf.Field(index)
		switch field.Type.Kind() {
		case reflect.Uint8, reflect.Uint64, reflect.Int, reflect.Int64, reflect.Bool:
		default:
			t.Fatal("observation gained a nonnumeric/private-data field", field.Name, field.Type)
		}
	}
	raw, err := json.Marshal(received)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"records/s:", "http://", "mongodb://", "password", "Source", "Command"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatal("observation exposed request data", forbidden)
		}
	}
}

func TestExecutionMemoryObservationGrowthDeadlineKeepsActualCohortOwnership(t *testing.T) {
	fixture := newGrowthLedger(t)
	memory := fixture.memory
	var received []executionMemoryObservation
	memory.observations.sink = func(observation executionMemoryObservation) {
		received = append(received, observation)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	var wait *workingWait
	defer func() { memory.cancelWorkingWait(wait) }()
	target := growthRecordInitial + 64<<10
	wake, err := requestLedgerGrowth(ctx, fixture.record, target, &wait)
	if err != nil || wake == nil {
		t.Fatal("fixture failed to capture the actual Native cohort", err)
	}
	<-ctx.Done()
	wake, err = requestLedgerGrowth(ctx, fixture.record, target, &wait)
	if wake != nil || err != context.DeadlineExceeded {
		t.Fatal("observability changed the strict allocation deadline", wake, err)
	}
	// Logging later must describe the ownership at the actual deadline, not
	// invent a release from a donor that converged after the snapshot was taken.
	fixture.donor.finishWorking()
	memory.cancelWorkingWait(wait)
	memory.flushObservations()
	if len(received) != 1 {
		t.Fatal("deadline did not produce exactly one bounded observation", received)
	}
	observation := received[0]
	if observation.Reason != memoryGrowthDeadline || observation.TargetWorkingBytes != uint64(target) || observation.DeltaBytes != 64<<10 || observation.FIFOPosition != 0 || observation.WorkingClaims != 1 || observation.WaitNanos <= 0 {
		t.Fatal("deadline lost allocation target, FIFO position or elapsed wait", observation)
	}
	if observation.CohortCount != 1 || observation.CohortPreparing != 1 || observation.CohortOwnedWorkingBytes != growthNativeInitial || observation.CohortPreparingWorkingBytes != growthNativeInitial || observation.AccountedBytes != observation.UsedBytes {
		t.Fatal("deadline fabricated donor release or lost actual ownership", observation)
	}
}

func TestExecutionMemoryObservationBlockedSinkHoldsNoRuntimeOrLedgerLock(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	memory := NewExecutionMemory()
	entered, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	memory.observations.sink = func(observation executionMemoryObservation) {
		close(entered)
		<-release
	}
	adapter := &singleExecutionAdapter{}
	runtime, err := New(adapter, DefaultLimits(), memory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		unblock.Do(func() { close(release) })
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := runtime.Close(cleanup); err != nil {
			t.Error(err)
		}
	}()
	failed := make(chan *pb.Failure, 1)
	go func() {
		work := plan(1, "private-resource", true)
		_, failure, _ := runtime.Submit(ctx, work, nil)
		failed <- failure
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("outer Submit did not synchronously flush its rejection")
	}
	progress := make(chan error, 1)
	go func() {
		_ = runtime.Snapshot()
		_ = memory.Snapshot()
		memory.mu.Lock()
		memory.known, memory.trusted, memory.budget = true, true, 128<<20
		memory.mu.Unlock()
		work := plan(2, "healthy", true)
		ticket, failure, _ := runtime.Submit(ctx, work, nil)
		if failure != nil {
			progress <- context.Canceled
			return
		}
		defer ticket.Ack()
		_, err := ticket.Wait(ctx)
		progress <- err
	}()
	select {
	case err := <-progress:
		if err != nil {
			t.Fatal("independent business operation failed beside the blocked sink", err)
		}
	case <-ctx.Done():
		t.Fatal("blocked sink held a ledger/runtime lock or prevented other operations")
	}
	unblock.Do(func() { close(release) })
	select {
	case failure := <-failed:
		if failure.GetCode() != pb.FailureCode_UNAVAILABLE || failure.GetMessage() != "execution memory capacity is unknown" {
			t.Fatal("observability changed the original admission failure", failure)
		}
	case <-ctx.Done():
		t.Fatal("original failed caller did not finish after logger release")
	}
}

func TestExecutionMemoryObservationNormalFlushAllocatesNothing(t *testing.T) {
	memory := NewExecutionMemory()
	allocations := testing.AllocsPerRun(100, func() { memory.flushObservations() })
	if allocations != 0 || memory.observations.count != 0 || len(memory.observations.pending) != 0 {
		t.Fatal("normal no-pressure flush allocated or produced observations", allocations)
	}
}

func TestExecutionMemoryObservationActualBoundaryReasons(t *testing.T) {
	cases := []struct {
		name   string
		reason executionMemoryReason
		reject func(*testing.T, growthLedger)
	}{
		{name: "native-grow", reason: memoryNativeGrowth, reject: func(t *testing.T, fixture growthLedger) {
			if err := fixture.donor.grow(int(fixture.memory.budget) + 1); err != errExecutionMemory {
				t.Fatal("Native growth rejection changed", err)
			}
		}},
		{name: "record-growth", reason: memoryRecordGrowth, reject: func(t *testing.T, fixture growthLedger) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			var wait *workingWait
			defer func() { fixture.memory.cancelWorkingWait(wait) }()
			wake, err := requestLedgerGrowth(ctx, fixture.record, int(fixture.memory.budget)+1, &wait)
			if wake != nil || err != errExecutionMemory {
				t.Fatal("impossible record growth rejection changed", wake, err)
			}
		}},
		{name: "begin-read", reason: memoryBeginRead, reject: func(t *testing.T, fixture growthLedger) {
			fixture.memory.mu.Lock()
			fixture.record.preparing, fixture.donor.preparing = false, false
			fixture.memory.mu.Unlock()
			started, waiting := fixture.donor.beginRead(int(fixture.memory.budget) + 1)
			if started || waiting {
				t.Fatal("nonconverging IO rejection became an ownership wait", started, waiting)
			}
		}},
		{name: "settle", reason: memorySettle, reject: func(t *testing.T, fixture growthLedger) {
			if err := fixture.donor.settle(int(fixture.memory.budget)+1, 0); err != errExecutionMemory {
				t.Fatal("settlement rejection changed", err)
			}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fixture := newGrowthLedger(t)
			var received []executionMemoryObservation
			fixture.memory.observations.sink = func(observation executionMemoryObservation) {
				received = append(received, observation)
			}
			test.reject(t, fixture)
			fixture.memory.flushObservations()
			if len(received) != 1 || received[0].Reason != test.reason || received[0].AccountedBytes != received[0].UsedBytes {
				t.Fatal("actual failure boundary lost its reason or atomic ownership", received)
			}
		})
	}
}
