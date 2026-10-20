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

// The sink is deliberately blocked on a diagnostic caller while the SAME
// runtime must dispatch new work, grant a queued Native resume, cancel an active
// adapter and join Close. Merely acquiring the two mutexes would miss a stalled
// single dispatch loop or a logger blocking backend retirement.
func TestExecutionMemoryObservationDiagnosticSinkCannotStallSchedulerOrRetirement(t *testing.T) {
	for _, queuedDeadline := range []bool{false, true} {
		name := "scheduler-start-rejection"
		if queuedDeadline {
			name = "scheduler-queued-deadline"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			memory := NewExecutionMemory()
			memory.known, memory.trusted, memory.budget = true, true, 128<<20
			entered := make(chan executionMemoryObservation, executionMemoryObservationLimit)
			release := make(chan struct{})
			var unblock sync.Once
			memory.observations.sink = func(observation executionMemoryObservation) {
				entered <- observation
				<-release
			}
			gate := make(chan struct{})
			adapter := &nativeResumeAdapter{entered: make(chan string, 16), gate: gate}
			limits := DefaultLimits()
			limits.MaxInFlight, limits.BatchOperations, limits.BatchWait = 1, 1, 0
			// Queue before starting the real dispatch loop so the external Submit
			// cannot race to flush a failure produced by that loop.
			runtime := newRuntime(adapter, limits, memory)
			defer func() {
				unblock.Do(func() { close(release) })
				cancel()
				cleanup, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				if err := runtime.Close(cleanup); err != nil {
					t.Error(err)
				}
			}()
			var active *Ticket
			if queuedDeadline {
				work := plan(1, "active", true)
				var failure *pb.Failure
				active, failure, _ = runtime.Submit(ctx, work, nil)
				if failure != nil {
					t.Fatal(failure)
				}
			}
			caller := ctx
			stopCaller := func() {}
			work := plan(2, "failure", true)
			if queuedDeadline {
				caller, stopCaller = context.WithTimeout(ctx, 50*time.Millisecond)
			} else {
				work.WorkingBytes = int(memory.budget) + 1
			}
			defer stopCaller()
			rejected, failure, _ := runtime.Submit(caller, work, nil)
			if failure != nil {
				t.Fatal("failure bypassed actual scheduler boundary", failure)
			}
			go runtime.loop()
			event, err := rejected.Wait(ctx)
			if err != nil || event.GetReadResult().GetFailure() == nil {
				t.Fatal("scheduler did not return its strict failure", event, err)
			}
			rejected.Ack()
			if !memory.observations.ready.Load() {
				t.Fatal("scheduler observation was lost without an external pull")
			}
			select {
			case observation := <-entered:
				t.Fatal("scheduler invoked the sink itself", observation)
			default:
			}
			diagnosticDone := make(chan struct{})
			go func() {
				memory.FlushObservations()
				close(diagnosticDone)
			}()
			select {
			case observation := <-entered:
				expected := memoryStart
				if queuedDeadline {
					expected = memoryQueuedDeadline
				}
				if observation.Reason != expected {
					t.Fatal("diagnostic pull lost actual scheduler reason", observation)
				}
			case <-ctx.Done():
				t.Fatal("external diagnostic did not pull pending scheduler evidence")
			}
			if active != nil {
				close(gate)
				if _, err := active.Wait(ctx); err != nil {
					t.Fatal("existing adapter could not retire beside blocked diagnostic", err)
				}
				active.Ack()
			}
			healthy := plan(3, "healthy", true)
			ticket, failure, _ := runtime.Submit(ctx, healthy, nil)
			if failure != nil {
				t.Fatal("additional same-runtime Submit blocked or failed", failure)
			}
			if _, err := ticket.Wait(ctx); err != nil {
				t.Fatal("blocked diagnostic stalled actual same-runtime dispatch", err)
			}
			ticket.Ack()
			session := runtime.NewSession()
			defer session.Close()
			nativeWork := plan(4, "native", false)
			nativeWork.Command = nativeCall()
			nativeTicket, failure, _ := runtime.Submit(ctx, nativeWork, session)
			if failure != nil {
				t.Fatal(failure)
			}
			select {
			case emission := <-session.Events:
				if emission.Event.GetHead() == nil {
					t.Fatal("Native did not reach its real runtime IO boundary", emission.Event)
				}
				emission.Release()
			case <-ctx.Done():
				t.Fatal("blocked diagnostic stalled Native dispatch")
			}
			resumed := false
			for !resumed {
				select {
				case key := <-adapter.entered:
					resumed = key == "resume-native"
				case <-ctx.Done():
					t.Fatal("blocked diagnostic stalled queued Native BeginIO resume")
				}
			}
			for {
				select {
				case emission := <-session.Events:
					terminal := emission.Event == nil
					emission.Release()
					if terminal {
						goto nativeFinished
					}
				case <-ctx.Done():
					t.Fatal("blocked diagnostic stalled Native execution/publication retirement")
				}
			}
		nativeFinished:
			nativeTicket.Ack()
			session.Close()
			adapter.gate = make(chan struct{})
			cancelWork := plan(5, "active", true)
			cancelTicket, failure, _ := runtime.Submit(ctx, cancelWork, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			select {
			case key := <-adapter.entered:
				if key != "active" {
					t.Fatal("cancel fixture did not enter the adapter", key)
				}
			case <-ctx.Done():
				t.Fatal("blocked diagnostic prevented dispatch before cancellation")
			}
			cancelTicket.Abandon()
			closing, stop := context.WithTimeout(ctx, time.Second)
			defer stop()
			if err := runtime.Close(closing); err != nil {
				t.Fatal("blocked diagnostic prevented cancellation/Close from joining", err)
			}
			if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 {
				t.Fatal("runtime did not retire ownership while diagnostic remained blocked", snapshot)
			}
			unblock.Do(func() { close(release) })
			select {
			case <-diagnosticDone:
			case <-ctx.Done():
				t.Fatal("external diagnostic caller did not join after sink release")
			}
		})
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
