package store

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestCollectionFixedArrivalTraces(t *testing.T) {
	type traceStep struct {
		at       time.Duration
		arrivals int
		want     []uint64
		next     time.Duration
	}
	tests := []struct {
		name  string
		steps []traceStep
	}{
		{
			name: "sparse",
			steps: []traceStep{
				{at: 0, arrivals: 1, next: 10 * time.Millisecond},
				{at: 9 * time.Millisecond, next: 10 * time.Millisecond},
				{at: 10 * time.Millisecond, want: []uint64{0}},
				{at: 20 * time.Millisecond, arrivals: 1, next: 30 * time.Millisecond},
				{at: 29 * time.Millisecond, next: 30 * time.Millisecond},
				{at: 30 * time.Millisecond, want: []uint64{1}},
			},
		},
		{
			name: "burst",
			steps: []traceStep{
				{at: 0, arrivals: 1, next: 10 * time.Millisecond},
				{at: 3 * time.Millisecond, arrivals: 1, next: 10 * time.Millisecond},
				{at: 7 * time.Millisecond, arrivals: 1, want: []uint64{0, 1, 2}},
				{at: 8 * time.Millisecond, arrivals: 1, next: 18 * time.Millisecond},
				{at: 17 * time.Millisecond, next: 18 * time.Millisecond},
				{at: 18 * time.Millisecond, want: []uint64{3}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limits := DefaultLimits()
			limits.BatchOperations = 3
			limits.BatchWait = 10 * time.Millisecond
			runtime := newRuntime(nil, limits, nil)
			base := time.Now()
			var tickets []*Ticket
			var dispatched []uint64
			for _, step := range test.steps {
				now := base.Add(step.at)
				for range step.arrivals {
					ordinal := uint64(len(tickets))
					work := plan(ordinal, fmt.Sprintf("record-%d", ordinal), false)
					ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
					if failure != nil {
						t.Fatal(failure)
					}
					ticket.queuedAt = now
					tickets = append(tickets, ticket)
				}
				selected, next := runtime.selectLocked(now)
				var got []uint64
				if selected != nil {
					for _, ticket := range selected.items {
						got = append(got, ticket.plan.ID)
					}
					finish(runtime, selected)
					for _, ticket := range selected.items {
						ticket.Ack()
					}
				}
				if !slices.Equal(got, step.want) {
					t.Fatalf("at %s: dispatched %v, want %v", step.at, got, step.want)
				}
				if step.next != 0 && !next.Equal(base.Add(step.next)) {
					t.Fatalf("at %s: next dispatch %s, want %s", step.at, next.Sub(base), step.next)
				}
				dispatched = append(dispatched, got...)
			}
			if len(dispatched) != len(tickets) {
				t.Fatal("trace lost admitted operations", dispatched, len(tickets))
			}
			if snapshot := runtime.Snapshot(); snapshot.Pending != 0 || snapshot.Active != 0 || snapshot.Retained != 0 || snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 {
				t.Fatal("completed trace leaked queue, execution or result credit", snapshot)
			}
		})
	}
}

func TestReadyOldestCandidatePreservesCrossNamespaceSessionOrder(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(fmt.Sprintf("expired=%t", expired), func(t *testing.T) {
			limits := DefaultLimits()
			if expired {
				limits.BatchWait = time.Second
			}
			runtime := newRuntime(nil, limits, nil)
			session := runtime.NewSession()
			defer session.Close()
			base := time.Now().Add(-2 * time.Second)
			var tickets []*Ticket
			for index, key := range []string{"seed", "shared", "shared", "other"} {
				work := plan(uint64(index), key, false)
				work.BatchKey = "oldest"
				if index == 1 {
					work.BatchKey = "other-visibility"
				}
				ticket, failure, _ := runtime.Submit(t.Context(), work, session)
				if failure != nil {
					t.Fatal(failure)
				}
				ticket.queuedAt = base
				tickets = append(tickets, ticket)
			}
			for _, want := range [][]uint64{{0, 3}, {1}, {2}} {
				selected, _ := runtime.selectLocked(time.Now())
				if selected == nil {
					t.Fatal("ready namespace did not dispatch", want)
				}
				var got []uint64
				for _, ticket := range selected.items {
					got = append(got, ticket.plan.ID)
				}
				if !slices.Equal(got, want) {
					t.Fatal("namespace bypass reordered a session resource", got, want)
				}
				if len(want) == 1 && want[0] == 1 {
					blocked, _ := runtime.selectLocked(time.Now())
					if blocked != nil {
						t.Fatal("successor overtook an executing predecessor", blocked)
					}
				}
				finish(runtime, selected)
			}
			for _, ticket := range tickets {
				ticket.Ack()
			}
		})
	}
}

func TestCollectionWaitOverlapsBusySlotAndPreservesBackpressure(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxInFlight = 1
	limits.BatchOperations = 3
	limits.QueueOperations = 3
	limits.BatchWait = time.Hour
	adapter := &concurrencyAdapter{started: make(chan []*execution.Plan, 2), finish: make(chan struct{})}
	runtime, err := New(adapter, limits, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waitContext, stopWaiting := context.WithTimeout(t.Context(), 3*time.Second)
	defer stopWaiting()
	t.Cleanup(func() {
		cancel()
		closeContext, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := runtime.Close(closeContext); err != nil {
			t.Error(err)
		}
	})
	var tickets []*Ticket
	for index := range limits.BatchOperations {
		work := plan(uint64(index), fmt.Sprint(index), false)
		ticket, failure, _ := runtime.Submit(ctx, work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	select {
	case started := <-adapter.started:
		if len(started) != limits.BatchOperations {
			t.Fatal("continuous arrivals did not bypass collection on a full batch", len(started))
		}
	case <-waitContext.Done():
		t.Fatal("full batch did not start")
	}
	canceled, stop := context.WithCancel(ctx)
	for index := range limits.QueueOperations {
		caller := ctx
		if index == 1 {
			caller = canceled
		}
		work := plan(uint64(index+3), fmt.Sprint(index+3), false)
		ticket, failure, _ := runtime.Submit(caller, work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	work := plan(6, "overflow", false)
	_, failure, changed := runtime.Submit(ctx, work, nil)
	if failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("busy slot did not apply bounded queue backpressure", failure)
	}
	stop()
	event, err := tickets[4].Wait(waitContext)
	if err != nil || event.GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_STARTED {
		t.Fatal("canceled queued operation lost NOT_STARTED evidence", event, err)
	}
	if changed != nil {
		t.Fatal("full input queue created an unowned admission waiter")
	}
	runtime.mu.Lock()
	for _, ticket := range runtime.queue {
		// Advance the admitted work's collection age while its execution slot is
		// occupied, without a wall-clock sleep or a second scheduler clock.
		ticket.queuedAt = time.Now().Add(-2 * limits.BatchWait)
	}
	runtime.mu.Unlock()
	if snapshot := runtime.Snapshot(); snapshot.Active != 1 || snapshot.Pending != 2 {
		t.Fatal("busy execution drained waiting work or lost cancellation", snapshot)
	}
	select {
	case <-adapter.started:
		t.Fatal("ready waiting work exceeded the in-flight limit")
	default:
	}
	adapter.finish <- struct{}{}
	select {
	case started := <-adapter.started:
		if len(started) != 2 || started[0].ID != 3 || started[1].ID != 5 {
			t.Fatal("slot completion reset the collection wait or retained canceled work", started)
		}
	case <-waitContext.Done():
		t.Fatal("expired partial batch started a second collection window")
	}
	close(adapter.finish)
	for _, ticket := range tickets {
		if _, err := ticket.Wait(waitContext); err != nil {
			t.Fatal(err)
		}
		ticket.Ack()
	}
	if snapshot := runtime.Snapshot(); snapshot.Pending != 0 || snapshot.Active != 0 || snapshot.Retained != 0 || snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 {
		t.Fatal("busy-slot collection leaked credits", snapshot)
	}
}
