package store

import (
	"context"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
)

type deadlineAdapter struct {
	lifecycleAdapter
}

func (a *deadlineAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) bool {
	<-ctx.Done()
	for _, plan := range plans {
		failure := protocol.ContextFailure(ctx)
		result := execution.FailedEvent(plan.Command, pb.MutationOutcome_UNKNOWN, failure)
		output := result
		_ = emit(plan, output)
	}
	return false
}

func TestRuntimeCanceledCallDoesNotCountAsBackendTimeout(t *testing.T) {
	limits := DefaultLimits()
	adapter := &deadlineAdapter{}
	runtime := newRuntime(adapter, limits)
	ctx, cancel := context.WithCancel(context.Background())
	work := plan(1, "canceled", false)
	ticket, failure, _ := runtime.Submit(ctx, work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	runtime.mu.Lock()
	b := runtime.selectLocked(time.Now())
	runtime.mu.Unlock()
	if b == nil {
		t.Fatal("missing dispatched work")
	}
	cancel()
	b.cancel()
	runtime.execute(b)
	if got := runtime.Snapshot(); got.ConcurrencyLimit != limits.Concurrency {
		t.Fatal("caller cancellation reduced backend capacity", got)
	}
	if recordEvent(t, ticket).GetMutationResult().GetOutcome() != pb.MutationOutcome_UNKNOWN {
		t.Fatal("cancellation changed uncertain write evidence")
	}
	if got := testmetrics.Sample(testmetrics.Gather(t, runtime), "weir_store_backend_timeouts_total", nil).GetCounter().GetValue(); got != 0 {
		t.Fatal("caller cancellation counted as backend timeout", got)
	}
	ticket.Ack()
}

func TestRuntimeOwnedBackendDeadlineCountsTimeout(t *testing.T) {
	limits := DefaultLimits()
	adapter := &deadlineAdapter{}
	runtime := newRuntime(adapter, limits)
	work := plan(1, "timeout", false)
	ticket, failure, _ := runtime.Submit(context.Background(), work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	runtime.mu.Lock()
	b := runtime.selectLocked(time.Now())
	runtime.mu.Unlock()
	if b == nil || !b.timeoutOwned {
		t.Fatal("missing owned deadline")
	}
	b.cancel()
	b.ctx, b.cancel = context.WithDeadline(context.Background(), time.Unix(1, 0))
	runtime.execute(b)
	if got := runtime.Snapshot(); got.ConcurrencyLimit != limits.Concurrency {
		t.Fatal("backend deadline changed configured capacity", got)
	}
	if got := testmetrics.Sample(testmetrics.Gather(t, runtime), "weir_store_backend_timeouts_total", nil).GetCounter().GetValue(); got != 1 {
		t.Fatal("backend deadline was not counted", got)
	}
	ticket.Ack()
}

func TestRuntimePartiallyCanceledBatchPreservesEachCallOutcome(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchOperations = 2
	gate := make(chan struct{})
	close(gate)
	adapter := &recordBatchAdapter{started: make(chan []*execution.Plan, 1), gate: gate}
	runtime := newRuntime(adapter, limits)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	canceled, stop := context.WithCancel(ctx)
	var tickets []*Ticket
	for index, caller := range []context.Context{canceled, ctx, ctx} {
		work := plan(uint64(index+1), string(rune('a'+index)), false)
		ticket, failure, _ := runtime.Submit(caller, work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	runtime.mu.Lock()
	b := runtime.selectLocked(time.Now())
	runtime.mu.Unlock()
	if b == nil || len(b.items) != 2 {
		t.Fatal("missing shared batch")
	}
	stop()
	runtime.execute(b)
	if got := runtime.Snapshot(); got.ConcurrencyLimit != limits.Concurrency {
		t.Fatal("a partially canceled batch changed configured capacity", got)
	}
	if recordEvent(t, tickets[0]).GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_STARTED || recordEvent(t, tickets[1]).GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("cancellation leaked into the healthy caller's mutation evidence")
	}
	cancel()
	runtime.mu.Lock()
	runtime.cancelQueuedLocked()
	runtime.mu.Unlock()
	for _, ticket := range tickets {
		ticket.Ack()
	}
}
