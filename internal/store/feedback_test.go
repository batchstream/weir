package store

import (
	"context"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type canceledFeedbackAdapter struct {
	lifecycleAdapter
}

func (a *canceledFeedbackAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) execution.Feedback {
	<-ctx.Done()
	for _, plan := range plans {
		failure := protocol.ContextFailure(ctx)
		result := execution.FailedResult(plan.Operation, pb.MutationOutcome_UNKNOWN, failure)
		output := &execution.Output{Result: result}
		_ = emit(plan, output)
	}
	return execution.Congested
}

func TestRuntimeCanceledCallDoesNotBecomeBackendCongestion(t *testing.T) {
	limits := DefaultLimits()
	adapter := &canceledFeedbackAdapter{}
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
	if got := runtime.Snapshot(); got.ConcurrencyLimit != limits.Concurrency || got.Feedback != "neutral" {
		t.Fatal("caller cancellation reduced backend capacity", got)
	}
	if ticket.Result().Mutation.GetOutcome() != pb.MutationOutcome_UNKNOWN {
		t.Fatal("feedback changed uncertain write evidence")
	}
	ticket.Ack()
}

func TestRuntimeOwnedBackendDeadlineRemainsCongestion(t *testing.T) {
	limits := DefaultLimits()
	adapter := &canceledFeedbackAdapter{}
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
	if got := runtime.Snapshot(); got.ConcurrencyLimit != limits.Concurrency || got.Feedback != "congested" {
		t.Fatal("backend deadline lost its congestion evidence or changed configured capacity", got)
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
	if got := runtime.Snapshot(); got.ConcurrencyLimit != limits.Concurrency || got.Feedback != "healthy" {
		t.Fatal("a partially canceled batch changed configured capacity or feedback", got)
	}
	if tickets[0].Result().Mutation.GetOutcome() != pb.MutationOutcome_NOT_STARTED || tickets[1].Result().Mutation.GetOutcome() != pb.MutationOutcome_APPLIED {
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
