package store

import (
	"context"
	"testing"
	"time"

	"github.com/batchstream/weir/api/protocol"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type canceledFeedbackAdapter struct {
	lifecycleAdapter
}

func (a *canceledFeedbackAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) execution.Feedback {
	<-ctx.Done()
	for _, plan := range plans {
		failure := protocol.ContextFailure(ctx)
		result := protocol.ResultError(plan.Operation, pb.MutationOutcome_UNKNOWN, failure)
		_ = emit(plan, resultEvent(result))
	}
	return execution.Congested
}

func TestRuntimeCanceledCallDoesNotBecomeBackendCongestion(t *testing.T) {
	limits := DefaultLimits()
	limits.Collect = 0
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
	if got := runtime.Snapshot(); got.Window != limits.Concurrency || got.Feedback != "neutral" || got.LatencyProfiles != 0 {
		t.Fatal("caller cancellation reduced backend capacity", got)
	}
	if ticket.Result().GetMutation().GetOutcome() != pb.MutationOutcome_UNKNOWN {
		t.Fatal("controller changed uncertain write evidence")
	}
	ticket.Ack()
}

func TestRuntimeOwnedBackendDeadlineRemainsCongestion(t *testing.T) {
	limits := DefaultLimits()
	limits.Collect = 0
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
	if got := runtime.Snapshot(); got.Window != limits.Concurrency/2 || got.Feedback != "congested" {
		t.Fatal("backend deadline failed to reduce pressure", got)
	}
	ticket.Ack()
}

func TestRuntimePartiallyCanceledHealthyBatchCannotRecoverConcurrency(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchOperations = 2
	limits.Collect = 0
	gate := make(chan struct{})
	close(gate)
	adapter := &recordBatchAdapter{started: make(chan []*execution.Plan, 1), gate: gate}
	runtime := newRuntime(adapter, limits)
	runtime.controller.window = 1
	runtime.controller.credit = 3
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
	if b == nil || len(b.items) != 2 || !b.saturated {
		t.Fatal("missing shared saturated batch")
	}
	stop()
	runtime.execute(b)
	if got := runtime.Snapshot(); got.Window != 1 || got.Feedback != "healthy" || got.LatencyProfiles != 0 || runtime.controller.credit != 0 || b.recoveryEligible {
		t.Fatal("a partially canceled batch contributed recovery credit", got)
	}
	if tickets[0].Result().GetMutation().GetOutcome() != pb.MutationOutcome_NOT_STARTED || tickets[1].Result().GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
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
