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

type concurrencyAdapter struct {
	lifecycleAdapter
	started chan []*execution.Plan
	finish  chan struct{}
}

func (adapter *concurrencyAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) {
	adapter.started <- plans
	select {
	case <-adapter.finish:
	case <-ctx.Done():
	}
	for _, work := range plans {
		result := execution.FailedEvent(work.Command, pb.MutationOutcome_APPLIED, nil)
		_ = emit(work, result)
	}
	return
}

func TestZeroInFlightBudgetDoesNotLimitIndependentBatches(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchOperations = 1
	limits.MaxInFlight = 0
	const calls = 16
	adapter := &concurrencyAdapter{started: make(chan []*execution.Plan, calls), finish: make(chan struct{})}
	runtime, err := New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cancel()
		closeContext, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := runtime.Close(closeContext); err != nil {
			t.Error(err)
		}
	})
	tickets := make([]*Ticket, 0, calls)
	for index := range calls {
		work := plan(uint64(index), fmt.Sprint(index), false)
		ticket, failure, _ := runtime.Submit(ctx, work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	for range calls {
		select {
		case <-adapter.started:
		case <-ctx.Done():
			t.Fatal("zero budget limited execution", runtime.Snapshot())
		}
	}
	snapshot := runtime.Snapshot()
	if snapshot.Active != calls || snapshot.Pending != 0 {
		t.Fatal("zero budget retained waiting work", snapshot)
	}
	close(adapter.finish)
	for _, ticket := range tickets {
		if _, err := ticket.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		ticket.Ack()
	}
}

func TestInFlightBatchesBoundExecutionAndReturnCreditBeforeAck(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchOperations = 1
	limits.MaxInFlight = 3
	const calls = 16
	adapter := &concurrencyAdapter{started: make(chan []*execution.Plan, calls), finish: make(chan struct{})}
	runtime, err := New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cancel()
		closeContext, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := runtime.Close(closeContext); err != nil {
			t.Error(err)
		}
	})
	var tickets []*Ticket
	for index := range calls {
		work := plan(uint64(index), fmt.Sprint(index), false)
		work.WorkingBytes = 1 << 30
		ticket, failure, _ := runtime.Submit(ctx, work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	for range limits.MaxInFlight {
		select {
		case <-adapter.started:
		case <-ctx.Done():
			t.Fatal("available execution credit was not used", runtime.Snapshot())
		}
	}
	snapshot := runtime.Snapshot()
	if snapshot.Active != limits.MaxInFlight || snapshot.Pending != calls-limits.MaxInFlight || snapshot.PendingBytes != (calls-limits.MaxInFlight)*1024 || snapshot.WorkingBytes != limits.MaxInFlight*(1<<30) {
		t.Fatal("execution budget did not retain waiting work", snapshot)
	}
	select {
	case <-adapter.started:
		t.Fatal("execution exceeded its in-flight budget")
	default:
	}
	adapter.finish <- struct{}{}
	select {
	case <-adapter.started:
	case <-ctx.Done():
		t.Fatal("execution completion did not return credit before Ack")
	}
	snapshot = runtime.Snapshot()
	if snapshot.Active != limits.MaxInFlight || snapshot.Pending != calls-limits.MaxInFlight-1 || snapshot.Ready != 1 {
		t.Fatal("execution credit was confused with retained-result ownership", snapshot)
	}
	close(adapter.finish)
	for _, ticket := range tickets {
		if _, err := ticket.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		ticket.Ack()
	}
}

func TestBusyBackendKeepsRequestsQueuedAndFormsFullBatches(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxInFlight = 1
	limits.BatchOperations = 4
	limits.QueueOperations = 8
	adapter := &concurrencyAdapter{started: make(chan []*execution.Plan, 4), finish: make(chan struct{})}
	runtime, err := New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cancel()
		closeContext, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := runtime.Close(closeContext); err != nil {
			t.Error(err)
		}
	})
	first := plan(0, "first", false)
	ticket, failure, _ := runtime.Submit(ctx, first, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	tickets := []*Ticket{ticket}
	select {
	case plans := <-adapter.started:
		if len(plans) != 1 {
			t.Fatal("idle operation waited to fill a batch")
		}
	case <-ctx.Done():
		t.Fatal("idle operation did not dispatch")
	}
	for index := range limits.QueueOperations {
		work := plan(uint64(index+1), fmt.Sprint(index), false)
		ticket, failure, _ := runtime.Submit(ctx, work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	work := plan(100, "overflow", false)
	_, failure, changed := runtime.Submit(ctx, work, nil)
	if failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("busy backend did not cause bounded admission backpressure", failure)
	}
	if snapshot := runtime.Snapshot(); snapshot.Pending != limits.QueueOperations || snapshot.Active != 1 {
		t.Fatal("busy backend drained waiting credit into executions", snapshot)
	}
	runtime.BeginDrain()
	_, failure, _ = runtime.Submit(ctx, work, nil)
	if failure.GetCode() != pb.FailureCode_UNAVAILABLE {
		t.Fatal("draining store admitted new work", failure)
	}
	adapter.finish <- struct{}{}
	select {
	case plans := <-adapter.started:
		if len(plans) != limits.BatchOperations {
			t.Fatal("waiting independent requests did not form a full batch", len(plans))
		}
	case <-ctx.Done():
		t.Fatal("drain did not execute accepted waiting work")
	}
	select {
	case <-changed:
	default:
		t.Fatal("dispatch did not wake capacity waiters")
	}
	close(adapter.finish)
	for _, ticket := range tickets {
		if _, err := ticket.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		ticket.Ack()
	}
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if snapshot := runtime.Snapshot(); snapshot.Active != 0 || snapshot.Pending != 0 || snapshot.Retained != 0 {
		t.Fatal("drained store leaked execution or queue credit", snapshot)
	}
}

type canceledExecutionAdapter struct {
	lifecycleAdapter
	started  chan string
	canceled chan struct{}
	finish   chan struct{}
}

func (adapter *canceledExecutionAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) {
	work := plans[0]
	adapter.started <- work.Key
	outcome := pb.MutationOutcome_APPLIED
	var failure *pb.Failure
	if work.Key == "canceled" {
		<-ctx.Done()
		close(adapter.canceled)
		<-adapter.finish
		outcome = pb.MutationOutcome_UNKNOWN
		failure = protocol.ContextFailure(ctx)
	}
	result := execution.FailedEvent(work.Command, outcome, failure)
	_ = emit(work, result)
	return
}

func TestCancellationRetainsCreditUntilAdapterStops(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxInFlight = 1
	adapter := &canceledExecutionAdapter{started: make(chan string, 2), canceled: make(chan struct{}), finish: make(chan struct{})}
	runtime, err := New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cancel()
		select {
		case <-adapter.finish:
		default:
			close(adapter.finish)
		}
		closeContext, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := runtime.Close(closeContext); err != nil {
			t.Error(err)
		}
	})
	canceledPlan := plan(1, "canceled", false)
	abandoned, failure, _ := runtime.Submit(ctx, canceledPlan, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	select {
	case <-adapter.started:
	case <-ctx.Done():
		t.Fatal("execution did not start")
	}
	nextPlan := plan(2, "next", false)
	next, failure, _ := runtime.Submit(ctx, nextPlan, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	abandoned.Abandon()
	select {
	case <-adapter.canceled:
	case <-ctx.Done():
		t.Fatal("final caller abandonment did not cancel adapter execution")
	}
	if snapshot := runtime.Snapshot(); snapshot.Active != 1 || snapshot.Pending != 1 {
		t.Fatal("caller cancellation released an execution still using the backend", snapshot)
	}
	select {
	case <-adapter.started:
		t.Fatal("queued work reused credit before adapter stopped")
	default:
	}
	close(adapter.finish)
	result, err := next.Wait(ctx)
	if err != nil || result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("stopped adapter did not return its execution credit", result, err)
	}
	next.Ack()
}

func TestOldestBatchKeyKeepsProgressUnderInFlightLimit(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchWait = 0
	limits.MaxInFlight = 1
	limits.BatchOperations = 2
	runtime := newRuntime(nil, limits)
	var tickets []*Ticket
	for index, batchKey := range []string{"a", "b", "a"} {
		work := plan(uint64(index), fmt.Sprint(index), false)
		work.BatchKey = batchKey
		ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	runtime.mu.Lock()
	first, _ := runtime.selectLocked(time.Now())
	blocked, _ := runtime.selectLocked(time.Now())
	runtime.mu.Unlock()
	if first == nil || len(first.items) != 2 || first.items[0] != tickets[0] || first.items[1] != tickets[2] || blocked != nil {
		t.Fatal("batch compatibility or in-flight budget changed")
	}
	work := plan(3, "newest-a", false)
	work.BatchKey = "a"
	newest, failure, _ := runtime.Submit(t.Context(), work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	tickets = append(tickets, newest)
	runtime.mu.Lock()
	finish(runtime, first)
	second, _ := runtime.selectLocked(time.Now())
	if second != nil {
		finish(runtime, second)
	}
	third, _ := runtime.selectLocked(time.Now())
	if third != nil {
		finish(runtime, third)
	}
	runtime.mu.Unlock()
	if second == nil || len(second.items) != 1 || second.items[0] != tickets[1] || third == nil || third.items[0] != newest {
		t.Fatal("new compatible work starved the oldest waiting batch key")
	}
	for _, ticket := range tickets {
		ticket.Ack()
	}
}

func TestExecutionBudgetIsPerStore(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxInFlight = 1
	busyAdapter := &concurrencyAdapter{started: make(chan []*execution.Plan, 1), finish: make(chan struct{})}
	busy, err := New(busyAdapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close(context.Background())
	otherAdapter := &singleExecutionAdapter{}
	other, err := New(otherAdapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close(context.Background())
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	first := plan(1, "busy", false)
	blocked, failure, _ := busy.Submit(ctx, first, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	defer blocked.Abandon()
	select {
	case <-busyAdapter.started:
	case <-ctx.Done():
		t.Fatal("busy Store did not consume execution credit")
	}
	second := plan(2, "other", false)
	ticket, failure, _ := other.Submit(ctx, second, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	event, err := ticket.Wait(ctx)
	if err != nil || event.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("another Store's full execution budget blocked independent work", event, err)
	}
	ticket.Ack()
	if snapshot := busy.Snapshot(); snapshot.Active != 1 {
		t.Fatal("busy Store released credit without finishing", snapshot)
	}
}
