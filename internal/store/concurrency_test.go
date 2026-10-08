package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type concurrencyAdapter struct {
	lifecycleAdapter
	started chan struct{}
	finish  chan struct{}
}

func (adapter *concurrencyAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) bool {
	adapter.started <- struct{}{}
	select {
	case <-adapter.finish:
	case <-ctx.Done():
	}
	for _, work := range plans {
		result := execution.FailedEvent(work.Command, pb.MutationOutcome_APPLIED, nil)
		_ = emit(work, result)
	}
	return false
}

func TestIndependentBatchesHaveNoConcurrencyOrWorkingMemoryCap(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchOperations = 1
	const calls = 16
	adapter := &concurrencyAdapter{started: make(chan struct{}, calls), finish: make(chan struct{})}
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
	for range calls {
		select {
		case <-adapter.started:
		case <-ctx.Done():
			t.Fatal("independent execution was capped", runtime.Snapshot())
		}
	}
	snapshot := runtime.Snapshot()
	if snapshot.Active != calls || snapshot.Pending != 0 || snapshot.PendingBytes != 0 {
		t.Fatal("dispatch retained queue capacity or limited execution", snapshot)
	}
	close(adapter.finish)
	for _, ticket := range tickets {
		if _, err := ticket.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		ticket.Ack()
	}
}
