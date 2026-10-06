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

func (adapter *concurrencyAdapter) PrepareRecord(record *execution.Record) (*execution.Plan, *pb.Failure) {
	work, failure := adapter.lifecycleAdapter.PrepareRecord(record)
	work.WorkingBytes = 16 << 20
	return work, failure
}

func (adapter *concurrencyAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) bool {
	adapter.started <- struct{}{}

	select {
	case <-adapter.finish:
	case <-ctx.Done():
	}
	for _, work := range plans {
		result := execution.FailedEvent(work.Command, pb.MutationOutcome_APPLIED, nil)
		output := result
		_ = emit(work, output)
	}
	return false
}

func TestCompletionPreservesConfiguredDispatch(t *testing.T) {
	for _, capacity := range []int{3, 4} {
		name := fmt.Sprintf("working_slots=%d", capacity)
		t.Run(name, func(t *testing.T) {
			limits := DefaultLimits()
			limits.Concurrency = 4
			limits.BatchOperations = 1
			limits.WorkingBytes = capacity * (16 << 20)
			adapter := &concurrencyAdapter{started: make(chan struct{}, 8), finish: make(chan struct{}, 8)}
			runtime, err := New(adapter, limits)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer func() {
				cancel()
				closeContext, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				if err := runtime.Close(closeContext); err != nil {
					t.Error(err)
				}
			}()
			var tickets []*Ticket
			for i := range 8 {
				work := plan(1, fmt.Sprint(i), false)
				work.WorkingBytes = 16 << 20
				var ticket *Ticket
				var failure *pb.Failure
				ticket, failure, _ = runtime.Submit(ctx, work, nil)
				if failure != nil {
					t.Fatal(failure)
				}
				tickets = append(tickets, ticket)
			}
			for range capacity {
				select {
				case <-adapter.started:
				case <-ctx.Done():
					t.Fatal("configured capacity did not dispatch", runtime.Snapshot())
				}
			}
			if snapshot := runtime.Snapshot(); snapshot.Active != capacity || snapshot.Pending != 8-capacity || snapshot.WorkingBytes != capacity*(16<<20) {
				t.Fatal("execution exceeded configured or working-byte capacity", snapshot)
			}
			adapter.finish <- struct{}{}
			// Completing one call must immediately admit the next queued batch.
			select {
			case <-adapter.started:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("completion left configured capacity idle", runtime.Snapshot())
			}
			if snapshot := runtime.Snapshot(); snapshot.Active != capacity || snapshot.ConcurrencyLimit != limits.Concurrency {
				t.Fatal("completion changed dispatch capacity", snapshot)
			}
			for range 7 {
				adapter.finish <- struct{}{}
			}
			for _, ticket := range tickets {
				_, err = ticket.Wait(ctx)
				if err != nil {
					t.Fatal(err)
				}
				ticket.Ack()
			}
		})
	}
}
