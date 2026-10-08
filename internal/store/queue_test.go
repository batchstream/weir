package store

import (
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestQueueCountAndBytesReleaseAtDispatch(t *testing.T) {
	for _, countBound := range []bool{true, false} {
		name := "bytes"
		if countBound {
			name = "operations"
		}
		t.Run(name, func(t *testing.T) {
			limits := DefaultLimits()
			limits.BatchOperations = 1
			if countBound {
				limits.QueueOperations = 2
			} else {
				limits.QueueBytes = 2048
			}
			runtime := newRuntime(nil, limits)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var tickets []*Ticket
			for index := range 2 {
				work := plan(uint64(index), string(rune('a'+index)), false)
				ticket, failure, _ := runtime.Submit(ctx, work, nil)
				if failure != nil {
					t.Fatal(failure)
				}
				tickets = append(tickets, ticket)
			}
			work := plan(3, "third", false)
			_, failure, changed := runtime.Submit(ctx, work, nil)
			if failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
				t.Fatal("full queue accepted an operation", failure)
			}
			runtime.mu.Lock()
			batch := runtime.selectLocked(time.Now())
			runtime.mu.Unlock()
			if batch == nil {
				t.Fatal("nothing dispatched")
			}
			select {
			case <-changed:
			default:
				t.Fatal("dispatch did not wake queue waiters")
			}
			ticket, failure, _ := runtime.Submit(ctx, work, nil)
			if failure != nil {
				t.Fatal("active operation retained queue capacity", failure)
			}
			tickets = append(tickets, ticket)
			snapshot := runtime.Snapshot()
			if snapshot.Pending != 2 || snapshot.PendingBytes != 2048 || snapshot.Active != 1 {
				t.Fatal(snapshot)
			}
			cancel()
			runtime.mu.Lock()
			runtime.cancelQueuedLocked()
			finish(runtime, batch)
			runtime.mu.Unlock()
			for _, ticket := range tickets {
				ticket.Ack()
			}
			if snapshot := runtime.Snapshot(); snapshot.PendingBytes != 0 || snapshot.Retained != 0 {
				t.Fatal("queue capacity leaked", snapshot)
			}
		})
	}
}

func TestBatchDeadlineComesOnlyFromInterestedCallers(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchOperations = 2
	runtime := newRuntime(nil, limits)
	bounded, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	callers := []context.Context{bounded, t.Context()}
	for index, caller := range callers {
		work := plan(uint64(index), string(rune('a'+index)), false)
		if _, failure, _ := runtime.Submit(caller, work, nil); failure != nil {
			t.Fatal(failure)
		}
	}
	runtime.mu.Lock()
	batch := runtime.selectLocked(time.Now())
	runtime.mu.Unlock()
	if batch == nil || len(batch.items) != 2 {
		t.Fatal("missing shared batch")
	}
	defer batch.cancel()
	if _, bounded := batch.ctx.Deadline(); bounded {
		t.Fatal("unbounded caller inherited another deadline")
	}
	stop()
	if batch.ctx.Err() != nil {
		t.Fatal("one caller canceled the shared batch")
	}
	runtime.mu.Lock()
	finish(runtime, batch)
	runtime.mu.Unlock()
	for _, ticket := range batch.items {
		ticket.Ack()
	}
}
