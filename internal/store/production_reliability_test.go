package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestSparseReadsBypassCollectingNamespacesInRunningScheduler(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchWait = time.Hour
	limits.MaxInFlight = 1
	adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 64)}
	runtime, err := New(adapter, limits, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	var writes []*Ticket
	for index := range 16 {
		work := plan(uint64(index), fmt.Sprint(index), false)
		work.BatchKey = fmt.Sprintf("collecting-%d", index)
		ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		writes = append(writes, ticket)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for index := range 32 {
		work := plan(uint64(index), fmt.Sprint(index), true)
		work.BatchKey = fmt.Sprintf("read-%d", index)
		ticket, failure, _ := runtime.Submit(ctx, work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		event, err := ticket.Wait(ctx)
		if err != nil || event.GetReadResult().GetDocument() == nil {
			t.Fatal("sparse read waited behind unrelated write collection", index, event, err)
		}
		ticket.Ack()
	}
	if snapshot := runtime.Snapshot(); snapshot.Pending != len(writes) || snapshot.Active != 0 || snapshot.Ready != 0 {
		t.Fatal("sparse reads flushed unrelated writes or retained execution credit", snapshot)
	}
	runtime.BeginDrain()
	for _, ticket := range writes {
		if _, err := ticket.Wait(ctx); err != nil {
			t.Fatal("collected writes failed to drain after reads completed", err)
		}
		ticket.Ack()
	}
	waitReleased(t, runtime)
}

func TestSlowNativeConsumersUseBoundedOutputCapacity(t *testing.T) {
	limits := DefaultLimits()
	limits.QueueOperations = 1
	adapter := &singleExecutionAdapter{}
	runtime, err := New(adapter, limits, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	var sessions []*Session
	for index := range limits.MaxInFlight {
		session := runtime.NewSession()
		defer session.Close()
		sessions = append(sessions, session)
		work := plan(uint64(index), fmt.Sprint(index), false)
		work.Command = nativeCall()
		_, failure, _ := runtime.Submit(ctx, work, session)
		if failure != nil {
			t.Fatal(failure)
		}
		select {
		case <-session.Events:
		case <-ctx.Done():
			t.Fatal("Native invocation did not reach its stalled consumer")
		}
	}
	work := plan(100, "ordinary", false)
	ticket, failure, _ := runtime.Submit(ctx, work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	event, err := ticket.Wait(ctx)
	if err != nil || event.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("bounded Native output consumed every backend slot", event, err)
	}
	ticket.Ack()
	if snapshot := runtime.Snapshot(); snapshot.Active != 0 || snapshot.Paused != limits.MaxInFlight || snapshot.Pending != 0 || runtime.memory.Snapshot().UsedBytes > runtime.memory.Snapshot().BudgetBytes {
		t.Fatal("stalled Native output escaped its separate bounds", snapshot)
	}
	for _, session := range sessions {
		session.Close()
	}
	waitReleased(t, runtime)
	if snapshot := runtime.Snapshot(); snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 || snapshot.WorkingBytes != 0 {
		t.Fatal("canceling saturated Native consumers leaked accounting", snapshot)
	}
}
