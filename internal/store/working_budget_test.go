package store

import (
	"context"
	"testing"
	"time"
)

func TestWorkingBudgetBackfillsIndependentWorkWithoutBreakingSessionOrder(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchOperations = 1
	limits.WorkingBytes = 48 << 20
	runtime := newRuntime(nil, limits)
	session := runtime.NewSession()
	defer session.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workloads := []struct {
		key   string
		bytes int
	}{
		{"active", 32 << 20},
		{"ordered", 24 << 20},
		{"ordered", 8 << 20},
		{"independent", 8 << 20},
	}
	var tickets []*Ticket
	for index, workload := range workloads {
		work := plan(uint64(index+1), workload.key, false)
		work.WorkingBytes = workload.bytes
		ticket, failure, _ := runtime.Submit(ctx, work, session)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	runtime.mu.Lock()
	first := runtime.selectLocked(time.Now())
	second := runtime.selectLocked(time.Now())
	if first == nil || second == nil || second.items[0] != tickets[3] {
		runtime.mu.Unlock()
		t.Fatal("a blocked large operation prevented independent bounded work")
	}
	if runtime.selectLocked(time.Now()) != nil {
		runtime.mu.Unlock()
		t.Fatal("ordered successor bypassed its blocked predecessor")
	}
	finish(runtime, first)
	third := runtime.selectLocked(time.Now())
	if third == nil || third.items[0] != tickets[1] || runtime.selectLocked(time.Now()) != nil {
		runtime.mu.Unlock()
		t.Fatal("session order changed after working budget became available")
	}
	finish(runtime, third)
	fourth := runtime.selectLocked(time.Now())
	if fourth == nil || fourth.items[0] != tickets[2] {
		runtime.mu.Unlock()
		t.Fatal("ordered successor failed to resume")
	}
	finish(runtime, second)
	finish(runtime, fourth)
	runtime.mu.Unlock()
	for _, ticket := range tickets {
		ticket.Ack()
	}
}

func TestWorkingBudgetChargesLargestMemberOfSharedBatch(t *testing.T) {
	limits := DefaultLimits()
	limits.WorkingBytes = 24 << 20
	runtime := newRuntime(nil, limits)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var tickets []*Ticket
	for index, megabytes := range []int{8, 24, 16, 8} {
		work := plan(uint64(index+1), string(rune('a'+index)), false)
		work.WorkingBytes = megabytes << 20
		if index == 3 {
			work.BatchKey = "separate"
		}
		ticket, failure, _ := runtime.Submit(ctx, work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	first := selectCrossBatch(runtime)
	if first == nil || len(first.items) != 3 || runtime.Snapshot().WorkingBytes != 24<<20 {
		t.Fatal("shared batch did not reserve its largest working envelope", runtime.Snapshot())
	}
	if selectCrossBatch(runtime) != nil {
		t.Fatal("another batch exceeded the remaining working budget")
	}
	runtime.mu.Lock()
	finish(runtime, first)
	runtime.mu.Unlock()
	second := selectCrossBatch(runtime)
	if second == nil || len(second.items) != 1 || second.items[0] != tickets[3] || runtime.Snapshot().WorkingBytes != 8<<20 {
		t.Fatal("released working budget did not admit the pending batch", runtime.Snapshot())
	}
	runtime.mu.Lock()
	finish(runtime, second)
	runtime.mu.Unlock()
	for _, ticket := range tickets {
		ticket.Ack()
	}
	if snapshot := runtime.Snapshot(); snapshot.WorkingBytes != 0 || snapshot.Retained != 0 {
		t.Fatal("completed batches retained reservations", snapshot)
	}
}
