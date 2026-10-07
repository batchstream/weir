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
