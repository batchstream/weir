package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/execution"
)

func TestMemoryLimitedFlightsAreSaturatedBeforeConfiguredConcurrency(t *testing.T) {
	limits := DefaultLimits()
	limits.WorkingBytes = 128 << 20
	limits.Concurrency = 8
	limits.BatchOperations = 1
	runtime := newRuntime(nil, limits)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var tickets []*Ticket
	for index := range 9 {
		work := plan(uint64(index+1), fmt.Sprint(index), true)
		work.WorkingBytes = 17 << 20
		work.ResultBytes = 2048
		ticket, failure, _ := runtime.Submit(ctx, work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	runtime.mu.Lock()
	var flights []*batch
	for range 7 {
		flight := runtime.selectLocked(time.Now())
		if flight == nil {
			runtime.mu.Unlock()
			t.Fatal("working budget failed to admit its seven bounded slots")
		}
		flights = append(flights, flight)
	}
	last := flights[len(flights)-1]
	if runtime.active != 7 || runtime.controller.window != 8 || !last.saturated || runtime.selectLocked(time.Now()) != nil {
		runtime.mu.Unlock()
		t.Fatal("memory-limited backlog was not recognized as saturation")
	}
	now := time.Unix(10, 0)
	last.duration = 3 * time.Millisecond
	for range 4 {
		observeLatency(&runtime.controller, last, now)
	}
	last.duration = 20 * time.Millisecond
	for index := range 3 {
		observeLatency(&runtime.controller, last, now.Add(time.Second+time.Duration(index)*50*time.Millisecond))
	}
	if runtime.controller.window != 7 {
		runtime.mu.Unlock()
		t.Fatal("successful slow memory-limited work failed to reduce backend pressure")
	}
	for _, flight := range flights {
		finish(runtime, flight)
	}
	cancel()
	runtime.cancelQueuedLocked()
	runtime.mu.Unlock()
	for _, ticket := range tickets {
		ticket.Ack()
	}
}

func TestMemorySaturationIncludesDemandArrivingAfterDispatch(t *testing.T) {
	limits := DefaultLimits()
	limits.WorkingBytes = 128 << 20
	limits.Concurrency = 8
	limits.BatchOperations = 1
	adapter := &scanTestAdapter{}
	runtime := newRuntime(adapter, limits)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var tickets []*Ticket
	var flights []*batch
	for index := range 7 {
		work := plan(uint64(index+1), fmt.Sprint(index), true)
		work.WorkingBytes = 17 << 20
		work.ResultBytes = 2048
		ticket, failure, _ := runtime.Submit(ctx, work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
		runtime.mu.Lock()
		flight := runtime.selectLocked(time.Now())
		runtime.mu.Unlock()
		if flight == nil || flight.saturated {
			t.Fatal("dispatch without pending work was incorrectly saturated")
		}
		flights = append(flights, flight)
	}
	work := plan(8, "late", true)
	work.WorkingBytes = 17 << 20
	work.ResultBytes = 2048
	ticket, failure, _ := runtime.Submit(ctx, work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	tickets = append(tickets, ticket)
	last := flights[len(flights)-1]
	runtime.execute(last)
	if !last.saturated {
		t.Fatal("completion released its memory permit before recognizing late demand")
	}
	runtime.mu.Lock()
	for _, flight := range flights[:len(flights)-1] {
		finish(runtime, flight)
	}
	cancel()
	runtime.cancelQueuedLocked()
	runtime.mu.Unlock()
	for _, ticket := range tickets {
		ticket.Ack()
	}
}

func TestWorkingBudgetBackfillsIndependentWorkWithoutBreakingSessionOrder(t *testing.T) {
	limits := DefaultLimits()
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
		work.Singleton = true
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
	if runtime.selectLocked(time.Now()) != nil || !runtime.saturatedLocked() {
		runtime.mu.Unlock()
		t.Fatal("ordered successor bypassed its blocked predecessor or hid memory pressure")
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

func TestNativeAndLuaExplicitHealthyFeedbackCanRecoverWithoutLatencyLearning(t *testing.T) {
	for _, kind := range []string{"native", "lua"} {
		t.Run(kind, func(t *testing.T) {
			c := controller{window: 1}
			b := latencyBatch(1, 1024, "put")
			b.items[0].plan.Singleton = true
			b.items[0].plan.Streaming = kind == "native"
			b.latency, b.latencyEligible = classifyLatency(b.items)
			b.duration = time.Minute
			now := time.Unix(10, 0)
			for range 4 {
				observeLatency(&c, b, now)
			}
			if c.window != 2 || len(c.profiles) != 0 || c.baseline != 0 || c.sample != 0 {
				t.Fatal("explicit healthy opaque work failed recovery or learned consumer/Lua latency", c)
			}
		})
	}
}

func TestSuccessfulSlowFloorDoesNotPauseButExplicitRejectionDoes(t *testing.T) {
	c := controller{window: 1}
	b := latencyBatch(16, 1024, "read")
	now := time.Unix(10, 0)
	opts := observation{batch: b, feedback: execution.Healthy, maximum: 1, now: now}
	b.duration = time.Millisecond
	for range 4 {
		c.observe(opts)
	}
	b.duration = 20 * time.Millisecond
	for index := range 100 {
		opts.now = now.Add(time.Duration(index) * 100 * time.Millisecond)
		if reason := c.observe(opts); reason != "" || c.window != 1 || c.epoch != 0 || !c.cooldown.IsZero() || c.credit != 0 {
			t.Fatal("successful slow floor introduced idle pauses or recovery credit", c, reason)
		}
	}
	opts.feedback = execution.Congested
	reason := c.observe(opts)
	if reason != "backend" || c.window != 1 || c.epoch != 1 || !c.cooldown.After(opts.now) {
		t.Fatal("explicit rejection at floor failed to pause dispatch", c, reason)
	}
	b.epoch = c.epoch
	b.duration = time.Millisecond
	opts.feedback = execution.Healthy
	opts.maximum = 2
	opts.now = opts.now.Add(time.Second)
	for range 4 {
		c.observe(opts)
	}
	if c.window != 2 {
		t.Fatal("healthy floor did not recover after explicit rejection cooldown", c)
	}
}
