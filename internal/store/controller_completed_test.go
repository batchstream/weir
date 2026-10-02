package store

import (
	"testing"
	"time"

	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
)

func nativeProbeBatch() *batch {
	b := latencyBatch(1, 1024, "put")
	b.items[0].plan.Singleton = true
	b.items[0].plan.Streaming = true
	b.latency, b.latencyEligible = classifyLatency(b.items)
	b.duration = time.Minute
	return b
}

func TestCompletedNativeTransportProbesOncePerSecondAfterCongestion(t *testing.T) {
	c := controller{window: 4}
	b := nativeProbeBatch()
	now := time.Unix(10, 0)
	opts := observation{batch: b, feedback: execution.Congested, maximum: 4, now: now}
	c.observe(opts)
	if c.window != 2 {
		t.Fatal("missing congestion reduction", c)
	}
	b.epoch = c.epoch
	opts.feedback = execution.Completed
	opts.now = now.Add(time.Second - time.Nanosecond)
	c.observe(opts)
	if c.window != 2 {
		t.Fatal("opaque completed transport grew before the one-second probe interval", c)
	}
	opts.now = now.Add(time.Second)
	c.observe(opts)
	if c.window != 3 {
		t.Fatal("complete Native exchange did not probe recovered capacity", c)
	}
	opts.now = now.Add(2 * time.Second)
	c.observe(opts)
	if c.window != 3 {
		t.Fatal("an old concurrent flight duplicated the capacity probe", c)
	}
	b.epoch = c.epoch
	opts.now = now.Add(2*time.Second - time.Nanosecond)
	c.observe(opts)
	if c.window != 3 {
		t.Fatal("capacity probes were not independently rate limited", c)
	}
	opts.now = now.Add(2 * time.Second)
	c.observe(opts)
	if c.window != 4 {
		t.Fatal("Native-only traffic remained stuck after backend recovery", c)
	}
	b.epoch = c.epoch
	for index := range 20 {
		opts.now = now.Add(time.Duration(index+3) * time.Second)
		c.observe(opts)
	}
	if c.window != 4 || c.credit != 0 || len(c.profiles) != 0 || c.baseline != 0 || c.sample != 0 {
		t.Fatal("completed transport exceeded bounds or polluted successful-record evidence", c)
	}
}

func TestCompletedNativeTransportRequiresInterestDemandAndCooldown(t *testing.T) {
	c := controller{window: 2}
	b := nativeProbeBatch()
	now := time.Unix(10, 0)
	opts := observation{batch: b, feedback: execution.Completed, maximum: 4, now: now}
	b.recoveryEligible = false
	c.observe(opts)
	if c.window != 2 || c.credit != 0 {
		t.Fatal("canceled complete transport recovered capacity", c)
	}
	b.recoveryEligible = true
	b.saturated = false
	c.observe(opts)
	if c.window != 2 {
		t.Fatal("low demand created an unnecessary capacity probe", c)
	}
	b.saturated = true
	c.cooldown = now.Add(time.Second)
	c.observe(opts)
	if c.window != 2 {
		t.Fatal("opaque reply bypassed backend cooldown", c)
	}
	opts.now = now.Add(2 * time.Second)
	opts.feedback = execution.Neutral
	c.observe(opts)
	if c.window != 2 || c.credit != 0 {
		t.Fatal("business error or incomplete exchange recovered capacity", c)
	}
	opts.feedback = execution.Completed
	c.observe(opts)
	if c.window != 3 {
		t.Fatal("interested complete exchange failed to probe after cooldown", c)
	}
}

func TestCompletedTransportMetricsRemainDistinctFromHealthyBusinessEvidence(t *testing.T) {
	limits := DefaultLimits()
	runtime := newRuntime(nil, limits)
	runtime.controller.window = 1
	b := nativeProbeBatch()
	runtime.mu.Lock()
	runtime.observeLocked(b, execution.Completed)
	runtime.mu.Unlock()
	snapshot := runtime.Snapshot()
	if snapshot.Feedback != "completed" || snapshot.Window != 2 || snapshot.LatencyProfiles != 0 {
		t.Fatal("completed transport was conflated with business health", snapshot)
	}
	families := testmetrics.Gather(t, runtime)
	completedLabels := map[string]string{"feedback": "completed"}
	healthyLabels := map[string]string{"feedback": "healthy"}
	if testmetrics.Sample(families, "weir_store_feedback", completedLabels).GetGauge().GetValue() != 1 || testmetrics.Sample(families, "weir_store_feedback", healthyLabels).GetGauge().GetValue() != 0 {
		t.Fatal("transport probe feedback was not independently observable")
	}
}
