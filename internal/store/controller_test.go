package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/execution"
)

func latencyBatch(count, bytes int, action string) *batch {
	items := make([]*Ticket, count)
	for index := range items {
		plan := recordActionPlan(uint64(index+1), fmt.Sprint(index), action)
		plan.Bytes = bytes
		plan.BatchKey = "records"
		items[index] = &Ticket{plan: plan}
	}
	class, eligible := classifyLatency(items)
	b := &batch{items: items, latency: class, latencyEligible: eligible, recoveryEligible: true, saturated: true}
	return b
}

func observeLatency(c *controller, b *batch, now time.Time) string {
	b.epoch = c.epoch
	opts := observation{batch: b, feedback: execution.Healthy, maximum: 8, now: now}
	return c.observe(opts)
}

func TestLatencyControllerCapacityDecreaseBeforeRejectionAndRecovery(t *testing.T) {
	c := controller{window: 8}
	b := latencyBatch(16, 1024, "put")
	now := time.Unix(10, 0)
	b.duration = 3 * time.Millisecond
	for index := range 4 {
		observeLatency(&c, b, now.Add(time.Duration(index)*time.Millisecond))
	}
	if c.baseline != b.duration || len(c.profiles) != 1 {
		t.Fatal("healthy baseline did not warm", c)
	}
	b.duration = 12 * time.Millisecond
	for index := range 3 {
		peer := *b
		peer.saturated = false
		observeLatency(&c, &peer, now.Add(time.Second+time.Duration(index)*50*time.Millisecond))
		reason := observeLatency(&c, b, now.Add(time.Second+time.Duration(index)*50*time.Millisecond))
		if index < 2 && (reason != "" || c.window != 8) {
			t.Fatal("one or two outliers reduced concurrency", c)
		}
		if index == 2 && (reason != "latency" || c.window != 7) {
			t.Fatal("successful slow calls failed to reduce pressure", c, reason)
		}
	}
	oldEpoch := b.epoch
	opts := observation{batch: b, feedback: execution.Congested, maximum: 8, now: now.Add(2 * time.Second)}
	c.observe(opts)
	if oldEpoch == c.epoch || c.window != 7 {
		t.Fatal("old concurrent work reduced the window twice", c)
	}
	b.duration = 3 * time.Millisecond
	for index := range 7 {
		observeLatency(&c, b, now.Add(2200*time.Millisecond+time.Duration(index)*time.Millisecond))
	}
	if c.window != 8 {
		t.Fatal("healthy database recovery failed to grow concurrency", c)
	}
}

func TestLatencyControllerNoiseLowDemandAndPermanentIdleChange(t *testing.T) {
	c := controller{window: 8}
	b := latencyBatch(16, 1024, "read")
	now := time.Unix(10, 0)
	b.duration = 3 * time.Millisecond
	for index := range 4 {
		observeLatency(&c, b, now.Add(time.Duration(index)*time.Millisecond))
	}
	for index := range 10 {
		b.duration = 40 * time.Millisecond
		observeLatency(&c, b, now.Add(time.Duration(index)*time.Second))
		b.duration = 3 * time.Millisecond
		observeLatency(&c, b, now.Add(time.Duration(index)*time.Second+time.Millisecond))
	}
	if c.window != 8 {
		t.Fatal("isolated latency spikes reduced concurrency", c)
	}
	b.saturated, b.lowDemand = false, true
	b.duration = 40 * time.Millisecond
	for index := range 32 {
		observeLatency(&c, b, now.Add(time.Duration(index)*time.Second))
	}
	if c.window != 8 || c.baseline < 39*time.Millisecond {
		t.Fatal("idle work reduced capacity or permanently pinned an old latency", c)
	}
	// Recovery changes the estimate without turning one fast sample into a
	// permanent minimum that normal concurrent work can no longer satisfy.
	b.duration = 3 * time.Millisecond
	observeLatency(&c, b, now.Add(time.Minute))
	if c.baseline <= b.duration || c.baseline >= 39*time.Millisecond {
		t.Fatal("faster recovered backend did not smoothly refresh baseline", c)
	}
	for index := range 32 {
		observeLatency(&c, b, now.Add(time.Minute+time.Duration(index+1)*time.Millisecond))
	}
	if c.baseline > 3500*time.Microsecond {
		t.Fatal("recovered fixed service cost did not converge", c)
	}
}

func TestLatencyControllerSustainedPressureFloorCooldownAndBoundedRecovery(t *testing.T) {
	c := controller{window: 4}
	b := latencyBatch(16, 1024, "read")
	now := time.Unix(10, 0)
	opts := observation{batch: b, feedback: execution.Healthy, maximum: 4, now: now}
	b.duration = 3 * time.Millisecond
	for range 4 {
		c.observe(opts)
	}
	b.duration = 20 * time.Millisecond
	for stage := range 6 {
		for index := range 3 {
			b.epoch = c.epoch
			opts.now = now.Add(time.Duration(stage+1)*time.Second + time.Duration(index)*50*time.Millisecond)
			c.observe(opts)
		}
	}
	if c.window != 1 {
		t.Fatal("sustained pressure did not reach a bounded positive floor", c)
	}
	b.duration = 3 * time.Millisecond
	b.epoch = c.epoch
	opts.now = c.cooldown.Add(-time.Nanosecond)
	for range 20 {
		c.observe(opts)
	}
	if c.window != 1 {
		t.Fatal("healthy calls bypassed cooldown", c)
	}
	b.saturated = false
	opts.now = now.Add(time.Minute)
	for range 20 {
		c.observe(opts)
	}
	if c.window != 1 {
		t.Fatal("low demand increased backend concurrency", c)
	}
	b.saturated = true
	for index := range 100 {
		b.epoch = c.epoch
		opts.now = now.Add(time.Minute + time.Duration(index)*100*time.Millisecond)
		c.observe(opts)
	}
	if c.window != 4 {
		t.Fatal("recovery failed to reach or exceeded the configured hard cap", c)
	}
}

func TestLatencyControllerChangingBatchSizeAndBytesShareComparableEvidence(t *testing.T) {
	c := controller{window: 8}
	now := time.Unix(10, 0)
	for index := range 32 {
		count := 9 + index%8
		bytes := 1025 + index*25
		b := latencyBatch(count, bytes, "put")
		// Fixed 1ms plus per-record and per-byte work varies normally.
		b.duration = time.Millisecond + time.Duration(count)*time.Millisecond + time.Duration(count*bytes)*time.Microsecond
		observeLatency(&c, b, now.Add(time.Duration(index)*100*time.Millisecond))
	}
	if c.window != 8 || len(c.profiles) != 1 {
		t.Fatal("normal batch or byte variation changed concurrency or fragmented evidence", c)
	}
	for index := range 3 {
		b := latencyBatch(9+index, 1025, "put")
		b.duration = time.Duration(9+index) * 10 * time.Millisecond
		observeLatency(&c, b, now.Add(4*time.Second+time.Duration(index)*50*time.Millisecond))
	}
	if c.window != 7 {
		t.Fatal("changing batch sizes prevented capacity loss detection", c)
	}
	large := latencyBatch(16, 1<<20, "put")
	large.duration = time.Second
	for index := range 7 {
		observeLatency(&c, large, now.Add(6*time.Second+time.Duration(index)*time.Millisecond))
	}
	if c.window != 8 || len(c.profiles) != 2 {
		t.Fatal("larger documents reused small-document latency evidence", c)
	}
}

func TestLatencyControllerNeutralAndCanceledSamplesDoNotChangeEvidence(t *testing.T) {
	c := controller{window: 8}
	b := latencyBatch(16, 1024, "put")
	now := time.Unix(10, 0)
	b.duration = 3 * time.Millisecond
	for index := range 4 {
		observeLatency(&c, b, now.Add(time.Duration(index)*time.Millisecond))
	}
	b.duration = time.Second
	opts := observation{batch: b, feedback: execution.Neutral, maximum: 8, now: now.Add(time.Second)}
	for range 20 {
		c.observe(opts)
	}
	if c.window != 8 || c.baseline != 3*time.Millisecond || c.sample != 3*time.Millisecond {
		t.Fatal("business failure polluted latency model", c)
	}
	b.latencyEligible = false
	for index := range 20 {
		observeLatency(&c, b, now.Add(time.Duration(index)*time.Second))
	}
	if c.window != 8 || c.baseline != 3*time.Millisecond || c.sample != 3*time.Millisecond {
		t.Fatal("caller cancellation or opaque work polluted latency model", c)
	}
}

func TestLatencyControllerSlowEvidenceExpiresAcrossIdleGaps(t *testing.T) {
	c := controller{window: 8}
	b := latencyBatch(16, 1024, "read")
	now := time.Unix(10, 0)
	b.duration = time.Millisecond
	for range 4 {
		observeLatency(&c, b, now)
	}
	b.duration = 20 * time.Millisecond
	observeLatency(&c, b, now.Add(time.Second))
	observeLatency(&c, b, now.Add(time.Second+50*time.Millisecond))
	observeLatency(&c, b, now.Add(time.Minute))
	if c.window != 8 {
		t.Fatal("isolated bursts retained stale congestion evidence", c)
	}
}

func TestLatencyClassNativeLuaScanAndBoundedProfileCardinality(t *testing.T) {
	b := latencyBatch(1, 1024, "put")
	b.items[0].plan.Streaming = true
	if _, eligible := classifyLatency(b.items); eligible {
		t.Fatal("Native consumer wait entered backend latency evidence")
	}
	b.items[0].plan.Streaming = false
	b.items[0].plan.Singleton = true
	if _, eligible := classifyLatency(b.items); eligible {
		t.Fatal("Lua CPU work entered record latency evidence")
	}
	b.items[0].plan.CleanupRequired = true
	class, eligible := classifyLatency(b.items)
	if !eligible || class.kind != 3 {
		t.Fatal("Scan page execution lost its independent evidence class")
	}
	c := controller{window: 8}
	now := time.Unix(10, 0)
	b.duration = time.Millisecond
	b.latencyEligible = true
	for index := range 10000 {
		b.latency.target = fmt.Sprint(index)
		observeLatency(&c, b, now)
	}
	if len(c.profiles) != maxLatencyProfiles {
		t.Fatal("target churn grew an unbounded controller cache", len(c.profiles))
	}
}
