package store

import (
	"testing"
	"time"

	"github.com/batchstream/weir/internal/execution"
)

func TestQuotaBurstsKeepSlowEvidenceAndHoldRecoveryUntilCapacityReturns(t *testing.T) {
	c := controller{window: 8}
	b := latencyBatch(8, 1024, "read")
	now := time.Unix(10, 0)
	b.duration = time.Millisecond
	for range 4 {
		observeLatency(&c, b, now)
	}
	for index := range 30 {
		at := now.Add(time.Second + time.Duration(index)*100*time.Millisecond)
		b.duration = 90 * time.Millisecond
		observeLatency(&c, b, at)
		window := c.window
		b.duration = time.Millisecond
		for fast := range 4 {
			observeLatency(&c, b, at.Add(time.Duration(fast+1)*time.Millisecond))
		}
		if c.window != window || (at.Before(c.latencyHoldUntil) && c.credit != 0) {
			t.Fatal("quota burst raised capacity while saturated slow work continued", c)
		}
	}
	if c.window != 1 {
		t.Fatal("alternating fast and slow successes failed to reduce bounded capacity", c)
	}
	for range 20 {
		observeLatency(&c, b, c.latencyHoldUntil.Add(-time.Nanosecond))
	}
	if c.window != 1 {
		t.Fatal("fast successes bypassed recent latency pressure", c)
	}
	for index := range 100 {
		b.epoch = c.epoch
		opts := observation{batch: b, feedback: execution.Healthy, maximum: 8, now: c.latencyHoldUntil.Add(time.Duration(index) * 100 * time.Millisecond)}
		c.observe(opts)
	}
	if c.window != 8 {
		t.Fatal("sustained recovered capacity failed to reach the configured cap", c)
	}
}

func TestSlowEvidenceUsesOneSecondBoundInsteadOfAccumulatingIsolatedOutliers(t *testing.T) {
	c := controller{window: 8}
	b := latencyBatch(8, 1024, "read")
	now := time.Unix(10, 0)
	b.duration = time.Millisecond
	for range 4 {
		observeLatency(&c, b, now)
	}
	for index := range 10 {
		b.duration = 90 * time.Millisecond
		observeLatency(&c, b, now.Add(time.Second+time.Duration(index)*600*time.Millisecond))
		b.duration = time.Millisecond
		observeLatency(&c, b, now.Add(time.Second+time.Duration(index)*600*time.Millisecond+time.Millisecond))
	}
	if c.window != 8 {
		t.Fatal("outliers outside one bounded evidence window combined to reduce capacity", c)
	}
}

func TestOtherTargetSharesOnlyConfirmedPressureWithoutSupplyingLatencyEvidence(t *testing.T) {
	c := controller{window: 2}
	first := latencyBatch(8, 1024, "read")
	second := latencyBatch(8, 1024, "read")
	second.latency.target = "other-target"
	first.duration, second.duration = time.Millisecond, 50*time.Millisecond
	now := time.Unix(10, 0)
	for range 4 {
		observeLatency(&c, first, now)
		observeLatency(&c, second, now)
	}
	c.window, c.credit = 2, 0
	first.duration = 90 * time.Millisecond
	observeLatency(&c, first, now.Add(time.Second))
	observeLatency(&c, first, now.Add(time.Second+50*time.Millisecond))
	for index := range 20 {
		observeLatency(&c, second, now.Add(time.Second+100*time.Millisecond+time.Duration(index)*time.Millisecond))
	}
	if c.window != 3 || !c.latencyHoldUntil.IsZero() || c.profiles[first.latency].slow != 2 || c.profiles[second.latency].slow != 0 {
		t.Fatal("unconfirmed pressure held another target or mixed its independent latency evidence", c)
	}
	observeLatency(&c, first, now.Add(1200*time.Millisecond))
	hold := c.latencyHoldUntil
	for index := range 20 {
		observeLatency(&c, second, now.Add(1201*time.Millisecond+time.Duration(index)*time.Millisecond))
	}
	if c.window != 2 || c.credit != 0 || hold != now.Add(2200*time.Millisecond) || c.latencyHoldUntil != hold || c.profiles[second.latency].slow != 0 {
		t.Fatal("confirmed pressure failed to block shared recovery or healthy unrelated work renewed the hold", c)
	}
	for index := range 4 {
		observeLatency(&c, second, now.Add(2300*time.Millisecond+time.Duration(index)*time.Millisecond))
	}
	if c.window != 3 {
		t.Fatal("other healthy target did not recover after the shared capacity hold expired", c)
	}
}

func TestCompletedProbeRespectsRecordLatencyHoldWithoutLearningItsOwnDuration(t *testing.T) {
	c := controller{window: 2}
	b := latencyBatch(8, 1024, "read")
	now := time.Unix(10, 0)
	b.duration = time.Millisecond
	for range 4 {
		observeLatency(&c, b, now)
	}
	c.window, c.credit = 2, 0
	b.duration = 90 * time.Millisecond
	for index := range 3 {
		observeLatency(&c, b, now.Add(time.Second+time.Duration(index)*50*time.Millisecond))
	}
	native := nativeProbeBatch()
	native.epoch = c.epoch
	native.duration = time.Minute
	opts := observation{batch: native, feedback: execution.Completed, maximum: 4, now: now.Add(1500 * time.Millisecond)}
	for range 20 {
		c.observe(opts)
	}
	if c.window != 1 || c.credit != 0 || len(c.profiles) != 1 || c.sample != b.duration {
		t.Fatal("opaque completed transport bypassed record pressure or learned consumer latency", c)
	}
	opts.now = now.Add(2200 * time.Millisecond)
	c.observe(opts)
	if c.window != 2 {
		t.Fatal("opaque completed transport did not resume bounded probes after pressure ended", c)
	}
}
