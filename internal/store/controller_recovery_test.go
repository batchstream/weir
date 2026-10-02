package store

import (
	"testing"
	"time"

	"github.com/batchstream/weir/internal/execution"
)

func TestFastLowDemandOutlierDoesNotPreventRecoveryUnderContinuedDemand(t *testing.T) {
	c := controller{window: 4}
	b := latencyBatch(8, 1024, "read")
	now := time.Unix(10, 0)
	opts := observation{batch: b, feedback: execution.Healthy, maximum: 4, now: now}
	b.duration = 3 * time.Millisecond
	for range 4 {
		c.observe(opts)
	}
	b.lowDemand, b.saturated = true, false
	b.duration = 100 * time.Microsecond
	opts.now = now.Add(100 * time.Millisecond)
	c.observe(opts)
	if c.baseline <= 2500*time.Microsecond || c.baseline >= 3*time.Millisecond {
		t.Fatal("one idle fast response pinned the baseline instead of smoothing it", c)
	}

	b.lowDemand, b.saturated = false, true
	b.duration = 30 * time.Millisecond
	baseline := c.baseline
	for stage := range 3 {
		for index := range 3 {
			b.epoch = c.epoch
			opts.now = now.Add(time.Duration(stage+1)*time.Second + time.Duration(index)*50*time.Millisecond)
			reason := c.observe(opts)
			if index == 2 && reason != "latency" {
				t.Fatal("sustained queued successful pressure did not reduce the window", stage, c, reason)
			}
		}
	}
	if c.window != 1 || c.baseline != baseline {
		t.Fatal("real pressure was learned as normal service cost", c)
	}
	hold := c.latencyHoldUntil
	b.duration = 3 * time.Millisecond
	for index := range 60 {
		b.epoch = c.epoch
		opts.now = now.Add(3110*time.Millisecond + time.Duration(index)*10*time.Millisecond)
		if reason := c.observe(opts); reason != "" {
			t.Fatal("recovered service cost was misclassified as continued pressure", c, reason)
		}
		if c.window != 1 || c.latencyHoldUntil != hold {
			t.Fatal("healthy recovery bypassed the confirmed pressure hold", c)
		}
	}
	for index := range 100 {
		b.epoch = c.epoch
		opts.now = hold.Add(time.Millisecond + time.Duration(index)*10*time.Millisecond)
		if reason := c.observe(opts); reason != "" {
			t.Fatal("continued demand renewed stale pressure after recovery", c, reason)
		}
	}
	if c.window != 4 || c.latencyHoldUntil != hold || c.baseline < 2990*time.Microsecond {
		t.Fatal("healthy queued work failed to recover bounded concurrency", c)
	}
}

func TestPreviousEpochHealthyCompletionsLearnWithoutActuating(t *testing.T) {
	c := controller{window: 2}
	b := latencyBatch(8, 1024, "read")
	now := time.Unix(10, 0)
	opts := observation{batch: b, feedback: execution.Healthy, maximum: 2, now: now}
	b.duration = 3 * time.Millisecond
	for range 4 {
		c.observe(opts)
	}
	opts.feedback, opts.now = execution.Congested, now.Add(time.Second)
	c.observe(opts)
	if c.window != 1 || c.epoch != 1 {
		t.Fatal("explicit rejection failed to cut the current window", c)
	}
	cooldown, epoch := c.cooldown, c.epoch
	opts.feedback = execution.Healthy
	b.duration = 2500 * time.Microsecond
	for index := range 28 {
		opts.now = now.Add(1001*time.Millisecond + time.Duration(index)*time.Millisecond)
		if reason := c.observe(opts); reason != "" {
			t.Fatal("previous flight acted on the new concurrency window", c, reason)
		}
	}
	profile := c.profiles[b.latency]
	if profile.healthy != 28 || profile.baseline >= 2600*time.Microsecond || c.window != 1 ||
		c.epoch != epoch || c.cooldown != cooldown || c.credit != 0 || !c.latencyHoldUntil.IsZero() {
		t.Fatal("previous successful flight lost valid learning or obtained recovery credit", c, profile)
	}
	// Old slow completions still count as successful work, but their old
	// concurrency cannot confirm pressure at the newly reduced window.
	b.duration = 30 * time.Millisecond
	for index := range 3 {
		opts.now = now.Add(1030*time.Millisecond + time.Duration(index)*50*time.Millisecond)
		c.observe(opts)
	}
	if profile.healthy != 31 || profile.slow != 0 || c.epoch != epoch || !c.latencyHoldUntil.IsZero() {
		t.Fatal("previous flight established pressure after the concurrency change", c, profile)
	}
	for _, feedback := range []execution.Feedback{execution.Congested, execution.Completed, execution.Neutral} {
		opts.feedback = feedback
		c.observe(opts)
		if c.epoch != epoch || c.cooldown != cooldown || profile.healthy != 31 || profile.slow != 0 {
			t.Fatal("previous non-healthy flight changed the current controller", feedback, c, profile)
		}
	}
	b.epoch = c.epoch
	opts.feedback = execution.Healthy
	for index := range 3 {
		opts.now = now.Add(1200*time.Millisecond + time.Duration(index)*50*time.Millisecond)
		c.observe(opts)
	}
	if profile.slow != 3 || profile.healthy != 34 || !c.latencyHoldUntil.IsZero() {
		t.Fatal("late normal work was excluded from the current slow fraction", c, profile)
	}
	opts.now = now.Add(1350 * time.Millisecond)
	c.observe(opts)
	if c.latencyHoldUntil != now.Add(2350*time.Millisecond) || c.epoch != epoch || c.cooldown != cooldown {
		t.Fatal("new sustained floor pressure failed its confirmed bounded hold", c)
	}
}

func TestCanceledCompletionsDoNotResetOrDilutePressureAcrossEpochs(t *testing.T) {
	c := controller{window: 8}
	b := latencyBatch(8, 1024, "read")
	now := time.Unix(10, 0)
	b.duration = time.Millisecond
	for range 4 {
		observeLatency(&c, b, now)
	}
	b.duration = 30 * time.Millisecond
	for index := range 2 {
		observeLatency(&c, b, now.Add(time.Second+time.Duration(index)*50*time.Millisecond))
	}
	profile := c.profiles[b.latency]
	canceled := *b
	canceled.recoveryEligible, canceled.latencyEligible = false, false
	canceled.duration = 100 * time.Microsecond
	opts := observation{batch: &canceled, feedback: execution.Neutral, maximum: 8, now: now.Add(1051 * time.Millisecond)}
	for _, feedback := range []execution.Feedback{execution.Neutral, execution.Healthy} {
		opts.feedback = feedback
		for range 20 {
			c.observe(opts)
		}
	}
	if profile.healthy != 2 || profile.slow != 2 || c.baseline != time.Millisecond || c.credit != 0 {
		t.Fatal("caller cancellation changed independent backend pressure evidence", c, profile)
	}
	reason := observeLatency(&c, b, now.Add(1100*time.Millisecond))
	if reason != "latency" || c.window != 7 {
		t.Fatal("caller cancellation erased confirmation of real pressure", c, reason)
	}
	baseline, sample, epoch := c.baseline, c.sample, c.epoch
	opts.feedback, opts.now = execution.Healthy, now.Add(1101*time.Millisecond)
	canceled.epoch = epoch - 1
	for range 20 {
		c.observe(opts)
	}
	if profile.healthy != 0 || profile.slow != 0 || c.baseline != baseline || c.sample != sample || c.epoch != epoch {
		t.Fatal("previous canceled flight trained after a concurrency adjustment", c, profile)
	}
}

func TestEligibleBusinessFailureStillClearsCurrentPressureEvidence(t *testing.T) {
	c := controller{window: 8}
	b := latencyBatch(8, 1024, "read")
	now := time.Unix(10, 0)
	b.duration = time.Millisecond
	for range 4 {
		observeLatency(&c, b, now)
	}
	b.duration = 30 * time.Millisecond
	for index := range 2 {
		observeLatency(&c, b, now.Add(time.Second+time.Duration(index)*50*time.Millisecond))
	}
	opts := observation{batch: b, feedback: execution.Neutral, maximum: 8, now: now.Add(1051 * time.Millisecond)}
	c.observe(opts)
	profile := c.profiles[b.latency]
	if profile.healthy != 0 || profile.slow != 0 || !profile.evidenceSince.IsZero() || !profile.slowSince.IsZero() ||
		c.baseline != time.Millisecond || c.credit != 0 || !c.latencyHoldUntil.IsZero() {
		t.Fatal("eligible business failure retained unreliable pressure evidence or trained latency", c, profile)
	}
	reason := observeLatency(&c, b, now.Add(1100*time.Millisecond))
	if reason != "" || c.window != 8 || profile.slow != 1 || profile.healthy != 1 {
		t.Fatal("a business failure no longer separated distinct successful-pressure bursts", c, profile, reason)
	}
}
