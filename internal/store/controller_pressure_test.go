package store

import (
	"testing"
	"time"

	"github.com/batchstream/weir/internal/execution"
)

func TestRareQuotaOrGCTailsDoNotRenewFloorHoldOrPreventRecovery(t *testing.T) {
	c := controller{window: 1}
	b := latencyBatch(8, 1024, "read")
	now := time.Unix(10, 0)
	opts := observation{batch: b, feedback: execution.Healthy, maximum: 1, now: now}
	b.duration = time.Millisecond
	for range 4 {
		c.observe(opts)
	}
	b.duration = 90 * time.Millisecond
	for index := range 3 {
		opts.now = now.Add(time.Second + time.Duration(index)*50*time.Millisecond)
		c.observe(opts)
	}
	hold := now.Add(2100 * time.Millisecond)
	if c.latencyHoldUntil != hold || c.window != 1 || c.epoch != 0 || !c.cooldown.IsZero() {
		t.Fatal("confirmed floor pressure failed its bounded hold or paused successful work", c)
	}
	opts.maximum = 4
	for index := range 2000 {
		b.epoch = c.epoch
		b.duration = time.Millisecond
		if index%100 == 99 {
			b.duration = 90 * time.Millisecond
		}
		opts.now = now.Add(1101*time.Millisecond + time.Duration(index)*time.Millisecond)
		reason := c.observe(opts)
		if reason != "" || c.latencyHoldUntil != hold {
			t.Fatal("one-percent tails renewed confirmed pressure or reduced recovered capacity", index, c, reason)
		}
	}
	if c.window != 4 {
		t.Fatal("rare slow successes kept recovered capacity at its floor", c)
	}
}

func TestEveryLearnableHealthySampleDilutesShortSaturatedTailBursts(t *testing.T) {
	for _, lowDemand := range []bool{false, true} {
		name := "concurrent"
		if lowDemand {
			name = "low-demand"
		}
		t.Run(name, func(t *testing.T) {
			c := controller{window: 8}
			b := latencyBatch(8, 1024, "read")
			now := time.Unix(10, 0)
			b.duration = time.Millisecond
			for range 4 {
				observeLatency(&c, b, now)
			}
			b.saturated, b.lowDemand = false, lowDemand
			for index := range 28 {
				observeLatency(&c, b, now.Add(time.Second+time.Duration(index)*time.Millisecond))
			}
			if c.profiles[b.latency].healthy != 28 {
				t.Fatal("normal successful work was excluded from the bounded denominator", c.profiles[b.latency])
			}
			b.saturated, b.lowDemand = true, false
			b.duration = 90 * time.Millisecond
			for index := range 3 {
				reason := observeLatency(&c, b, now.Add(1100*time.Millisecond+time.Duration(index)*50*time.Millisecond))
				if reason != "" {
					t.Fatal("less than ten-percent slow work became confirmed pressure", reason)
				}
			}
			profile := c.profiles[b.latency]
			if c.window != 8 || !c.latencyHoldUntil.IsZero() || profile.slow != 3 || profile.healthy != 31 {
				t.Fatal("short saturated tails forgot previous healthy completions", c, profile)
			}
			reason := observeLatency(&c, b, now.Add(1250*time.Millisecond))
			if reason != "latency" || c.window != 7 || c.latencyHoldUntil != now.Add(2250*time.Millisecond) {
				t.Fatal("pressure exceeding the slow fraction failed confirmation", c, reason)
			}
		})
	}
}

func TestSparseSustainedSlowWorkConfirmsWithoutMinimumThroughput(t *testing.T) {
	c := controller{window: 8}
	b := latencyBatch(8, 1024, "read")
	now := time.Unix(10, 0)
	b.duration = time.Millisecond
	for range 4 {
		observeLatency(&c, b, now)
	}
	b.duration = 200 * time.Millisecond
	for index := range 3 {
		reason := observeLatency(&c, b, now.Add(time.Second+time.Duration(index)*400*time.Millisecond))
		if index < 2 && (reason != "" || !c.latencyHoldUntil.IsZero()) {
			t.Fatal("sparse pressure was confirmed before three bounded samples", c, reason)
		}
		if index == 2 && (reason != "latency" || c.window != 7 || c.latencyHoldUntil != now.Add(2800*time.Millisecond)) {
			t.Fatal("sparse sustained pressure depended on a high request rate", c, reason)
		}
	}
}

func TestCanceledHealthyCompletionsCannotDiluteRealPressureEvidence(t *testing.T) {
	c := controller{window: 8}
	b := latencyBatch(8, 1024, "read")
	now := time.Unix(10, 0)
	b.duration = time.Millisecond
	for range 4 {
		observeLatency(&c, b, now)
	}
	b.duration = 90 * time.Millisecond
	for index := range 2 {
		observeLatency(&c, b, now.Add(time.Second+time.Duration(index)*50*time.Millisecond))
	}
	b.recoveryEligible = false
	b.duration = time.Millisecond
	for index := range 100 {
		observeLatency(&c, b, now.Add(1051*time.Millisecond+time.Duration(index)*time.Millisecond))
	}
	profile := c.profiles[b.latency]
	if profile.healthy != 2 || profile.slow != 2 || c.credit != 0 || c.baseline != time.Millisecond {
		t.Fatal("canceled callers diluted successful-backend pressure or contributed recovery", c, profile)
	}
	b.recoveryEligible = true
	b.duration = 90 * time.Millisecond
	reason := observeLatency(&c, b, now.Add(1200*time.Millisecond))
	if reason != "latency" || c.window != 7 {
		t.Fatal("caller cancellations prevented independent backend pressure confirmation", c, reason)
	}
}
