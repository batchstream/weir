package store

import (
	"math/bits"
	"math/rand/v2"
	"time"

	"github.com/batchstream/weir/internal/execution"
)

const (
	maxLatencyProfiles    = 64
	latencyEvidenceWindow = time.Second
)

// Compare similarly sized work on the same target. In particular, a larger
// batch must not look like congestion merely because it contains more records.
type latencyClass struct {
	target     string
	kind       uint8
	operations uint8
	bytes      uint8
}

type latencyProfile struct {
	baseline      time.Duration
	samples       int
	healthy       int
	slow          int
	evidenceSince time.Time
	slowSince     time.Time
	used          uint64
}

type controller struct {
	window, credit       int
	epoch                uint64
	cooldown, lastGrowth time.Time
	latencyHoldUntil     time.Time
	profiles             map[latencyClass]*latencyProfile
	observations         uint64
	baseline, sample     time.Duration
}

type observation struct {
	batch    *batch
	feedback execution.Feedback
	maximum  int
	now      time.Time
}

func classifyLatency(items []*Ticket) (latencyClass, bool) {
	class := latencyClass{}
	if len(items) == 0 {
		return class, false
	}
	class.operations = uint8(bits.Len(uint(len(items) - 1)))
	class.target = items[0].plan.BatchKey
	reads, mutations, bytes := 0, 0, 0
	for _, item := range items {
		plan := item.plan
		if plan.Streaming {
			// Native includes time waiting for the consumer to accept output.
			return class, false
		}
		if plan.CleanupRequired {
			// Scan pages are fetched before the separate bounded publisher runs.
			class.kind = 3
			class.target = plan.Key
		} else if plan.Singleton || plan.Operation == nil {
			// Lua and opaque native operations do not share a record cost model.
			return class, false
		} else if plan.Operation.GetRead() != nil {
			reads++
		} else {
			mutations++
		}
		bytes += plan.Bytes
	}
	if class.kind != 3 {
		switch {
		case reads == 0:
			class.kind = 1
		case mutations != 0:
			class.kind = 2
		}
	}
	average := (bytes + len(items) - 1) / len(items)
	class.bytes = uint8(bits.Len(uint(average)))
	return class, true
}

func (c *controller) profile(class latencyClass) *latencyProfile {
	if c.profiles == nil {
		c.profiles = make(map[latencyClass]*latencyProfile)
	}
	c.observations++
	profile := c.profiles[class]
	if profile == nil {
		if len(c.profiles) == maxLatencyProfiles {
			var oldest latencyClass
			used := ^uint64(0)
			for key, candidate := range c.profiles {
				if candidate.used < used {
					oldest, used = key, candidate.used
				}
			}
			delete(c.profiles, oldest)
		}
		profile = &latencyProfile{}
		c.profiles[class] = profile
	}
	profile.used = c.observations
	return profile
}

// Explicit capacity failures reduce concurrency immediately. Successful calls
// may also expose pressure before the database rejects work: three slow samples
// over at least 100ms and at least 10% of comparable successful work are required,
// with 2ms slack for scheduling noise. A caller cancellation or business error
// never participates in the latency model.
func (c *controller) observe(opts observation) string {
	b := opts.batch
	currentEpoch := b.epoch == c.epoch
	if !currentEpoch && opts.feedback != execution.Healthy {
		return ""
	}
	if opts.feedback == execution.Congested {
		c.reduce(opts.now, true)
		return "backend"
	}
	if opts.feedback == execution.Completed {
		// Opaque Native replies establish transport completion, not business
		// health. Probe at most one extra bounded slot per second, without
		// learning streaming duration or mixing with healthy-record credits.
		c.credit = 0
		if b.recoveryEligible && b.saturated && !opts.now.Before(c.cooldown) && !opts.now.Before(c.latencyHoldUntil) &&
			opts.now.Sub(c.lastGrowth) >= time.Second && c.window < opts.maximum {
			c.window++
			c.epoch++
			c.lastGrowth = opts.now
		}
		return ""
	}
	if opts.feedback != execution.Healthy {
		c.credit = 0
		if b.recoveryEligible {
			if profile := c.profiles[b.latency]; profile != nil {
				profile.healthy, profile.slow = 0, 0
				profile.evidenceSince, profile.slowSince = time.Time{}, time.Time{}
			}
		}
		return ""
	}
	if !b.recoveryEligible {
		if currentEpoch {
			c.credit = 0
		}
		return ""
	}
	if b.latencyEligible && b.duration > 0 {
		profile := c.profile(b.latency)
		if profile.evidenceSince.IsZero() || opts.now.Sub(profile.evidenceSince) >= latencyEvidenceWindow {
			profile.healthy, profile.slow = 0, 0
			profile.evidenceSince, profile.slowSince = opts.now, time.Time{}
		}
		profile.healthy++
		// Scale to the operation-count bucket's upper bound. For a fixed plus
		// per-record cost, normal size variation inside a bucket stays within
		// the 2x allowance while frequent 9..16-record batches share evidence.
		duration := b.duration * time.Duration(1<<b.latency.operations) / time.Duration(max(1, len(b.items)))
		c.sample = duration
		slow := profile.samples >= 4 && duration > 2*profile.baseline+2*time.Millisecond
		if profile.samples < 4 {
			profile.samples++
			profile.baseline += (duration - profile.baseline) / time.Duration(profile.samples)
		} else if !slow || b.lowDemand {
			// Follow ordinary service cost in both directions. One unusually fast
			// completion must not pin a historical minimum, and successful queued
			// slow work must not train sustained pressure into the baseline.
			profile.baseline += (duration - profile.baseline) / 8
		}
		c.baseline = profile.baseline
		if !currentEpoch {
			// A previous flight still supplies valid successful-work statistics.
			// Its old concurrency cannot establish pressure after an adjustment
			// or act on the new window, so only current work supplies the numerator.
			return ""
		}
		if slow {
			c.credit = 0
			if !b.saturated {
				// Earlier members of the same concurrent flight may not have
				// filled the window yet. Their slow completion must not erase
				// pressure evidence from the member that did fill it.
				return ""
			}
			if profile.slow == 0 {
				profile.slowSince = opts.now
			}
			profile.slow++
			if profile.slow >= 3 && profile.slow*10 >= profile.healthy && opts.now.Sub(profile.slowSince) >= 100*time.Millisecond {
				// Retain fast quota bursts in the denominator without erasing the
				// bounded slow evidence. Only confirmed pressure blocks shared
				// recovery; rare GC/CFS tails must not keep renewing a floor hold.
				c.latencyHoldUntil = opts.now.Add(latencyEvidenceWindow)
				profile.healthy, profile.slow = 0, 0
				profile.evidenceSince, profile.slowSince = time.Time{}, time.Time{}
				if c.window > 1 {
					c.reduce(opts.now, false)
					return "latency"
				}
				// Successful slow work at the floor stays single-file and bounded;
				// only an explicit capacity rejection pauses dispatch further.
			}
			return ""
		}
		if b.lowDemand {
			profile.slow, profile.slowSince = 0, time.Time{}
		}
	}
	if !currentEpoch {
		return ""
	}
	if !b.saturated || opts.now.Before(c.cooldown) || opts.now.Before(c.latencyHoldUntil) {
		c.credit = 0
		return ""
	}
	c.credit = min(c.credit+1, max(4, c.window))
	if c.credit >= max(4, c.window) && opts.now.Sub(c.lastGrowth) >= 250*time.Millisecond && c.window < opts.maximum {
		c.window++
		c.epoch++
		c.credit = 0
		c.lastGrowth = opts.now
	}
	return ""
}

func (c *controller) reduce(now time.Time, rejected bool) {
	if rejected {
		c.window = max(1, c.window/2)
	} else {
		// A successful slow response is weaker evidence than a rejection.
		c.window = max(1, c.window-1)
	}
	c.epoch++
	c.credit = 0
	c.lastGrowth = now
	c.cooldown = now.Add(time.Duration(100+rand.IntN(201)) * time.Millisecond)
}
