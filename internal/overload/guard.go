// Package overload sheds new work while live memory nears the Go memory limit.
package overload

import (
	"context"
	"math"
	"runtime"
	"runtime/metrics"
	"sync"
	"time"
)

// Admission closes at 80% of the Go memory limit and reopens at or below 70%.
const (
	highWatermark  = 80
	lowWatermark   = 70
	sampleInterval = 100 * time.Millisecond
	// collectInterval paces the collections that refresh the live heap while
	// work is refused.
	collectInterval = time.Second
)

type Target interface {
	SetOverloaded(bool)
}

// Guard samples live memory and tells its targets when it crosses the
// watermarks. Without a Go memory limit it never sheds.
type Guard struct {
	targets   []Target
	samples   []metrics.Sample
	collected time.Time
	mu        sync.Mutex
	state     Snapshot
}

// Snapshot is the last observation. Limit zero means no Go memory limit.
type Snapshot struct {
	Live, Limit uint64
	Overloaded  bool
}

func New(targets []Target) *Guard {
	samples := []metrics.Sample{
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/memory/classes/heap/free:bytes"},
		{Name: "/memory/classes/heap/unused:bytes"},
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/gc/heap/live:bytes"},
		{Name: "/gc/gomemlimit:bytes"},
	}
	guard := &Guard{targets: targets, samples: samples}
	// Publish the first observation before listeners can admit any work.
	guard.sample()
	return guard
}

func (g *Guard) Snapshot() Snapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state
}

// Run samples until ctx ends. It is the only caller of sample after New.
func (g *Guard) Run(ctx context.Context) {
	tick := time.NewTicker(sampleInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			g.sample()
		}
	}
}

// sample measures the memory the collector cannot reclaim: the live heap at the
// last collection plus stacks and runtime metadata. Free heap, unused span space
// and garbage do not count, since the collector reuses them before the limit.
func (g *Guard) sample() {
	// The live heap only changes at a collection. While refusing work the
	// allocation rate drops, so collect to notice finished work promptly.
	if g.Snapshot().Overloaded && time.Since(g.collected) >= collectInterval {
		runtime.GC()
		g.collected = time.Now()
	}
	metrics.Read(g.samples)
	total := g.samples[0].Value.Uint64()
	reclaimable := g.samples[1].Value.Uint64() + g.samples[2].Value.Uint64() + g.samples[3].Value.Uint64() + g.samples[4].Value.Uint64()
	live := g.samples[5].Value.Uint64() + total - min(total, reclaimable)
	limit := g.samples[6].Value.Uint64()
	if limit == math.MaxInt64 {
		limit = 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.observeLocked(live, limit)
}

func (g *Guard) observeLocked(live, limit uint64) {
	overloaded := g.state.Overloaded
	switch {
	case limit == 0:
		overloaded = false
	case live >= limit/100*highWatermark:
		overloaded = true
	case live <= limit/100*lowWatermark:
		overloaded = false
	}
	changed := overloaded != g.state.Overloaded
	g.state = Snapshot{Live: live, Limit: limit, Overloaded: overloaded}
	if !changed {
		return
	}
	for _, target := range g.targets {
		target.SetOverloaded(overloaded)
	}
}
