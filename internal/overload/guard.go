// Package overload sheds new work while Go memory nears the runtime memory limit.
package overload

import (
	"context"
	"math"
	"runtime/metrics"
	"sync"
	"time"
)

// Admission closes at 80% of the Go memory limit and reopens at 70%.
const (
	highWatermark  = 80
	lowWatermark   = 70
	sampleInterval = 100 * time.Millisecond
)

type Target interface {
	SetOverloaded(bool)
}

// Guard samples Go-managed memory and tells its targets when it crosses the
// watermarks. Without a Go memory limit it never sheds.
type Guard struct {
	targets []Target
	samples []metrics.Sample
	mu      sync.Mutex
	state   Snapshot
}

// Snapshot is the last observation. Limit zero means no Go memory limit.
type Snapshot struct {
	Used, Limit uint64
	Overloaded  bool
}

func New(targets []Target) *Guard {
	samples := []metrics.Sample{
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
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

// sample reads the memory the Go runtime holds from the OS, which is what the
// runtime compares with its own limit.
func (g *Guard) sample() {
	g.mu.Lock()
	defer g.mu.Unlock()
	metrics.Read(g.samples)
	used := g.samples[0].Value.Uint64() - g.samples[1].Value.Uint64()
	limit := g.samples[2].Value.Uint64()
	if limit == math.MaxInt64 {
		limit = 0
	}
	g.observeLocked(used, limit)
}

func (g *Guard) observeLocked(used, limit uint64) {
	overloaded := g.state.Overloaded
	switch {
	case limit == 0:
		overloaded = false
	case used >= limit/100*highWatermark:
		overloaded = true
	case used <= limit/100*lowWatermark:
		overloaded = false
	}
	changed := overloaded != g.state.Overloaded
	g.state = Snapshot{Used: used, Limit: limit, Overloaded: overloaded}
	if !changed {
		return
	}
	for _, target := range g.targets {
		target.SetOverloaded(overloaded)
	}
}
