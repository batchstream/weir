// Package overload samples process memory independently of database runtimes.
package overload

import (
	"context"
	"runtime"
	"sync"
	"time"
)

type Target interface {
	SetOverloaded(bool)
}

// Guard owns the existing memory sampler and exposes its last observation.
// There is no second diagnostics sampling loop.
type Guard struct {
	mu      sync.Mutex
	targets []Target
	state   Snapshot
	profile memoryProfile
}

type Snapshot struct {
	Budget, Bytes     uint64
	Source            string
	Observed, Latched bool
	ProcessValid      bool
	Unknown           bool
	Cgroup            CgroupSnapshot
}

// Cgroup is the most pressured finite visible level, with its own current/max
// pair (never leaf usage divided by an ancestor limit). With no finite level it
// reports the leaf current and Finite=false. Paths never leave the sampler.
type CgroupSnapshot struct {
	Current, Limit uint64
	Levels         int
	Finite, Valid  bool
	State          string // not_applicable, v2, unknown, profile_changed
	Scope          string // none, leaf, ancestor
}

type observation struct {
	bytes        uint64
	source       string
	processValid bool
	cgroup       CgroupSnapshot
	high, low    bool
}

func New(targets []Target, budget uint64) *Guard {
	state := Snapshot{Budget: budget}
	guard := &Guard{targets: targets, state: state, profile: newMemoryProfile()}
	// Publish the first observation before listeners can admit any work.
	guard.sample(guard.profile.observe())
	return guard
}

func (g *Guard) Snapshot() Snapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state
}

func (g *Guard) Run(ctx context.Context) {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			g.sample(g.profile.observe())
		}
	}
}

func (g *Guard) sample(o observation) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.state.Bytes, g.state.Source, g.state.Observed = o.bytes, o.source, true
	g.state.ProcessValid, g.state.Cgroup = o.processValid, o.cgroup
	g.state.Unknown = !o.processValid || o.cgroup.State == "unknown" || o.cgroup.State == "profile_changed"
	if g.state.Unknown || g.state.Budget == 0 || o.high || o.bytes >= watermark(g.state.Budget, 80, true) {
		g.state.Latched = true
	} else if o.low && o.bytes <= watermark(g.state.Budget, 70, false) {
		g.state.Latched = false
	}
	for _, target := range g.targets {
		target.SetOverloaded(g.state.Latched)
	}
}

// Split before multiplying, including for uint64-sized budgets.
func watermark(n, percent uint64, ceil bool) uint64 {
	remainder := n % 100 * percent
	if ceil {
		remainder += 99
	}
	return n/100*percent + remainder/100
}

func goBytes() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Sys - m.HeapReleased
}
