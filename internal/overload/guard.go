// Package overload samples process memory independently of database runtimes.
package overload

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

type Limits struct {
	HighWatermark, LowWatermark uint64
	SampleInterval              time.Duration
}

func DefaultLimits() Limits {
	limits := Limits{HighWatermark: 80, LowWatermark: 70, SampleInterval: 100 * time.Millisecond}
	return limits
}

func (limits Limits) Validate() error {
	if limits.HighWatermark > 100 || limits.HighWatermark < 1 || limits.LowWatermark >= limits.HighWatermark || limits.SampleInterval <= 0 {
		return fmt.Errorf("invalid memory pressure watermarks or sampling interval")
	}
	return nil
}

type Target interface {
	SetOverloaded(bool)
}

// Guard owns the existing memory sampler and exposes its last observation.
// There is no second diagnostics sampling loop.
type Guard struct {
	limits        Limits
	mu            sync.Mutex
	targets       []Target
	state         Snapshot
	profile       memoryProfile
	processBudget uint64
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
	Capacity       uint64
	Levels         int
	Finite, Valid  bool
	State          string // not_applicable, v2, unknown
	Scope          string // none, leaf, ancestor
}

type observation struct {
	bytes        uint64
	source       string
	processValid bool
	cgroup       CgroupSnapshot
	high, low    bool
}

func New(targets []Target, limits Limits) *Guard {
	budget := processMemoryBudget()
	state := Snapshot{Budget: budget}
	guard := &Guard{limits: limits, targets: targets, state: state, profile: newMemoryProfile(), processBudget: budget}
	// Publish the first observation before listeners can admit any work.
	guard.sample(guard.profile.observe(limits))
	return guard
}

func (g *Guard) Snapshot() Snapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state
}

func (g *Guard) Run(ctx context.Context) {
	tick := time.NewTicker(g.limits.SampleInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			g.sample(g.profile.observe(g.limits))
		}
	}
}

func (g *Guard) sample(o observation) {
	g.mu.Lock()
	defer g.mu.Unlock()
	limits := g.limits
	if limits.HighWatermark == 0 {
		limits = DefaultLimits()
	}
	g.state.Bytes, g.state.Source, g.state.Observed = o.bytes, o.source, true
	g.state.ProcessValid, g.state.Cgroup = o.processValid, o.cgroup
	g.state.Unknown = !o.processValid || o.cgroup.State == "unknown"
	g.state.Budget = g.processBudget
	if o.cgroup.Valid && o.cgroup.Finite {
		capacity := o.cgroup.Capacity
		if capacity == 0 {
			capacity = o.cgroup.Limit
		}
		g.state.Budget = smallerBudget(g.state.Budget, capacity)
	}
	// Missing observations do not manufacture overload. Actual process or
	// container pressure controls admission once a trustworthy sample exists.
	if o.high || o.processValid && g.processBudget != 0 && o.bytes >= watermark(g.processBudget, limits.HighWatermark, true) {
		g.state.Latched = true
	} else if !g.state.Unknown && o.processValid && !o.high && (!o.cgroup.Valid || o.low) && (g.processBudget == 0 || o.bytes <= watermark(g.processBudget, limits.LowWatermark, false)) {
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

// Zero means no known limit, not a zero-byte resource allowance.
func smallerBudget(left, right uint64) uint64 {
	if left == 0 {
		return right
	}
	if right == 0 {
		return left
	}
	return min(left, right)
}
