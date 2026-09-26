// Package overload samples process memory independently of database runtimes.
package overload

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
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
}
type Snapshot struct {
	Budget, Bytes     uint64
	Source            string
	Observed, Latched bool
}

func New(targets []Target, budget uint64) *Guard {
	state := Snapshot{Budget: effectiveBudget(budget), Source: "unobserved"}
	guard := &Guard{targets: targets, state: state}
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
			n, source := processBytes()
			g.sample(n, source)
		}
	}
}
func (g *Guard) sample(n uint64, source string) {
	g.mu.Lock()
	g.state.Bytes, g.state.Source, g.state.Observed = n, source, true
	if n >= g.state.Budget*80/100 {
		g.state.Latched = true
	} else if n <= g.state.Budget*70/100 {
		g.state.Latched = false
	}
	latched := g.state.Latched
	g.mu.Unlock()
	for _, target := range g.targets {
		target.SetOverloaded(latched)
	}
}
func effectiveBudget(budget uint64) uint64 {
	if runtime.GOOS == "linux" {
		raw, err := os.ReadFile("/sys/fs/cgroup/memory.max")
		if err == nil {
			n, e := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
			if e == nil && n > 0 && n < budget {
				budget = n
			}
		}
	}
	return budget
}
func processBytes() (uint64, string) {
	if runtime.GOOS == "linux" {
		raw, err := os.ReadFile("/proc/self/statm")
		if err == nil {
			f := strings.Fields(string(raw))
			if len(f) > 1 {
				n, e := strconv.ParseUint(f[1], 10, 64)
				if e == nil {
					return n * uint64(os.Getpagesize()), "linux_rss"
				}
			}
		}
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Sys - m.HeapReleased, "go_sys_minus_released"
}
