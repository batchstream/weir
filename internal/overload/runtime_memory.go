package overload

import (
	"math"
	"os"
	"runtime/debug"
)

type runtimeMemoryLimit struct {
	prior, lastApplied int64
	owned              bool
}

// ManageRuntimeMemoryLimit explicitly lets this process owner budget Go memory
// from trusted Guard samples. Ordinary Guard construction never changes the
// process-wide runtime setting. The returned cleanup follows the owner's lifetime.
func (g *Guard) ManageRuntimeMemoryLimit() func() {
	g.mu.Lock()
	defer g.mu.Unlock()
	noop := func() {}
	if _, explicit := os.LookupEnv("GOMEMLIMIT"); explicit || g.runtimeMemory != nil {
		return noop
	}
	current := debug.SetMemoryLimit(-1)
	if current != math.MaxInt64 {
		return noop
	}
	owner := &runtimeMemoryLimit{prior: current, lastApplied: current, owned: true}
	g.runtimeMemory = owner
	limits := g.limits
	if limits.HighWatermark == 0 {
		limits = DefaultLimits()
	}
	g.updateRuntimeMemoryLocked(limits)
	cleanup := func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.runtimeMemory != owner {
			return
		}
		if owner.owned && debug.SetMemoryLimit(-1) == owner.lastApplied {
			debug.SetMemoryLimit(owner.prior)
		}
		g.runtimeMemory = nil
		g.updateRuntimeMemoryLocked(limits)
	}
	return cleanup
}

func (g *Guard) updateRuntimeMemoryLocked(limits Limits) {
	current := debug.SetMemoryLimit(-1)
	owner := g.runtimeMemory
	if owner != nil && owner.owned {
		if current != owner.lastApplied {
			// Another process component now owns the setting. Never take it back.
			owner.owned = false
		} else if target := runtimeMemoryTarget(g.state, limits.LowWatermark); target != 0 && target != current {
			debug.SetMemoryLimit(target)
			owner.lastApplied, current = target, target
		}
	}
	g.state.GoRuntimeLimit = uint64(current)
	g.state.GoRuntimeManaged = owner != nil && owner.owned && owner.lastApplied != math.MaxInt64
}

func runtimeMemoryTarget(snapshot Snapshot, lowWatermark uint64) int64 {
	if !snapshot.Observed || snapshot.Unknown || !snapshot.ProcessValid || snapshot.Budget == 0 || lowWatermark == 0 ||
		snapshot.Cgroup.Valid && snapshot.Cgroup.Finite && snapshot.Cgroup.Limit == 0 {
		return 0
	}
	// Keep Go's managed memory below the recovery watermark, leaving 20% of
	// that allowance for the executable, kernel buffers, mmap and other non-Go use.
	recovery := watermark(snapshot.Budget, lowWatermark, false)
	target := watermark(recovery, 80, false)
	return int64(min(target, uint64(math.MaxInt64)))
}
