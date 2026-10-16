package overload

import (
	"math"
	"os"
	"runtime/debug"
)

const minimumAutomaticRuntimeLimit = 16 << 20

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
	if !snapshot.Observed || snapshot.Unknown || !snapshot.ProcessValid || !snapshot.RecoveryAllowanceKnown || snapshot.RecoveryAllowance == 0 || lowWatermark == 0 ||
		snapshot.Cgroup.Valid && snapshot.Cgroup.Finite && snapshot.Cgroup.Limit == 0 {
		return 0
	}
	// The process shares each cgroup's recovery allowance with other users. Keep
	// 20% of its remaining share for executable, kernel buffers and non-Go memory.
	target := watermark(snapshot.RecoveryAllowance, 80, false)
	// A tiny remaining share cannot fit the runtime's necessary resident memory.
	// Bound futile GC work with a small floor, capped by the ordinary full-budget
	// target. The OS Guard still decides whether actual pressure can recover.
	floor := min(uint64(minimumAutomaticRuntimeLimit), watermark(watermark(snapshot.Budget, lowWatermark, false), 80, false))
	target = max(target, floor)
	return int64(min(target, uint64(math.MaxInt64)))
}
