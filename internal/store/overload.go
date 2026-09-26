package store

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Guard uses RSS on Linux, constrained by cgroup v2 memory.max when available.
// Other platforms use Go Sys-HeapReleased: explicitly a degraded, not RSS, signal.
func Guard(ctx context.Context, stores map[string]*Runtime, budget uint64) {
	budget = effectiveBudget(budget)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	latched := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			n := processBytes()
			if n >= budget*80/100 {
				latched = true
			} else if n <= budget*70/100 {
				latched = false
			}
			for _, r := range stores {
				r.SetOverloaded(latched)
			}
		}
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
func processBytes() uint64 {
	if runtime.GOOS == "linux" {
		raw, err := os.ReadFile("/proc/self/statm")
		if err == nil {
			f := strings.Fields(string(raw))
			if len(f) > 1 {
				n, e := strconv.ParseUint(f[1], 10, 64)
				if e == nil {
					return n * uint64(os.Getpagesize())
				}
			}
		}
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Sys - m.HeapReleased
}
