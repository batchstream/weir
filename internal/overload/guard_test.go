package overload

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type guardTarget struct{ latched atomic.Bool }

func (t *guardTarget) SetOverloaded(value bool) { t.latched.Store(value) }

func TestGuardHysteresisAndUnknown(t *testing.T) {
	target := &guardTarget{}
	state := Snapshot{Budget: 1000}
	guard := &Guard{targets: []Target{target}, state: state, processBudget: 1000}
	cg := CgroupSnapshot{State: "v2", Valid: true}
	for _, step := range []struct {
		n                      uint64
		high, low, valid, want bool
	}{
		{100, false, true, true, false},
		{800, false, true, true, true},
		{750, false, true, true, true},
		{700, false, true, true, false},
		{100, true, false, true, true},  // another process in the cgroup
		{100, false, false, true, true}, // intermediate cgroup pressure
		{100, false, true, false, true}, // missing RSS cannot clear the latch
		{100, false, true, true, false},
	} {
		o := observation{bytes: step.n, source: "linux_rss", processValid: step.valid, cgroup: cg, high: step.high, low: step.low}
		guard.sample(o)
		s := guard.Snapshot()
		if s.Latched != step.want || target.latched.Load() != step.want || s.Bytes != step.n || !s.Observed {
			t.Fatal(s, step)
		}
	}
	for _, status := range []string{"unknown"} {
		cg.State = status
		o := observation{bytes: 0, source: "linux_rss", processValid: true, cgroup: cg, low: true}
		guard.sample(o)
		if s := guard.Snapshot(); s.Latched || !s.Unknown {
			t.Fatal(s)
		}
	}
	if watermark(^uint64(0), 80, true) != 14757395258967641292 || watermark(^uint64(0), 70, false) != 12912720851596686130 {
		t.Fatal("overflow")
	}
}

func TestGuardStartupRunCancelAndSnapshots(t *testing.T) {
	target := &guardTarget{}
	targets := []Target{target}
	guard := New(targets, DefaultLimits())
	if s := guard.Snapshot(); !s.Observed || s.Budget == 0 || s.Latched != target.latched.Load() {
		t.Fatal("startup must sample synchronously", s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); guard.Run(ctx) }()
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for range 1000 {
				guard.Snapshot()
			}
		})
	}
	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sampler did not join")
	}
	readers.Wait()
	s := guard.Snapshot()
	time.Sleep(150 * time.Millisecond)
	if guard.Snapshot() != s {
		t.Fatal("sampling after join")
	}
	source := "go_sys_minus_released"
	if runtime.GOOS == "darwin" {
		source = "darwin_phys_footprint"
	}
	if !s.Observed || s.Bytes == 0 || runtime.GOOS != "linux" && (s.Source != source || s.Cgroup.State != "not_applicable") {
		t.Fatal(s)
	}
}
