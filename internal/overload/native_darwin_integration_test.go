//go:build integration && darwin

package overload

import (
	"context"
	"os"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

// darwinMemoryBudget bounds the fixture's extra allocations while exercising
// the real phys_footprint sampler.
const darwinMemoryBudget = 2 << 30

func TestDarwinMemoryNative(t *testing.T) {
	if os.Getenv("WEIR_MEMORY_DARWIN_NATIVE") != "1" {
		t.Skip("requires WEIR_MEMORY_DARWIN_NATIVE=1; allocates up to 2 GiB")
	}
	beforeGoroutines := runtime.NumGoroutine()
	beforeFDs := darwinFDCount(t)
	t.Logf("native Go=%s %s/%s PID=%d", runtime.Version(), runtime.GOOS, runtime.GOARCH, os.Getpid())
	libproc.once.Do(bindLibproc)
	if libproc.err != nil {
		t.Fatal(libproc.err)
	}
	handle := libproc.handle
	var invalid rusageV0
	if code := libproc.call(int32(os.Getpid()), -1, &invalid); code != -1 {
		t.Fatal("real invalid flavor", code)
	}
	runtime.KeepAlive(&invalid)
	if o := processObservation(); !o.processValid {
		t.Fatal("native call recovery", o)
	}
	target := &guardTarget{}
	targets := []Target{target}
	guard := New(targets, DefaultLimits())
	// Bound fixture allocations while exercising the actual memory sampler.
	guard.processBudget = darwinMemoryBudget
	initial := guard.Snapshot()
	if initial.Unknown || initial.Latched || initial.Bytes > 128<<20 {
		t.Fatal("startup", initial)
	}
	pressure := &darwinPressure{base: initial.Bytes}
	defer pressure.set(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); guard.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("Guard join deadline")
		}
	}()
	var workers sync.WaitGroup
	workers.Add(4)
	for range 4 {
		go func() {
			defer workers.Done()
			for range 500 {
				if o := processObservation(); !o.processValid {
					t.Error("concurrent native sample invalid")
				}
				guard.Snapshot()
			}
		}()
	}
	for _, step := range []struct {
		label   string
		percent uint64
		latched bool
	}{{"high", 85, true}, {"middle", 75, true}, {"low", 0, false}} {
		pressure.set(t, step.percent)
		runtime.GC()
		start := time.Now()
		// Middle must stay latched for more than two sampler intervals.
		if step.label == "middle" {
			time.Sleep(300 * time.Millisecond)
		}
		for {
			s := guard.Snapshot()
			if !s.Unknown && s.Latched == step.latched {
				if step.label == "middle" && (s.Bytes <= watermark(darwinMemoryBudget, 70, false) || s.Bytes >= watermark(darwinMemoryBudget, 80, true)) {
					t.Fatal("not in hysteresis band", s)
				}
				if s.Source != "darwin_phys_footprint" || s.Cgroup.State != "not_applicable" || target.latched.Load() != step.latched {
					t.Fatal(s)
				}
				t.Logf("%s elapsed=%s footprint=%d allocated=%d", step.label, time.Since(start), s.Bytes, len(pressure.pages)<<20)
				break
			}
			if time.Since(start) > 2*time.Second {
				t.Fatal("Guard did not converge within 2s", s)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	workers.Wait()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled Run failed to join")
	}
	for range 32 {
		next := New(nil, DefaultLimits())
		canceled, stop := context.WithCancel(context.Background())
		stop()
		next.Run(canceled)
		if s := next.Snapshot(); s.Unknown || s.Latched {
			t.Fatal("repeat init", s)
		}
	}
	if libproc.handle != handle {
		t.Fatal("library lifetime changed")
	}
	afterGoroutines, afterFDs := runtime.NumGoroutine(), darwinFDCount(t)
	t.Logf("joined native sampler: goroutines=%d->%d file descriptors=%d->%d", beforeGoroutines, afterGoroutines, beforeFDs, afterFDs)
	if afterGoroutines > beforeGoroutines || afterFDs > beforeFDs {
		t.Fatal("native sampler resources remained after join")
	}
}

// darwinPressure holds anonymous mappings so the process footprint reaches a
// percentage of darwinMemoryBudget.
type darwinPressure struct {
	base  uint64
	pages [][]byte
}

func (p *darwinPressure) set(t *testing.T, percent uint64) {
	t.Helper()
	target := uint64(darwinMemoryBudget) * percent / 100
	count := 0
	if target > p.base {
		count = int((target - p.base) / (1 << 20))
	}
	for len(p.pages) > count {
		last := len(p.pages) - 1
		if err := syscall.Munmap(p.pages[last]); err != nil {
			t.Error(err)
		}
		p.pages = p.pages[:last]
	}
	for len(p.pages) < count {
		page, err := syscall.Mmap(-1, 0, 1<<20, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
		if err != nil {
			t.Fatal(err)
		}
		p.pages = append(p.pages, page)
		for i := 0; i < len(page); i += os.Getpagesize() {
			page[i] = 1
		}
	}
}

func darwinFDCount(t *testing.T) int {
	t.Helper()
	files, err := os.ReadDir("/dev/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(files)
}
