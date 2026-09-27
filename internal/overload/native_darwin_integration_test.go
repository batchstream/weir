//go:build integration && darwin

package overload

import (
	"context"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil/testmemory"
)

func TestDarwinMemoryNative(t *testing.T) {
	pressure := testmemory.Open(t)
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
	guard := New(targets, testmemory.Budget)
	if s := guard.Snapshot(); s.Unknown || s.Latched {
		t.Fatal("startup", s)
	}
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
		pressure.Set(t, step.percent)
		runtime.GC()
		start := time.Now()
		if step.label == "middle" {
			time.Sleep(300 * time.Millisecond)
		}
		for {
			s := guard.Snapshot()
			oracle := testmemory.Oracle(t)
			if !s.Unknown && s.Latched == step.latched && testmemory.Difference(s.Bytes, oracle) < testmemory.Tolerance {
				if step.label == "middle" && (s.Bytes <= watermark(testmemory.Budget, 70, false) || s.Bytes >= watermark(testmemory.Budget, 80, true)) {
					t.Fatal("outside frozen middle band", s)
				}
				if s.Source != "darwin_phys_footprint" || s.Cgroup.State != "not_applicable" || target.latched.Load() != step.latched {
					t.Fatal(s)
				}
				t.Logf("%s elapsed=%s footprint=%d SDK_oracle=%d difference=%d latched=%v", step.label, time.Since(start), s.Bytes, oracle, testmemory.Difference(s.Bytes, oracle), s.Latched)
				break
			}
			if time.Since(start) > 2*time.Second {
				t.Fatal("frozen convergence deadline", s, oracle)
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
		next := New(nil, testmemory.Budget)
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
	t.Logf("joined native sampler: goroutines=%d->%d file descriptors=%d->%d; all owned mmap released", beforeGoroutines, afterGoroutines, beforeFDs, afterFDs)
	if afterGoroutines > beforeGoroutines || afterFDs > beforeFDs {
		t.Fatal("native sampler resources remained after join")
	}
	t.Log("real invalid flavor=-1, native recovery, concurrent samples/GC, 32 repeated initialization/canceled Run; one process-lifetime library reference")
}

func darwinFDCount(t *testing.T) int {
	t.Helper()
	files, err := os.ReadDir("/dev/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(files)
}
