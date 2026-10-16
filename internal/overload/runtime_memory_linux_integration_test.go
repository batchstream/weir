//go:build integration && linux

package overload

import (
	"context"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

func TestLinuxAutomaticRuntimeMemoryRecoversReleasedHeap(t *testing.T) {
	runLinuxRuntimeMemoryRecovery(t, true)
}

func TestLinuxExplicitOffKeepsReleasedHeapBaseline(t *testing.T) {
	runLinuxRuntimeMemoryRecovery(t, false)
}

func runLinuxRuntimeMemoryRecovery(t *testing.T, automatic bool) {
	t.Helper()
	if os.Getenv("WEIR_MEMORY_RECOVERY") != "1" {
		t.Skip("requires an owned 256 MiB Linux cgroup and a separate process per case")
	}
	_, explicit := os.LookupEnv("GOMEMLIMIT")
	if automatic && explicit || !automatic && os.Getenv("GOMEMLIMIT") != "off" {
		t.Fatal("automatic case requires unset GOMEMLIMIT; baseline requires explicit off")
	}
	limits := DefaultLimits()
	target := &guardTarget{}
	targets := []Target{target}
	guard := New(targets, limits)
	if snapshot := guard.Snapshot(); snapshot.Unknown || snapshot.Latched || snapshot.Cgroup.Capacity != 256<<20 || snapshot.Cgroup.Limit != 256<<20 {
		t.Fatal("unexpected owned memory fixture", snapshot)
	}
	cleanup := guard.ManageRuntimeMemoryLimit()
	defer cleanup()
	if snapshot := guard.Snapshot(); snapshot.GoRuntimeManaged != automatic || automatic && snapshot.GoRuntimeLimit != 150323855 || !automatic && snapshot.GoRuntimeLimit != math.MaxInt64 {
		t.Fatal("runtime memory policy differs from the process owner", snapshot)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		guard.Run(ctx)
	}()
	defer func() { cancel(); <-done }()
	heap := make([]byte, 224<<20)
	for index := 0; index < len(heap); index += os.Getpagesize() {
		heap[index] = 1
	}
	deadline := time.Now().Add(2 * time.Second)
	for !target.latched.Load() {
		if time.Now().After(deadline) {
			t.Fatal("live Go heap did not trigger real memory protection", guard.Snapshot())
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if snapshot := guard.Snapshot(); !snapshot.Latched || snapshot.Cgroup.WorkingSet < watermark(256<<20, 80, true) {
		t.Fatal("live heap pressure falsely recovered", snapshot)
	}
	runtime.KeepAlive(heap)
	heap = nil
	started := time.Now()
	deadline = started.Add(5 * time.Second)
	var stats runtime.MemStats
	for {
		snapshot := guard.Snapshot()
		runtime.ReadMemStats(&stats)
		recovered := !snapshot.Unknown && !snapshot.Latched && snapshot.Bytes <= watermark(256<<20, 70, false) &&
			snapshot.Cgroup.WorkingSet <= watermark(256<<20, 70, false)
		if recovered {
			if !automatic {
				t.Logf("explicit-off baseline spontaneously recovered after=%s HeapAlloc=%d HeapIdle=%d HeapReleased=%d snapshot=%+v", time.Since(started), stats.HeapAlloc, stats.HeapIdle, stats.HeapReleased, snapshot)
				return
			}
			t.Logf("automatic recovery=%s HeapAlloc=%d HeapIdle=%d HeapReleased=%d GoLimit=%d snapshot=%+v", time.Since(started), stats.HeapAlloc, stats.HeapIdle, stats.HeapReleased, debug.SetMemoryLimit(-1), snapshot)
			return
		}
		if time.Now().After(deadline) {
			if automatic {
				t.Fatalf("released heap failed to recover within 5s without forced GC: HeapAlloc=%d HeapIdle=%d HeapReleased=%d snapshot=%+v", stats.HeapAlloc, stats.HeapIdle, stats.HeapReleased, snapshot)
			}
			t.Logf("explicit-off baseline still rejects after=%s HeapAlloc=%d HeapIdle=%d HeapReleased=%d snapshot=%+v", time.Since(started), stats.HeapAlloc, stats.HeapIdle, stats.HeapReleased, snapshot)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}
