//go:build integration && linux

package overload

import (
	"context"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	runtimemetrics "runtime/metrics"
	"strconv"
	"syscall"
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
	if snapshot := guard.Snapshot(); snapshot.GoRuntimeManaged != automatic || automatic && (!snapshot.RecoveryAllowanceKnown || snapshot.GoRuntimeLimit == 0 || snapshot.GoRuntimeLimit > 150323855 || snapshot.GoRuntimeLimit != watermark(snapshot.RecoveryAllowance, 80, false)) || !automatic && snapshot.GoRuntimeLimit != math.MaxInt64 {
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

// This experiment keeps tmpfs pages charged to the cgroup without mapping them
// into Go's RSS, including the case where other users occupy the recovery line.
func TestLinuxRuntimeMemoryExternalPressure(t *testing.T) {
	value := os.Getenv("WEIR_RUNTIME_SHARED_PRESSURE_MIB")
	if value == "" {
		t.Skip("explicit owned 512 MiB Linux shared-memory experiment required")
	}
	if _, explicit := os.LookupEnv("GOMEMLIMIT"); explicit {
		t.Fatal("shared-pressure experiment requires absent explicit GOMEMLIMIT")
	}
	sharedMiB, err := strconv.Atoi(value)
	if err != nil || sharedMiB != 344 && sharedMiB != 352 && sharedMiB != 368 {
		t.Fatal("requires a fixed 344/352/368 MiB shared fixture")
	}
	limits := DefaultLimits()
	guard := New(nil, limits)
	if snapshot := guard.Snapshot(); snapshot.Unknown || snapshot.Cgroup.Capacity != 512<<20 {
		t.Fatal("unexpected owned external-pressure fixture", snapshot)
	}
	cleanup := guard.ManageRuntimeMemoryLimit()
	defer cleanup()
	file, err := os.CreateTemp("/dev/shm", "weir-owned-runtime-pressure-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(file.Name()); err != nil {
			t.Error(err)
		}
	})
	block := make([]byte, 32<<10)
	for remaining := sharedMiB << 20; remaining > 0; remaining -= len(block) {
		if _, err := file.Write(block); err != nil {
			file.Close()
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	guard.sample(guard.profile.observe(limits))
	if sharedMiB == 368 {
		heap := make([]byte, 64<<20)
		for index := 0; index < len(heap); index += os.Getpagesize() {
			heap[index] = 1
		}
		guard.sample(guard.profile.observe(limits))
		if !guard.Snapshot().Latched {
			t.Fatal("live heap plus external pressure did not latch admission", guard.Snapshot())
		}
		runtime.KeepAlive(heap)
		heap = nil
	}
	before := runtimeMemoryCPU(t)
	started := time.Now()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		guard.Run(ctx)
	}()
	defer func() { cancel(); <-done }()
	for time.Since(started) < 5*time.Second {
		// Model a low-frequency caller without forcing GC or OS reclamation.
		work := make([]byte, 64<<10)
		for index := 0; index < len(work); index += os.Getpagesize() {
			work[index] = 1
		}
		runtime.KeepAlive(work)
		time.Sleep(20 * time.Millisecond)
	}
	after := runtimeMemoryCPU(t)
	snapshot := guard.Snapshot()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	t.Logf("external=%dMiB elapsed=%s process_CPU=%gs GC_CPU=%gs GC_cycles=%g heap=%d Go_memory=%d snapshot=%+v", sharedMiB, time.Since(started), after[0]-before[0], after[1]-before[1], after[2]-before[2], memory.HeapAlloc, memory.Sys-memory.HeapReleased, snapshot)
	if snapshot.Unknown || !snapshot.RecoveryAllowanceKnown || !snapshot.GoRuntimeManaged || snapshot.GoRuntimeLimit < minimumAutomaticRuntimeLimit {
		t.Fatal("external pressure lost trusted allowance or installed zero runtime limit", snapshot)
	}
	if sharedMiB == 368 && (snapshot.RecoveryAllowance != 0 || !snapshot.Latched) {
		t.Fatal("other users above the recovery line created a false process allowance", snapshot)
	}
}

func runtimeMemoryCPU(t *testing.T) [3]float64 {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	samples := []runtimemetrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}, {Name: "/gc/cycles/total:gc-cycles"}}
	runtimemetrics.Read(samples)
	values := [3]float64{float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6, samples[0].Value.Float64(), float64(samples[1].Value.Uint64())}
	return values
}
