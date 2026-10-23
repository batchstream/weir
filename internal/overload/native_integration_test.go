//go:build integration && linux

package overload

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestLinuxReclaimablePageCacheDoesNotOverload(t *testing.T) {
	if os.Getenv("WEIR_MEMORY_PAGE_CACHE") != "1" {
		t.Skip("requires WEIR_MEMORY_PAGE_CACHE=1 in a 256 MiB cgroup; see scripts/test-memory-linux.py")
	}
	guard := New(nil, DefaultLimits())
	initial := guard.Snapshot()
	const limit = 256 << 20
	if initial.Unknown || initial.Cgroup.Limit != limit || initial.Latched {
		t.Fatal("unexpected owned fixture", initial)
	}
	file, err := os.Create(filepath.Join(t.TempDir(), "owned-page-cache"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	chunk := make([]byte, 1<<20)
	for range 224 {
		if _, err := file.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	guard.sample(guard.profile.observe(DefaultLimits()))
	snapshot := guard.Snapshot()
	inactive := independentStat(t, "inactive_file")
	current := independentNumber(t, "/sys/fs/cgroup/memory.current")
	rss := independentRSS(t)
	t.Logf("file_cache current=%d inactive_file=%d working_set=%d RSS=%d snapshot=%+v", current, inactive, current-min(current, inactive), rss, snapshot)
	if current < watermark(limit, 80, true) || inactive < 200<<20 || rss > 32<<20 {
		t.Fatal("fixture did not establish high current with low RSS and reclaimable cache")
	}
	if snapshot.Unknown || snapshot.Latched {
		t.Fatal("reclaimable page cache rejected healthy work", snapshot)
	}
	page, err := syscall.Mmap(-1, 0, 224<<20, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if page != nil {
			_ = syscall.Munmap(page)
		}
	}()
	for i := 0; i < len(page); i += os.Getpagesize() {
		page[i] = 1
	}
	guard.sample(guard.profile.observe(DefaultLimits()))
	snapshot = guard.Snapshot()
	t.Logf("anonymous pressure snapshot=%+v", snapshot)
	if snapshot.Unknown || !snapshot.Latched || snapshot.Cgroup.WorkingSet < watermark(limit, 80, true) {
		t.Fatal("real anonymous memory pressure did not latch", snapshot)
	}
	if err := syscall.Munmap(page); err != nil {
		t.Fatal(err)
	}
	page = nil
	// Restore a high raw charge after anonymous pages have evicted some cache.
	// Recovery must depend on live working set even while current stays high.
	if _, err := file.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	for range 224 {
		if _, err := file.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	guard.sample(guard.profile.observe(DefaultLimits()))
	snapshot = guard.Snapshot()
	t.Logf("released anonymous pressure snapshot=%+v", snapshot)
	if snapshot.Unknown || snapshot.Latched || snapshot.Cgroup.Current < watermark(limit, 80, true) {
		t.Fatal("released anonymous pressure did not recover", snapshot)
	}
}

func independentStat(t *testing.T, key string) uint64 {
	t.Helper()
	raw, err := os.ReadFile("/sys/fs/cgroup/memory.stat")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		var name string
		var n uint64
		_, err := fmt.Sscanf(line, "%s %d", &name, &n)
		if err == nil && name == key {
			return n
		}
	}
	t.Fatal("memory.stat field absent", key)
	return 0
}

func TestLinuxReleasedGoHeapRecoversAfterScavenge(t *testing.T) {
	if os.Getenv("WEIR_MEMORY_PAGE_CACHE") != "1" {
		t.Skip("requires WEIR_MEMORY_PAGE_CACHE=1 in a 256 MiB cgroup; see scripts/test-memory-linux.py")
	}
	guard := New(nil, DefaultLimits())
	if snapshot := guard.Snapshot(); snapshot.Unknown || snapshot.Cgroup.Limit != 256<<20 || snapshot.Latched {
		t.Fatal("unexpected owned fixture", snapshot)
	}
	heap := make([]byte, 224<<20)
	for i := 0; i < len(heap); i += os.Getpagesize() {
		heap[i] = 1
	}
	guard.sample(guard.profile.observe(DefaultLimits()))
	t.Logf("live Go heap snapshot=%+v", guard.Snapshot())
	runtime.KeepAlive(heap)
	if !guard.Snapshot().Latched {
		t.Fatal("large live Go heap did not latch pressure")
	}
	heap = nil
	runtime.GC()
	guard.sample(guard.profile.observe(DefaultLimits()))
	snapshot := guard.Snapshot()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	t.Logf("after GC heap_alloc=%d heap_idle=%d heap_released=%d snapshot=%+v", stats.HeapAlloc, stats.HeapIdle, stats.HeapReleased, snapshot)
	if snapshot.Cgroup.WorkingSet >= watermark(256<<20, 80, true) && !snapshot.Latched {
		t.Fatal("low live Go heap hid real resident pressure", snapshot)
	}
	// Release pages only in this owned test; production Guard never forces GC.
	debug.FreeOSMemory()
	start := time.Now()
	for {
		guard.sample(guard.profile.observe(DefaultLimits()))
		snapshot := guard.Snapshot()
		runtime.ReadMemStats(&stats)
		if !snapshot.Unknown && !snapshot.Latched {
			t.Logf("released Go heap recovered after=%s heap_alloc=%d heap_idle=%d heap_released=%d snapshot=%+v", time.Since(start), stats.HeapAlloc, stats.HeapIdle, stats.HeapReleased, snapshot)
			return
		}
		if time.Since(start) > 5*time.Second {
			t.Fatalf("released Go heap remained overloaded after=%s heap_alloc=%d heap_idle=%d heap_released=%d snapshot=%+v", time.Since(start), stats.HeapAlloc, stats.HeapIdle, stats.HeapReleased, snapshot)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestLinuxSharedMemoryDoesOverload(t *testing.T) {
	if os.Getenv("WEIR_MEMORY_SHMEM") != "1" {
		t.Skip("requires WEIR_MEMORY_SHMEM=1 in a 256 MiB cgroup with 240 MiB shm; see scripts/test-memory-linux.py")
	}
	guard := New(nil, DefaultLimits())
	if snapshot := guard.Snapshot(); snapshot.Unknown || snapshot.Cgroup.Limit != 256<<20 || snapshot.Latched {
		t.Fatal("unexpected owned fixture", snapshot)
	}
	file, err := os.CreateTemp("/dev/shm", "weir-owned-pressure-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())
	defer file.Close()
	chunk := make([]byte, 1<<20)
	for range 224 {
		if _, err := file.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	guard.sample(guard.profile.observe(DefaultLimits()))
	snapshot := guard.Snapshot()
	shmem := independentStat(t, "shmem")
	inactive := independentStat(t, "inactive_file")
	t.Logf("shared_memory shmem=%d inactive_file=%d snapshot=%+v", shmem, inactive, snapshot)
	if snapshot.Unknown || !snapshot.Latched || shmem < 220<<20 || snapshot.Cgroup.WorkingSet < watermark(256<<20, 80, true) {
		t.Fatal("real shared-memory pressure was treated as reclaimable cache", snapshot)
	}
	if err := file.Truncate(0); err != nil {
		t.Fatal(err)
	}
	guard.sample(guard.profile.observe(DefaultLimits()))
	snapshot = guard.Snapshot()
	t.Logf("released shared_memory snapshot=%+v", snapshot)
	if snapshot.Unknown || snapshot.Latched {
		t.Fatal("released shared-memory pressure did not recover", snapshot)
	}
}

func TestLinuxMemoryNative(t *testing.T) {
	if os.Getenv("WEIR_MEMORY_NATIVE") != "1" {
		t.Skip("requires WEIR_MEMORY_NATIVE=1; see scripts/test-memory-linux.py")
	}
	var uname syscall.Utsname
	if err := syscall.Uname(&uname); err != nil {
		t.Fatal(err)
	}
	t.Logf("native kernel=%s machine=%s Go=%s compiled=%s/%s PID=%d", uts(uname.Release[:]), uts(uname.Machine[:]), runtime.Version(), runtime.GOOS, runtime.GOARCH, os.Getpid())
	targets := []Target{&guardTarget{}}
	const budget = 128 << 20
	guard := New(targets, DefaultLimits())
	// Bound fixture allocations while exercising the actual memory sampler.
	guard.processBudget = budget
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); guard.Run(ctx) }()
	defer func() { cancel(); <-done }()
	initial := guard.Snapshot()
	if initial.Unknown || initial.Cgroup.Limit != 512<<20 || !initial.Cgroup.Finite || initial.Latched || initial.Source != "linux_rss" {
		t.Fatal(initial)
	}
	var pages [][]byte
	defer func() {
		for _, page := range pages {
			_ = syscall.Munmap(page)
		}
	}()
	base := initial.Bytes
	for _, step := range []struct {
		label   string
		percent uint64
		latched bool
	}{{"high", 85, true}, {"middle", 75, true}, {"low", 0, false}} {
		target := uint64(budget) * step.percent / 100
		count := 0
		if target > base {
			count = int((target - base) / (1 << 20))
		}
		if count > 112 {
			t.Fatal("allocation ceiling", count)
		}
		for len(pages) > count {
			last := len(pages) - 1
			if err := syscall.Munmap(pages[last]); err != nil {
				t.Fatal(err)
			}
			pages = pages[:last]
		}
		for len(pages) < count {
			page, err := syscall.Mmap(-1, 0, 1<<20, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < len(page); i += os.Getpagesize() {
				page[i] = 1
			}
			pages = append(pages, page)
		}
		start := time.Now()
		// Middle must stay latched for more than two sampler intervals.
		if step.label == "middle" {
			time.Sleep(300 * time.Millisecond)
		}
		for {
			s := guard.Snapshot()
			rss := independentRSS(t)
			if s.Latched == step.latched && absDiff(s.Bytes, rss) < 8<<20 {
				if step.label == "middle" && (s.Bytes <= watermark(budget, 70, false) || s.Bytes >= watermark(budget, 80, true)) {
					t.Fatal("not in hysteresis band", s)
				}
				current := independentNumber(t, "/sys/fs/cgroup/memory.current")
				t.Logf("%s elapsed=%s snapshot=%+v smaps_rollup_Rss=%d RSS_delta=%d cgroup_current=%d delta=%d", step.label, time.Since(start), s, rss, absDiff(s.Bytes, rss), current, absDiff(s.Cgroup.Current, current))
				if absDiff(s.Cgroup.Current, current) > 16<<20 {
					t.Fatal("cgroup sample mismatch")
				}
				break
			}
			if time.Since(start) > 2*time.Second {
				t.Fatal("Guard did not converge within 2s", s, rss)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}
func uts(raw []int8) string {
	var b []byte
	for _, c := range raw {
		if c == 0 {
			break
		}
		b = append(b, byte(c))
	}
	return string(b)
}
func absDiff(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}
func independentNumber(t *testing.T, name string) uint64 {
	t.Helper()
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}
func independentRSS(t *testing.T) uint64 {
	t.Helper()
	raw, err := os.ReadFile("/proc/self/smaps_rollup")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "Rss:" {
			n, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return n * 1024
		}
	}
	t.Fatal("RSS absent")
	return 0
}
