//go:build integration && linux

package overload

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestLinuxMemoryNative(t *testing.T) {
	if os.Getenv("WEIR_MEMORY_NATIVE") != "1" {
		t.Skip("requires owned scripts/test-memory-linux.py fixture")
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
				t.Fatal("fixed 2s convergence deadline", s, rss)
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
