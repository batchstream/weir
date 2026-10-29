package overload

import (
	"math"
	"runtime"
	"runtime/debug"
	"testing"
	"testing/fstest"
	"time"
)

type guardTarget struct{ calls []bool }

func (t *guardTarget) SetOverloaded(value bool) { t.calls = append(t.calls, value) }

// sink keeps churned allocations on the heap.
var sink []byte

func TestGuardHysteresis(t *testing.T) {
	target := &guardTarget{}
	guard := &Guard{targets: []Target{target}}
	for _, step := range []struct {
		live, limit uint64
		want        bool
	}{
		{100, 1000, false},
		{800, 1000, true},
		{750, 1000, true},
		{700, 1000, false},
		{900, 1000, true},
		{900, 0, false}, // removing the limit reopens admission
		{2000, 0, false},
	} {
		guard.observeLocked(step.live, step.limit)
		if got := guard.Snapshot(); got.Overloaded != step.want || got.Live != step.live || got.Limit != step.limit {
			t.Fatal(got, step)
		}
	}
	// Targets hear only transitions.
	want := []bool{true, false, true, false}
	if len(target.calls) != len(want) {
		t.Fatal(target.calls)
	}
	for i := range want {
		if target.calls[i] != want[i] {
			t.Fatal(target.calls)
		}
	}
}

func TestGuardSamplesTheGoMemoryLimit(t *testing.T) {
	prior := debug.SetMemoryLimit(math.MaxInt64)
	defer debug.SetMemoryLimit(prior)
	target := &guardTarget{}
	targets := []Target{target}
	guard := New(targets)
	if got := guard.Snapshot(); got.Live == 0 || got.Limit != 0 || got.Overloaded {
		t.Fatal("unlimited runtime", got)
	}
	// A limit below current use closes admission on the next sample.
	debug.SetMemoryLimit(1 << 20)
	guard.sample()
	if got := guard.Snapshot(); got.Limit != 1<<20 || !got.Overloaded || len(target.calls) != 1 {
		t.Fatal("tight limit", got)
	}
	debug.SetMemoryLimit(math.MaxInt64)
	guard.sample()
	if got := guard.Snapshot(); got.Overloaded || len(target.calls) != 2 {
		t.Fatal("restored limit", got)
	}
}

// Garbage and free heap the collector keeps between cycles must not count:
// with the default GOGC they reach about twice the live heap.
func TestGuardShedsOnLiveMemoryNotGarbage(t *testing.T) {
	const limit = 128 << 20
	prior := debug.SetMemoryLimit(limit)
	defer debug.SetMemoryLimit(prior)
	runtime.GC()
	guard := New(nil)

	held := make([][]byte, 0, 112)
	for range 48 {
		held = append(held, make([]byte, 1<<20))
	}
	for range 400 {
		sink = make([]byte, 1<<20)
		guard.sample()
		if got := guard.Snapshot(); got.Overloaded {
			t.Fatal("garbage above a 48 MiB live heap closed admission", got)
		}
	}

	for range 64 {
		held = append(held, make([]byte, 1<<20))
	}
	runtime.GC()
	guard.sample()
	if got := guard.Snapshot(); !got.Overloaded {
		t.Fatal("a 112 MiB live heap kept admission open", got)
	}
	runtime.KeepAlive(held)

	// Once the work's memory is garbage, the guard collects to notice.
	deadline := time.Now().Add(3 * collectInterval)
	for guard.Snapshot().Overloaded {
		if time.Now().After(deadline) {
			t.Fatal("admission stayed closed after live memory was released", guard.Snapshot())
		}
		time.Sleep(10 * time.Millisecond)
		guard.sample()
	}
}

func TestGoMemoryLimitLeavesTenPercentOfTheContainer(t *testing.T) {
	files := fstest.MapFS{
		"proc/self/cgroup":         {Data: []byte("0::/\n")},
		"sys/fs/cgroup/memory.max": {Data: []byte("1000000000\n")},
	}
	if got := goMemoryLimit(files); got != 900000000 {
		t.Fatal(got)
	}
	empty := fstest.MapFS{}
	if got := goMemoryLimit(empty); got != 0 {
		t.Fatal(got)
	}
}

func TestExplicitGOMEMLIMITTakesPrecedence(t *testing.T) {
	prior := debug.SetMemoryLimit(math.MaxInt64)
	defer debug.SetMemoryLimit(prior)
	for _, value := range []string{"off", "1GiB"} {
		t.Setenv("GOMEMLIMIT", value)
		SetContainerMemoryLimit()
		if got := debug.SetMemoryLimit(-1); got != math.MaxInt64 {
			t.Fatal(value, got)
		}
	}
}

func TestContainerMemoryLimit(t *testing.T) {
	for _, test := range []struct {
		name  string
		files fstest.MapFS
		want  uint64
	}{
		{"namespaced", fstest.MapFS{
			"proc/self/cgroup":         {Data: []byte("0::/\n")},
			"sys/fs/cgroup/memory.max": {Data: []byte("536870912\n")},
		}, 512 << 20},
		{"host namespace", fstest.MapFS{
			"proc/self/cgroup": {Data: []byte("0::/system.slice/weir.service\n")},
			"sys/fs/cgroup/system.slice/weir.service/memory.max": {Data: []byte("1073741824\n")},
		}, 1 << 30},
		{"unlimited", fstest.MapFS{
			"proc/self/cgroup":         {Data: []byte("0::/\n")},
			"sys/fs/cgroup/memory.max": {Data: []byte("max\n")},
		}, 0},
		{"zero", fstest.MapFS{
			"proc/self/cgroup":         {Data: []byte("0::/\n")},
			"sys/fs/cgroup/memory.max": {Data: []byte("0\n")},
		}, 0},
		{"malformed", fstest.MapFS{
			"proc/self/cgroup":         {Data: []byte("0::/\n")},
			"sys/fs/cgroup/memory.max": {Data: []byte("512M\n")},
		}, 0},
		{"parent path", fstest.MapFS{
			"proc/self/cgroup":         {Data: []byte("0::/../\n")},
			"sys/fs/cgroup/memory.max": {Data: []byte("536870912\n")},
		}, 0},
		{"cgroup v1", fstest.MapFS{
			"proc/self/cgroup": {Data: []byte("12:memory:/docker/abc\n1:name=systemd:/docker/abc\n")},
		}, 0},
		{"no memory controller", fstest.MapFS{
			"proc/self/cgroup": {Data: []byte("0::/\n")},
		}, 0},
		{"not linux", fstest.MapFS{}, 0},
	} {
		if got := containerMemoryLimit(test.files); got != test.want {
			t.Fatal(test.name, got)
		}
	}
}
