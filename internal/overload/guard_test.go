package overload

import (
	"math"
	"runtime/debug"
	"testing"
	"testing/fstest"
)

type guardTarget struct{ calls []bool }

func (t *guardTarget) SetOverloaded(value bool) { t.calls = append(t.calls, value) }

func TestGuardHysteresis(t *testing.T) {
	target := &guardTarget{}
	guard := &Guard{targets: []Target{target}}
	for _, step := range []struct {
		used, limit uint64
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
		guard.observeLocked(step.used, step.limit)
		if got := guard.Snapshot(); got.Overloaded != step.want || got.Used != step.used || got.Limit != step.limit {
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
	if got := guard.Snapshot(); got.Used == 0 || got.Limit != 0 || got.Overloaded {
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
