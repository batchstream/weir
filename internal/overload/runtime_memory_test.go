package overload

import (
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"testing"
)

func isolateRuntimeMemory(t *testing.T) {
	t.Helper()
	prior := debug.SetMemoryLimit(math.MaxInt64)
	value, present := os.LookupEnv("GOMEMLIMIT")
	if err := os.Unsetenv("GOMEMLIMIT"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		debug.SetMemoryLimit(prior)
		var err error
		if present {
			err = os.Setenv("GOMEMLIMIT", value)
		} else {
			err = os.Unsetenv("GOMEMLIMIT")
		}
		if err != nil {
			t.Error(err)
		}
	})
}

func memoryLimitGuard() *Guard {
	guard := &Guard{limits: DefaultLimits(), processBudget: 8 << 30}
	cgroup := CgroupSnapshot{State: "v2", Valid: true, Finite: true, Limit: 1 << 30, Capacity: 512 << 20}
	sample := observation{bytes: 16 << 20, processValid: true, cgroup: cgroup, low: true}
	guard.sample(sample)
	return guard
}

func TestRuntimeMemoryLimitFollowsTrustedSmallestCapacityAndRestoresOwner(t *testing.T) {
	isolateRuntimeMemory(t)
	guard := memoryLimitGuard()
	if snapshot := guard.Snapshot(); snapshot.GoRuntimeLimit != math.MaxInt64 || snapshot.GoRuntimeManaged {
		t.Fatal("ordinary Guard construction changed the process runtime", snapshot)
	}
	cleanup := guard.ManageRuntimeMemoryLimit()
	defer cleanup()
	assertRuntimeMemoryTarget(t, guard, 512<<20)
	nested := guard.ManageRuntimeMemoryLimit()
	nested()
	assertRuntimeMemoryTarget(t, guard, 512<<20)
	cgroup := CgroupSnapshot{State: "v2", Valid: true, Finite: true, Limit: 1 << 30, Capacity: 256 << 20}
	sample := observation{bytes: 16 << 20, processValid: true, cgroup: cgroup, low: true}
	guard.sample(sample)
	assertRuntimeMemoryTarget(t, guard, 256<<20)
	unknown := CgroupSnapshot{State: "unknown"}
	sample.cgroup = unknown
	guard.sample(sample)
	assertRuntimeMemoryTarget(t, guard, 256<<20)
	sample.cgroup = cgroup
	sample.cgroup.Capacity = 1 << 30
	guard.sample(sample)
	assertRuntimeMemoryTarget(t, guard, 1<<30)
	cleanup()
	if snapshot := guard.Snapshot(); snapshot.GoRuntimeManaged || snapshot.GoRuntimeLimit != math.MaxInt64 || debug.SetMemoryLimit(-1) != math.MaxInt64 {
		t.Fatal("process owner did not restore its original runtime limit", snapshot)
	}
	debug.SetMemoryLimit(300 << 20)
	cleanup()
	if debug.SetMemoryLimit(-1) != 300<<20 {
		t.Fatal("repeated owner cleanup overwrote a later runtime limit")
	}
}

func assertRuntimeMemoryTarget(t *testing.T, guard *Guard, capacity uint64) {
	t.Helper()
	want := watermark(watermark(capacity, 70, false), 80, false)
	snapshot := guard.Snapshot()
	if snapshot.GoRuntimeLimit != want || !snapshot.GoRuntimeManaged || debug.SetMemoryLimit(-1) != int64(want) {
		t.Fatal("automatic runtime limit differs from the trusted recovery budget", snapshot, want)
	}
}

func TestRuntimeMemoryLimitRespectsExplicitSettings(t *testing.T) {
	for _, setting := range []string{"off", "", "1GiB"} {
		t.Run("environment_"+setting, func(t *testing.T) {
			isolateRuntimeMemory(t)
			t.Setenv("GOMEMLIMIT", setting)
			guard := memoryLimitGuard()
			cleanup := guard.ManageRuntimeMemoryLimit()
			cleanup()
			if snapshot := guard.Snapshot(); snapshot.GoRuntimeManaged || debug.SetMemoryLimit(-1) != math.MaxInt64 {
				t.Fatal("an explicitly supplied environment setting was replaced", snapshot)
			}
		})
	}
	t.Run("existing_finite_runtime_limit", func(t *testing.T) {
		isolateRuntimeMemory(t)
		debug.SetMemoryLimit(256 << 20)
		guard := memoryLimitGuard()
		cleanup := guard.ManageRuntimeMemoryLimit()
		cleanup()
		if snapshot := guard.Snapshot(); snapshot.GoRuntimeManaged || snapshot.GoRuntimeLimit != 256<<20 || debug.SetMemoryLimit(-1) != 256<<20 {
			t.Fatal("an explicit runtime limit was replaced", snapshot)
		}
	})
}

func TestRuntimeMemoryLimitPermanentlyYieldsToExternalOwner(t *testing.T) {
	for _, sampleBeforeCleanup := range []bool{false, true} {
		name := "cleanup_without_sample"
		if sampleBeforeCleanup {
			name = "sample_observes_external_owner"
		}
		t.Run(name, func(t *testing.T) {
			isolateRuntimeMemory(t)
			guard := memoryLimitGuard()
			cleanup := guard.ManageRuntimeMemoryLimit()
			defer cleanup()
			lastApplied := debug.SetMemoryLimit(400 << 20)
			if sampleBeforeCleanup {
				cgroup := CgroupSnapshot{State: "v2", Valid: true, Finite: true, Limit: 256 << 20, Capacity: 256 << 20}
				sample := observation{bytes: 16 << 20, processValid: true, cgroup: cgroup, low: true}
				guard.sample(sample)
				if snapshot := guard.Snapshot(); snapshot.GoRuntimeManaged || snapshot.GoRuntimeLimit != 400<<20 {
					t.Fatal("automatic budget did not yield to the external setting", snapshot)
				}
				// Matching a formerly owned value later does not grant ownership back.
				debug.SetMemoryLimit(lastApplied)
				guard.sample(sample)
				if snapshot := guard.Snapshot(); snapshot.GoRuntimeManaged || debug.SetMemoryLimit(-1) != lastApplied {
					t.Fatal("automatic budget took ownership back", snapshot)
				}
			}
			want := debug.SetMemoryLimit(-1)
			cleanup()
			if snapshot := guard.Snapshot(); snapshot.GoRuntimeManaged || debug.SetMemoryLimit(-1) != want {
				t.Fatal("cleanup replaced another component's runtime setting", snapshot)
			}
		})
	}
}

func TestRuntimeMemoryLimitWaitsForUsableBudgetAndNeverSetsZero(t *testing.T) {
	isolateRuntimeMemory(t)
	guard := &Guard{limits: DefaultLimits()}
	cleanup := guard.ManageRuntimeMemoryLimit()
	defer cleanup()
	if debug.SetMemoryLimit(-1) != math.MaxInt64 || guard.Snapshot().GoRuntimeManaged {
		t.Fatal("unobserved memory budget became a zero runtime allowance")
	}
	cgroup := CgroupSnapshot{State: "v2", Valid: true, Finite: true}
	sample := observation{bytes: 1, processValid: true, cgroup: cgroup, high: true}
	guard.sample(sample)
	if debug.SetMemoryLimit(-1) != math.MaxInt64 {
		t.Fatal("zero finite cgroup capacity became a runtime limit")
	}
	guard.processBudget = 256 << 20
	sample.cgroup.Limit, sample.cgroup.Capacity = 256<<20, 256<<20
	guard.sample(sample)
	assertRuntimeMemoryTarget(t, guard, 256<<20)
	limits := DefaultLimits()
	limits.LowWatermark = 0
	if limits.Validate() == nil || runtimeMemoryTarget(guard.Snapshot(), 0) != 0 {
		t.Fatal("a zero recovery watermark is valid or became a runtime limit")
	}
	unknown := Snapshot{Observed: true, ProcessValid: true, Budget: 0}
	if runtimeMemoryTarget(unknown, 70) != 0 {
		t.Fatal("unknown capacity became a runtime allowance")
	}
	huge := Snapshot{Observed: true, ProcessValid: true, Budget: math.MaxUint64}
	if runtimeMemoryTarget(huge, 70) != math.MaxInt64 {
		t.Fatal("large automatic capacity overflowed int64 runtime units")
	}
}

func TestRuntimeMemoryLimitUsesValidatedVisibleHierarchyAcrossCapacityChanges(t *testing.T) {
	isolateRuntimeMemory(t)
	f := fixture(t)
	f.level(t, "a/leaf", strconv.FormatUint(512<<20, 10), strconv.FormatUint(16<<20, 10))
	f.level(t, "a", strconv.FormatUint(1<<30, 10), strconv.FormatUint(700<<20, 10))
	guard := &Guard{limits: DefaultLimits(), processBudget: 8 << 30}
	guard.sample(f.profile.observe(DefaultLimits()))
	if snapshot := guard.Snapshot(); snapshot.Cgroup.Scope != "ancestor" || snapshot.Cgroup.Limit != 1<<30 || snapshot.Budget != 512<<20 {
		t.Fatal("fixture did not distinguish most pressured level from smallest capacity", snapshot)
	}
	cleanup := guard.ManageRuntimeMemoryLimit()
	defer cleanup()
	assertRuntimeMemoryTarget(t, guard, 512<<20)
	f.level(t, "a", strconv.FormatUint(256<<20, 10), strconv.FormatUint(128<<20, 10))
	guard.sample(f.profile.observe(DefaultLimits()))
	assertRuntimeMemoryTarget(t, guard, 256<<20)
	f.write(t, filepath.Join(f.mount, "a", "memory.max"), "temporarily incomplete")
	guard.sample(f.profile.observe(DefaultLimits()))
	if !guard.Snapshot().Unknown {
		t.Fatal("incomplete capacity change was trusted")
	}
	assertRuntimeMemoryTarget(t, guard, 256<<20)
	f.level(t, "a", strconv.FormatUint(1<<30, 10), strconv.FormatUint(128<<20, 10))
	guard.sample(f.profile.observe(DefaultLimits()))
	assertRuntimeMemoryTarget(t, guard, 512<<20)
}
