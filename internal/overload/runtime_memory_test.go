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
	t.Run("explicit_limit_below_automatic_floor", func(t *testing.T) {
		isolateRuntimeMemory(t)
		debug.SetMemoryLimit(1 << 20)
		guard := memoryLimitGuard()
		cleanup := guard.ManageRuntimeMemoryLimit()
		cleanup()
		if snapshot := guard.Snapshot(); snapshot.GoRuntimeManaged || snapshot.GoRuntimeLimit != 1<<20 || debug.SetMemoryLimit(-1) != 1<<20 {
			t.Fatal("an explicit finite limit below the automatic floor was replaced", snapshot)
		}
	})
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
	huge := Snapshot{Observed: true, ProcessValid: true, Budget: math.MaxUint64, RecoveryAllowance: math.MaxUint64, RecoveryAllowanceKnown: true}
	if runtimeMemoryTarget(huge, 70) != math.MaxInt64 {
		t.Fatal("large automatic capacity overflowed int64 runtime units")
	}
}

func TestRuntimeMemoryLimitUsesEveryVisibleRecoveryAllowance(t *testing.T) {
	isolateRuntimeMemory(t)
	f := fixture(t)
	rss := uint64(100 << 20)
	pages := strconv.FormatUint(rss/uint64(os.Getpagesize()), 10)
	f.write(t, filepath.Join(f.proc, "statm"), pages+" "+pages+" 1 0 0 0 0\n")
	f.level(t, "a/leaf", strconv.FormatUint(512<<20, 10), strconv.FormatUint(300<<20, 10))
	f.level(t, "a", strconv.FormatUint(2<<30, 10), strconv.FormatUint(1300<<20, 10))
	guard := &Guard{limits: DefaultLimits(), processBudget: 8 << 30}
	guard.sample(f.profile.observe(DefaultLimits()))
	if snapshot := guard.Snapshot(); snapshot.Cgroup.Scope != "ancestor" || snapshot.Cgroup.Limit != 2<<30 || snapshot.Budget != 512<<20 {
		t.Fatal("fixture did not distinguish most pressured level from smallest recovery allowance", snapshot)
	}
	cleanup := guard.ManageRuntimeMemoryLimit()
	defer cleanup()
	leafAllowance := watermark(512<<20, 70, false) - (300<<20 - rss)
	assertRuntimeRecoveryAllowance(t, guard, leafAllowance)

	f.level(t, "a", strconv.FormatUint(256<<20, 10), strconv.FormatUint(200<<20, 10))
	guard.sample(f.profile.observe(DefaultLimits()))
	ancestorAllowance := watermark(256<<20, 70, false) - (200<<20 - rss)
	assertRuntimeRecoveryAllowance(t, guard, ancestorAllowance)
	f.write(t, filepath.Join(f.mount, "a", "memory.max"), "temporarily incomplete")
	guard.sample(f.profile.observe(DefaultLimits()))
	snapshot := guard.Snapshot()
	if !snapshot.Unknown || snapshot.RecoveryAllowanceKnown || snapshot.GoRuntimeLimit != watermark(ancestorAllowance, 80, false) {
		t.Fatal("incomplete capacity change loosened the last trusted limit", snapshot)
	}
	f.level(t, "a", strconv.FormatUint(2<<30, 10), strconv.FormatUint(200<<20, 10))
	guard.sample(f.profile.observe(DefaultLimits()))
	assertRuntimeRecoveryAllowance(t, guard, leafAllowance)

	guard.processBudget = 128 << 20
	guard.sample(f.profile.observe(DefaultLimits()))
	assertRuntimeRecoveryAllowance(t, guard, watermark(128<<20, 70, false))

	prior := debug.SetMemoryLimit(-1)
	f.level(t, "a/leaf", strconv.FormatUint(512<<20, 10), strconv.FormatUint(500<<20, 10))
	guard.sample(f.profile.observe(DefaultLimits()))
	snapshot = guard.Snapshot()
	if !snapshot.Latched || !snapshot.RecoveryAllowanceKnown || snapshot.RecoveryAllowance != 0 || debug.SetMemoryLimit(-1) != prior {
		t.Fatal("validated external pressure above recovery line became unknown, zero limit or false recovery", snapshot)
	}
}

func TestRuntimeMemoryLimitDoesNotSetZeroWhenOthersOccupyRecoveryLine(t *testing.T) {
	isolateRuntimeMemory(t)
	guard := memoryLimitGuard()
	cleanup := guard.ManageRuntimeMemoryLimit()
	defer cleanup()
	previous := debug.SetMemoryLimit(-1)
	cgroup := CgroupSnapshot{State: "v2", Valid: true, Finite: true, Limit: 512 << 20, Capacity: 512 << 20}
	sample := observation{bytes: 16 << 20, processValid: true, cgroup: cgroup, high: true, recoveryAllowanceKnown: true}
	guard.sample(sample)
	snapshot := guard.Snapshot()
	if !snapshot.Latched || !snapshot.RecoveryAllowanceKnown || snapshot.RecoveryAllowance != 0 || debug.SetMemoryLimit(-1) != previous {
		t.Fatal("no process recovery share manufactured a zero runtime limit or recovery", snapshot)
	}
	sample.high, sample.low = false, true
	sample.recoveryAllowance = 32 << 20
	guard.sample(sample)
	assertRuntimeRecoveryAllowance(t, guard, 32<<20)
}

func assertRuntimeRecoveryAllowance(t *testing.T, guard *Guard, allowance uint64) {
	t.Helper()
	snapshot := guard.Snapshot()
	floor := min(uint64(minimumAutomaticRuntimeLimit), watermark(watermark(snapshot.Budget, 70, false), 80, false))
	want := max(watermark(allowance, 80, false), floor)
	if !snapshot.RecoveryAllowanceKnown || snapshot.RecoveryAllowance != allowance || snapshot.GoRuntimeLimit != want || !snapshot.GoRuntimeManaged || debug.SetMemoryLimit(-1) != int64(want) {
		t.Fatal("automatic runtime limit differs from the process recovery share", snapshot, allowance, want)
	}
}

func TestRuntimeMemoryLimitBoundsTinyRecoverySharesWithoutLooseningZero(t *testing.T) {
	isolateRuntimeMemory(t)
	guard := memoryLimitGuard()
	cleanup := guard.ManageRuntimeMemoryLimit()
	defer cleanup()
	cgroup := CgroupSnapshot{State: "v2", Valid: true, Finite: true, Limit: 512 << 20, Capacity: 512 << 20}
	sample := observation{bytes: 8 << 20, processValid: true, cgroup: cgroup, recoveryAllowance: 1, recoveryAllowanceKnown: true, high: true}
	guard.sample(sample)
	assertRuntimeRecoveryAllowance(t, guard, 1)
	if !guard.Snapshot().Latched {
		t.Fatal("the runtime floor falsely cleared actual OS pressure")
	}
	sample.recoveryAllowance = 0
	guard.sample(sample)
	if snapshot := guard.Snapshot(); !snapshot.Latched || !snapshot.RecoveryAllowanceKnown || snapshot.RecoveryAllowance != 0 || snapshot.GoRuntimeLimit != minimumAutomaticRuntimeLimit {
		t.Fatal("known zero share loosened or zeroed the existing runtime floor", snapshot)
	}
	sample.cgroup.Limit, sample.cgroup.Capacity = 8<<20, 8<<20
	sample.recoveryAllowance = 1
	guard.sample(sample)
	assertRuntimeRecoveryAllowance(t, guard, 1)
	if guard.Snapshot().GoRuntimeLimit >= minimumAutomaticRuntimeLimit {
		t.Fatal("automatic floor exceeded the original full-capacity target")
	}
}
