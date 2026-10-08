package overload

import "testing"

func TestAutomaticBudgetUsesHostProcessAndSmallestContainerCapacity(t *testing.T) {
	guard := &Guard{processBudget: 8 << 30}
	cgroup := CgroupSnapshot{State: "v2", Valid: true, Finite: true, Limit: 4 << 30, Capacity: 2 << 30}
	sample := observation{bytes: 64 << 20, processValid: true, cgroup: cgroup, low: true}
	guard.sample(sample)
	if snapshot := guard.Snapshot(); snapshot.Budget != 2<<30 || snapshot.Latched {
		t.Fatal(snapshot)
	}
	// A pressured ancestor and a tighter leaf have independent accounting.
	sample.high = true
	sample.low = false
	guard.sample(sample)
	if !guard.Snapshot().Latched {
		t.Fatal("container pressure did not close admission")
	}
	sample.high = false
	sample.low = true
	guard.sample(sample)
	if guard.Snapshot().Latched {
		t.Fatal("low container pressure did not recover")
	}
}

func TestMissingMemoryObservationDoesNotRejectHealthyWork(t *testing.T) {
	guard := &Guard{processBudget: 8 << 30}
	cgroup := CgroupSnapshot{State: "unknown"}
	sample := observation{bytes: 64 << 20, processValid: true, cgroup: cgroup}
	guard.sample(sample)
	if snapshot := guard.Snapshot(); !snapshot.Unknown || snapshot.Latched {
		t.Fatal(snapshot)
	}
}
