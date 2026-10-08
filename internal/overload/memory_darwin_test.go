package overload

import (
	"math"
	"testing"
	"unsafe"
)

func TestDarwinFootprintValidationAndRecovery(t *testing.T) {
	var usage rusageV0
	if unsafe.Sizeof(usage) != 96 || unsafe.Alignof(usage) != 8 || unsafe.Offsetof(usage.physicalFootprint) != 72 {
		t.Fatal("SDK V0 ABI")
	}
	state := Snapshot{Budget: 1000}
	target := &guardTarget{}
	guard := &Guard{state: state, processBudget: state.Budget, targets: []Target{target}}
	for _, step := range []struct {
		status         int32
		bytes          uint64
		valid, latched bool
	}{
		{-1, 0, false, false}, {0, 500, true, false}, {0, 850, true, true}, {0, 750, true, true},
		{-1, 500, false, true}, {0, 750, true, true}, {0, 500, true, false}, {0, 0, false, false},
		{0, math.MaxUint64, false, false}, {0, math.MaxInt64, true, true}, {1, 500, false, true}, {0, 500, true, false},
	} {
		guard.sample(footprintObservation(step.status, step.bytes))
		got := guard.Snapshot()
		if got.ProcessValid != step.valid || got.Unknown == step.valid || got.Latched != step.latched || target.latched.Load() != step.latched || got.Source != "darwin_phys_footprint" || got.Cgroup.State != "not_applicable" {
			t.Fatal(got, step)
		}
		if !step.valid && got.Bytes != 0 {
			t.Fatal("invalid sample published", got)
		}
	}
}
