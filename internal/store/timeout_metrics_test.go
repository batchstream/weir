package store

import (
	"testing"

	"github.com/batchstream/weir/internal/testutil/testmetrics"
)

func TestRuntimeTimeoutMetricsStartAtZero(t *testing.T) {
	runtime := newRuntime(nil, DefaultLimits())
	families := testmetrics.Gather(t, runtime)
	for _, family := range families {
		if family.GetName() == "weir_store_feedback" {
			t.Fatal("removed execution state metric remains registered")
		}
	}
	if got := testmetrics.Sample(families, "weir_store_backend_timeouts_total", nil).GetCounter().GetValue(); got != 0 {
		t.Fatal("new runtime has a backend timeout", got)
	}
}
