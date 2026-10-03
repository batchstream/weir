package store

import (
	"testing"

	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
)

func TestCompletedTransportMetricsRemainDistinctFromHealthyBusinessEvidence(t *testing.T) {
	limits := DefaultLimits()
	runtime := newRuntime(nil, limits)
	runtime.mu.Lock()
	runtime.metrics.feedback, runtime.metrics.observed = execution.Completed, true
	runtime.mu.Unlock()
	snapshot := runtime.Snapshot()
	if snapshot.Feedback != "completed" || snapshot.ConcurrencyLimit != limits.Concurrency {
		t.Fatal("completed transport was conflated with business health", snapshot)
	}
	families := testmetrics.Gather(t, runtime)
	completedLabels := map[string]string{"feedback": "completed"}
	healthyLabels := map[string]string{"feedback": "healthy"}
	if testmetrics.Sample(families, "weir_store_feedback", completedLabels).GetGauge().GetValue() != 1 || testmetrics.Sample(families, "weir_store_feedback", healthyLabels).GetGauge().GetValue() != 0 {
		t.Fatal("transport probe feedback was not independently observable")
	}
}
