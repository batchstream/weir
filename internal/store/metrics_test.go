package store

import (
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	dto "github.com/prometheus/client_model/go"
)

func counter(families map[string]*dto.MetricFamily, name string, labels map[string]string) float64 {
	family := families[name]
	if family == nil {
		return -1
	}
	for _, metric := range family.Metric {
		matched := true
		for _, pair := range metric.Label {
			if value, ok := labels[pair.GetName()]; ok && value != pair.GetValue() {
				matched = false
			}
		}
		if matched {
			if metric.Counter != nil {
				return metric.Counter.GetValue()
			}
			return metric.Gauge.GetValue()
		}
	}
	return -1
}

func TestMetricsCountFinalOutcomes(t *testing.T) {
	adapter := &fakeAdapter{}
	runtime := open(t, adapter, limits(func(l *Limits) { l.MaxInFlight = 3 }))
	wait(t, submit(t, t.Context(), runtime, readPlan("r"), nil))
	wait(t, submit(t, t.Context(), runtime, mutationPlan("m"), nil))
	emit := func(*pb.Event) error { return nil }
	native, failure := runtime.SubmitNative(t.Context(), nativePlan(), emit)
	if failure != nil {
		t.Fatal(failure)
	}
	wait(t, native)
	runtime.SetOverloaded(true)
	if _, failure := runtime.Submit(t.Context(), readPlan("x"), nil); failure == nil {
		t.Fatal("overloaded runtime admitted work")
	}
	families := testmetrics.Gather(t, runtime)
	checks := []struct {
		name   string
		labels map[string]string
		value  float64
	}{
		{"weir_store_records_total", map[string]string{"operation": "read", "outcome": "success"}, 1},
		{"weir_store_records_total", map[string]string{"operation": "mutate", "outcome": "applied"}, 1},
		{"weir_store_native_completions_total", map[string]string{"completion": "response_complete"}, 1},
		{"weir_store_rejections_total", map[string]string{"reason": "overload"}, 1},
		{"weir_store_executions_total", nil, 3},
		{"weir_store_active_executions_limit", nil, 3},
		{"weir_store_overloaded", nil, 1},
		{"weir_store_queued_operations", nil, 0},
	}
	for _, check := range checks {
		if got := counter(families, check.name, check.labels); got != check.value {
			t.Fatal(check.name, check.labels, got)
		}
	}
}
