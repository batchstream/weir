//go:build integration

package app

import (
	"math"
	"strings"
	"testing"

	"github.com/batchstream/weir/internal/testutil/testmetrics"
	dto "github.com/prometheus/client_model/go"
)

const qualificationIngressConnectionBytes = 512 << 10

func qualificationGauge(t *testing.T, metrics map[string]*dto.MetricFamily, name string, labels map[string]string) float64 {
	t.Helper()
	metric := testmetrics.Sample(metrics, name, labels)
	if metric == nil || metric.Gauge == nil || len(metric.Label) != len(labels) || labels == nil && len(metrics[name].Metric) != 1 {
		t.Fatal("required qualification gauge is missing or has unexpected labels", name, labels)
	}
	value := metric.GetGauge().GetValue()
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		t.Fatal("qualification gauge is not a finite nonnegative value", name, value)
	}
	return value
}

func qualificationExecutionMemory(t *testing.T, metrics map[string]*dto.MetricFamily, capacity float64) map[string]float64 {
	t.Helper()
	values := make(map[string]float64, 26)
	for _, name := range []string{"budget_bytes", "used_bytes", "resident_bytes", "input_reserved_bytes", "input_used_bytes", "wire_bytes", "wire_buffers", "io_reserve_bytes", "budget_known", "leases", "leases_limit", "native_leases", "native_leases_limit", "starting", "settled", "borrowed", "waiting", "terminal", "ingress_bytes", "ingress_reserved_bytes", "ingress_buffers", "ingress_messages", "ingress_waiting", "ingress_connections", "ingress_connection_bytes"} {
		values[name] = qualificationGauge(t, metrics, "weir_execution_memory_"+name, nil)
	}
	denials := metrics["weir_execution_memory_budget_rejections_total"].GetMetric()
	if len(denials) != 1 || denials[0].Counter == nil || len(denials[0].Label) != 0 {
		t.Fatal("Node execution memory denial counter is missing or labeled")
	}
	values["budget_rejections_total"] = denials[0].GetCounter().GetValue()
	if value := values["budget_rejections_total"]; math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		t.Fatal("Node execution memory denial counter is not finite and nonnegative", value)
	}
	families := 0
	for name := range metrics {
		if strings.HasPrefix(name, "weir_execution_memory_") {
			families++
		}
	}
	if families != 26 || values["budget_known"] != 1 || values["leases_limit"] != 32768 || values["native_leases_limit"] != 1024 || values["budget_bytes"] <= 0 || values["budget_bytes"] > capacity*.70*2/3 {
		t.Fatal("default shared execution memory policy is missing or changed", families, values)
	}
	if values["ingress_bytes"] > values["ingress_reserved_bytes"] || values["ingress_reserved_bytes"]+values["ingress_connection_bytes"] > values["resident_bytes"] || values["ingress_connection_bytes"] != values["ingress_connections"]*qualificationIngressConnectionBytes {
		t.Fatal("Node ingress/connection ownership exceeded the shared memory ledger", values)
	}
	states := values["starting"] + values["settled"] + values["borrowed"] + values["waiting"] + values["terminal"]
	if values["used_bytes"] > values["budget_bytes"] || values["resident_bytes"] > values["used_bytes"] || values["input_used_bytes"] > values["input_reserved_bytes"] || values["input_reserved_bytes"] > values["resident_bytes"] || values["wire_bytes"] > values["used_bytes"] || values["wire_buffers"] > values["leases"] || values["leases"] > values["leases_limit"] || values["native_leases"] > values["native_leases_limit"] || values["native_leases"] > values["leases"] || states != values["leases"] {
		t.Fatal("Node execution memory ledger exceeded its bounded ownership", values)
	}
	return values
}
