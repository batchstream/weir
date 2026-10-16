package app

import (
	"github.com/batchstream/weir/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

func (n *Node) registerMetrics() error {
	processOptions := collectors.ProcessCollectorOpts{}
	for _, collector := range []prometheus.Collector{
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(processOptions),
	} {
		if err := n.registry.Register(collector); err != nil {
			return err
		}
	}
	if err := n.registry.Register(n); err != nil {
		return err
	}
	if err := n.registry.Register(n.admission); err != nil {
		return err
	}

	for name, runtime := range n.stores {
		labels := prometheus.Labels{"store": name}
		if err := prometheus.WrapRegistererWith(labels, n.registry).Register(runtime); err != nil {
			return err
		}
	}

	if err := n.registry.Register(n.directory); err != nil {
		return err
	}

	for _, endpoint := range n.endpoints {
		name := "application"
		if endpoint.peer {
			name = "peer"
		}
		labels := prometheus.Labels{"listener": name}
		if err := prometheus.WrapRegistererWith(labels, n.registry).Register(endpoint.server); err != nil {
			return err
		}
	}

	return nil
}

func (n *Node) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(n, ch)
}

func (n *Node) Collect(ch chan<- prometheus.Metric) {
	n.mu.Lock()
	state := n.state
	n.mu.Unlock()

	desc := prometheus.NewDesc(
		"weir_node_state",
		"Node lifecycle. Readiness is independent of Store load or backend connectivity.",
		[]string{"state"},
		nil,
	)
	for _, label := range []string{"constructed", "serving", "failed", "draining", "closed"} {
		value := 0.0
		if state == label {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, label)
	}

	ready := 0.0
	if state == "serving" {
		ready = 1
	}
	desc = prometheus.NewDesc(
		"weir_node_ready",
		"Validated data listeners entered service and node is not draining or failed. Does not promise backend health.",
		nil,
		nil,
	)
	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, ready)

	reserved := store.ExecutionMemorySnapshot{}
	if n.executionMemory != nil {
		reserved = n.executionMemory.Snapshot()
	}
	executionKnown := 0.0
	if reserved.Known {
		executionKnown = 1
	}
	for name, value := range map[string]float64{
		"ingress_connection_bytes": float64(reserved.IngressConnectionBytes),
		"ingress_connections":      float64(reserved.IngressConnections),
		"ingress_bytes":            float64(reserved.IngressBytes),
		"ingress_reserved_bytes":   float64(reserved.IngressReservedBytes),
		"ingress_buffers":          float64(reserved.IngressBuffers),
		"ingress_messages":         float64(reserved.IngressMessages),
		"ingress_waiting":          float64(reserved.IngressWaiting),
		"budget_bytes":             float64(reserved.BudgetBytes),
		"used_bytes":               float64(reserved.UsedBytes),
		"resident_bytes":           float64(reserved.ResidentBytes),
		"wire_bytes":               float64(reserved.WireBytes),
		"wire_buffers":             float64(reserved.WireCount),
		"input_reserved_bytes":     float64(reserved.InputReservedBytes),
		"input_used_bytes":         float64(reserved.InputUsedBytes),
		"io_reserve_bytes":         float64(reserved.IOReserveBytes),
		"budget_known":             executionKnown,
		"leases":                   float64(reserved.Count),
		"leases_limit":             float64(reserved.CountLimit),
		"native_leases":            float64(reserved.NativeCount),
		"native_leases_limit":      float64(reserved.NativeCountLimit),
		"starting":                 float64(reserved.Starting),
		"settled":                  float64(reserved.Settled),
		"borrowed":                 float64(reserved.Borrowed),
		"waiting":                  float64(reserved.Waiting),
		"terminal":                 float64(reserved.Terminal),
	} {
		desc := prometheus.NewDesc(
			"weir_execution_memory_"+name,
			"Node-shared bounded input, execution, retained-result, encoded-response and native ingress reservations. Connection bytes are conservative parser/workspace promises, not measured RSS; RSS remains independently observed.",
			nil,
			nil,
		)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value)
	}
	executionRejections := prometheus.NewDesc("weir_execution_memory_budget_rejections_total", "Capacity denials at the shared execution memory boundary, including incomplete exchanges after dispatch; cancellations are excluded.", nil, nil)
	ch <- prometheus.MustNewConstMetric(executionRejections, prometheus.CounterValue, float64(reserved.BudgetRejections))

	memory := n.guard.Snapshot()
	observed, latched := 0.0, 0.0
	if memory.Observed {
		observed = 1
	}
	if memory.Latched {
		latched = 1
	}
	for name, value := range map[string]float64{
		"budget_bytes":    float64(memory.Budget),
		"sample_observed": observed,
		"latched":         latched,
	} {
		desc := prometheus.NewDesc(
			"weir_memory_"+name,
			"Existing overload Guard state, without additional sampling or backend calls.",
			nil,
			nil,
		)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value)
	}
	managed := 0.0
	if memory.GoRuntimeManaged {
		managed = 1
	}
	for name, value := range map[string]float64{
		"go_runtime_limit_bytes": float64(memory.GoRuntimeLimit),
		"go_runtime_managed":     managed,
	} {
		desc := prometheus.NewDesc(
			"weir_memory_"+name,
			"Last sampled Go soft memory limit (MaxInt64 means unlimited) and whether the CLI still owns an automatic finite limit; OS memory remains the admission boundary.",
			nil,
			nil,
		)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value)
	}
	known := 0.0
	if memory.RecoveryAllowanceKnown {
		known = 1
	}
	for name, value := range map[string]float64{
		"recovery_allowance_bytes": float64(memory.RecoveryAllowance),
		"recovery_allowance_known": known,
	} {
		desc := prometheus.NewDesc(
			"weir_memory_"+name,
			"Process resident allowance below all trusted recovery thresholds after accounting for memory outside process RSS; known distinguishes zero allowance from missing observations.",
			nil,
			nil,
		)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value)
	}

	desc = prometheus.NewDesc(
		"weir_memory_sample_bytes",
		"Last Guard sample by source; physical footprint and Go fallback are not RSS. Inactive sources are zero.",
		[]string{"source"},
		nil,
	)
	for _, source := range []string{"unobserved", "linux_rss", "darwin_phys_footprint", "go_sys_minus_released"} {
		value := 0.0
		if memory.Source == source {
			value = float64(memory.Bytes)
		}
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, source)
	}

	for name, value := range map[string]bool{
		"process_valid": memory.ProcessValid,
		"unknown":       memory.Unknown,
		"cgroup_finite": memory.Cgroup.Finite,
		"cgroup_valid":  memory.Cgroup.Valid,
	} {
		desc := prometheus.NewDesc(
			"weir_memory_"+name,
			"Guard observation validity; unknown observations alone do not latch overload; recovery requires complete observations.",
			nil,
			nil,
		)
		n := 0.0
		if value {
			n = 1
		}
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, n)
	}

	for name, value := range map[string]float64{
		"current_bytes":     float64(memory.Cgroup.Current),
		"working_set_bytes": float64(memory.Cgroup.WorkingSet),
		"limit_bytes":       float64(memory.Cgroup.Limit),
		"levels":            float64(memory.Cgroup.Levels),
	} {
		desc := prometheus.NewDesc(
			"weir_memory_cgroup_"+name,
			"Visible finite cgroup with greatest working-set/max pressure; current includes file cache, working set excludes validated inactive_file. Missing breakdown uses current. Unlimited reports leaf usage with finite=0. Values require cgroup_valid=1. Never added to process RSS.",
			nil,
			nil,
		)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value)
	}

	desc = prometheus.NewDesc(
		"weir_memory_cgroup_state",
		"Current visible cgroup profile, refreshed after complete observations; hidden ancestors are not observed.",
		[]string{"state"},
		nil,
	)
	for _, state := range []string{"not_applicable", "v2", "unknown"} {
		value := 0.0
		if memory.Cgroup.State == state {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, state)
	}

	desc = prometheus.NewDesc(
		"weir_memory_cgroup_scope",
		"Scope of the reported cgroup pair; no paths or dynamic labels.",
		[]string{"scope"},
		nil,
	)
	for _, scope := range []string{"none", "leaf", "ancestor"} {
		value := 0.0
		if memory.Cgroup.Scope == scope {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, scope)
	}

	n.drains.Collect(ch)
	n.drainDuration.Collect(ch)
}
