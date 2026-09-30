package app

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

func (n *Node) registerMetrics(cfg Config) error {
	processOptions := collectors.ProcessCollectorOpts{}
	for _, collector := range []prometheus.Collector{collectors.NewGoCollector(), collectors.NewProcessCollector(processOptions)} {
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
	for i, runtime := range n.runtimes {
		labels := prometheus.Labels{"store": n.localNames[i]}
		if err := prometheus.WrapRegistererWith(labels, n.registry).Register(runtime); err != nil {
			return err
		}
	}
	for i, remote := range n.remotes {
		labels := prometheus.Labels{"service": n.remoteNames[i]}
		if err := prometheus.WrapRegistererWith(labels, n.registry).Register(remote); err != nil {
			return err
		}
	}
	for i, srv := range n.servers {
		name := "application"
		if cfg.Basic.Listeners.Application == "" || i == 1 {
			name = "peer"
		}
		labels := prometheus.Labels{"listener": name}
		if err := prometheus.WrapRegistererWith(labels, n.registry).Register(srv); err != nil {
			return err
		}
	}
	return nil
}
func (n *Node) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(n, ch) }
func (n *Node) Collect(ch chan<- prometheus.Metric) {
	n.mu.Lock()
	state := n.state
	n.mu.Unlock()
	desc := prometheus.NewDesc("weir_node_state", "Node lifecycle. Readiness is independent of Store load or backend connectivity.", []string{"state"}, nil)
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
	desc = prometheus.NewDesc("weir_node_ready", "Validated data listeners entered service and node is not draining or failed. Does not promise backend health.", nil, nil)
	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, ready)
	memory := n.guard.Snapshot()
	observed, latched := 0.0, 0.0
	if memory.Observed {
		observed = 1
	}
	if memory.Latched {
		latched = 1
	}
	for name, value := range map[string]float64{"budget_bytes": float64(memory.Budget), "sample_observed": observed, "latched": latched} {
		desc := prometheus.NewDesc("weir_memory_"+name, "Existing overload Guard state, without additional sampling or backend calls.", nil, nil)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value)
	}
	desc = prometheus.NewDesc("weir_memory_sample_bytes", "Last Guard sample by source; physical footprint and Go fallback are not RSS. Inactive sources are zero.", []string{"source"}, nil)
	for _, source := range []string{"unobserved", "linux_rss", "darwin_phys_footprint", "go_sys_minus_released"} {
		value := 0.0
		if memory.Source == source {
			value = float64(memory.Bytes)
		}
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, source)
	}
	for name, value := range map[string]bool{"process_valid": memory.ProcessValid, "unknown": memory.Unknown, "cgroup_finite": memory.Cgroup.Finite, "cgroup_valid": memory.Cgroup.Valid} {
		desc := prometheus.NewDesc("weir_memory_"+name, "Guard observation validity; unknown observations close new admission.", nil, nil)
		n := 0.0
		if value {
			n = 1
		}
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, n)
	}
	for name, value := range map[string]float64{"current_bytes": float64(memory.Cgroup.Current), "limit_bytes": float64(memory.Cgroup.Limit), "levels": float64(memory.Cgroup.Levels)} {
		desc := prometheus.NewDesc("weir_memory_cgroup_"+name, "Most pressured visible finite cgroup current/max pair; unlimited reports leaf current with finite=0. Values require cgroup_valid=1. Never added to process RSS.", nil, nil)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value)
	}
	desc = prometheus.NewDesc("weir_memory_cgroup_state", "Static visible cgroup profile. Changed profiles require restart; hidden ancestors are not observed.", []string{"state"}, nil)
	for _, state := range []string{"not_applicable", "v2", "unknown", "profile_changed"} {
		value := 0.0
		if memory.Cgroup.State == state {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, state)
	}
	desc = prometheus.NewDesc("weir_memory_cgroup_scope", "Scope of the reported cgroup pair; no paths or dynamic labels.", []string{"scope"}, nil)
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
