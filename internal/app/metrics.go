package app

import (
	"github.com/prometheus/client_golang/prometheus"
)

func (n *Node) registerMetrics(cfg Config) error {
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
		if cfg.Application == "" || i == 1 {
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
	desc = prometheus.NewDesc("weir_memory_sample_bytes", "Last Guard sample by source; Go fallback is not RSS. Inactive sources are zero.", []string{"source"}, nil)
	for _, source := range []string{"unobserved", "linux_rss", "go_sys_minus_released"} {
		value := 0.0
		if memory.Source == source {
			value = float64(memory.Bytes)
		}
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, source)
	}
	n.drains.Collect(ch)
	n.drainDuration.Collect(ch)
}
