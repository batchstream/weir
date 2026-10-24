package app

import (
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

	return n.registry.Register(n.server)
}

func (n *Node) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(n, ch)
}

func (n *Node) Collect(ch chan<- prometheus.Metric) {
	ready := 0.0
	if n.ready() {
		ready = 1
	}
	desc := prometheus.NewDesc(
		"weir_node_ready",
		"The application listener entered service and the node is not draining or failed. Does not promise backend health.",
		nil,
		nil,
	)
	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, ready)

	memory := n.guard.Snapshot()
	overloaded := 0.0
	if memory.Overloaded {
		overloaded = 1
	}
	for _, gauge := range []struct {
		name, help string
		value      float64
	}{
		{"used_bytes", "Memory the Go runtime holds from the OS at the last sample.", float64(memory.Used)},
		{"limit_bytes", "Go memory limit the overload guard compares with; zero means none.", float64(memory.Limit)},
		{"overloaded", "Whether memory pressure is refusing new work.", overloaded},
	} {
		desc := prometheus.NewDesc("weir_memory_"+gauge.name, gauge.help, nil, nil)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, gauge.value)
	}
}
