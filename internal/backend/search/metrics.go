package search

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

func (a *Adapter) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(a, ch) }

func (a *Adapter) Collect(ch chan<- prometheus.Metric) {
	d := a.dialer
	d.mu.Lock()
	values := map[string]float64{
		"owned":    float64(d.owned),
		"peak":     float64(d.peak),
		"acquired": float64(d.acquired),
		"released": float64(d.released),
	}
	d.mu.Unlock()
	for name, value := range values {
		desc := prometheus.NewDesc(
			"weir_backend_connections_"+name,
			"Local backend connection ownership from DNS to raw Close completion; excludes remote tails and separate OCSP I/O.",
			nil,
			nil,
		)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value)
	}
}

func (a *Adapter) logConnections() {
	d := a.dialer
	d.mu.Lock()
	defer d.mu.Unlock()
	slog.Info(
		"backend_connections_closed",
		"backend",
		"search",
		"store",
		a.config.Store,
		"owned",
		d.owned,
		"peak",
		d.peak,
		"acquired",
		d.acquired,
		"released",
		d.released,
	)
}
