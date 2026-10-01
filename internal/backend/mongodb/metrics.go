package mongodb

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

type connectionSnapshot struct {
	Owned, Peak, Limit, Dialing, Closing int
	Acquired, Released                   uint64
}

func (d *boundedDialer) snapshot() connectionSnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := connectionSnapshot{Owned: d.owned, Peak: d.peak, Limit: d.limit, Dialing: d.dialing,
		Closing: d.closing, Acquired: d.acquired, Released: d.released}
	return s
}

func (a *Adapter) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(a, ch) }

func (a *Adapter) Collect(ch chan<- prometheus.Metric) {
	s := a.dialer.snapshot()
	values := map[string]float64{"owned": float64(s.Owned), "peak": float64(s.Peak), "limit": float64(s.Limit),
		"dialing": float64(s.Dialing), "closing": float64(s.Closing), "acquired": float64(s.Acquired), "released": float64(s.Released)}
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
	s := a.dialer.snapshot()
	slog.Info("backend_connections_closed", "backend", "mongo", "store", a.config.Store,
		"owned", s.Owned, "peak", s.Peak, "limit", s.Limit, "dialing", s.Dialing,
		"closing", s.Closing, "acquired", s.Acquired, "released", s.Released)
}
