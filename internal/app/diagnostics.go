package app

import (
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const diagnosticConnections = 4
const diagnosticHandlers = 2
const diagnosticTimeout = time.Second

// HTTP/1 only, one request per connection. A stalled connection never retains a
// handler slot beyond the read/write deadline. No timeout-handler goroutine or
// detached gather is used: the bounded handler owns its gather until it returns.
type diagnostics struct {
	listener    net.Listener
	http        *http.Server
	slots       chan struct{}
	connections chan struct{}
	rejections  *prometheus.CounterVec
	done        chan struct{}
}

func (n *Node) openDiagnostics(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return errors.New("diagnostic listener startup failed")
	}
	opts := prometheus.CounterOpts{Name: "weir_diagnostic_rejections_total", Help: "Diagnostics-only connection, concurrency or request rejection."}
	d := &diagnostics{listener: listener, slots: make(chan struct{}, diagnosticHandlers), connections: make(chan struct{}, diagnosticConnections), rejections: prometheus.NewCounterVec(opts, []string{"reason"}), done: make(chan struct{})}
	n.diagnostics = d
	for _, reason := range []string{"connections", "handlers", "request"} {
		d.rejections.WithLabelValues(reason)
	}
	if err := n.registry.Register(d); err != nil {
		return err
	}
	options := promhttp.HandlerOpts{DisableCompression: true}
	metrics := promhttp.HandlerFor(n.registry, options)
	handler := func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Connection", "close")
		select {
		case d.slots <- struct{}{}:
			defer func() { <-d.slots }()
		default:
			d.rejections.WithLabelValues("handlers").Inc()
			http.Error(w, "diagnostics busy", http.StatusServiceUnavailable)
			return
		}
		if request.Method != http.MethodGet || request.ContentLength != 0 || len(request.TransferEncoding) != 0 || request.URL.RawQuery != "" {
			d.rejections.WithLabelValues("request").Inc()
			http.Error(w, "invalid diagnostic request", http.StatusBadRequest)
			return
		}
		switch request.URL.Path {
		case "/livez":
			_, _ = io.WriteString(w, "ok\n")
		case "/readyz":
			if !n.ready() {
				http.Error(w, "not ready", http.StatusServiceUnavailable)
				return
			}
			_, _ = io.WriteString(w, "ready\n")
		case "/metrics":
			metrics.ServeHTTP(w, request)
		default:
			d.rejections.WithLabelValues("request").Inc()
			http.Error(w, "not found", http.StatusNotFound)
		}
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	d.http = &http.Server{Handler: http.HandlerFunc(handler), Protocols: protocols, ReadHeaderTimeout: diagnosticTimeout, ReadTimeout: diagnosticTimeout, WriteTimeout: diagnosticTimeout, IdleTimeout: diagnosticTimeout, MaxHeaderBytes: 4 << 10}
	d.http.SetKeepAlivesEnabled(false)
	return nil
}
func (d *diagnostics) serve() error {
	defer close(d.done)
	listener := &diagnosticListener{Listener: d.listener, owner: d}
	err := d.http.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func (n *Node) DiagnosticAddress() string {
	if n.diagnostics == nil {
		return ""
	}
	return n.diagnostics.listener.Addr().String()
}
func (n *Node) ready() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == "serving"
}
func (n *Node) listenerEnded(err error) {
	n.mu.Lock()
	if !n.closed {
		n.state = "failed"
	}
	n.mu.Unlock()
	n.Errors <- err
}
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
func (d *diagnostics) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(d, ch) }
func (d *diagnostics) Collect(ch chan<- prometheus.Metric) {
	values := map[string]int{"connections": len(d.connections), "connections_limit": cap(d.connections), "handlers": len(d.slots), "handlers_limit": cap(d.slots)}
	for name, value := range values {
		desc := prometheus.NewDesc("weir_diagnostic_"+name, "Independent diagnostic occupancy or limit.", nil, nil)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(value))
	}
	d.rejections.Collect(ch)
}

type diagnosticListener struct {
	net.Listener
	owner *diagnostics
}
type diagnosticConn struct {
	net.Conn
	owner *diagnostics
	once  sync.Once
	err   error
}

func (l *diagnosticListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.owner.connections <- struct{}{}:
			bounded := &diagnosticConn{Conn: conn, owner: l.owner}
			return bounded, nil
		default:
			l.owner.rejections.WithLabelValues("connections").Inc()
			_ = conn.Close()
		}
	}
}
func (c *diagnosticConn) Close() error {
	c.once.Do(func() { c.err = c.Conn.Close(); <-c.owner.connections })
	return c.err
}
// Data drain retains diagnostics until all owned runtimes and remote clients
// have closed. Shutdown of diagnostics itself has no extra grace period.
func (n *Node) closeDiagnostics() {
	if n.diagnostics == nil {
		return
	}
	d := n.diagnostics
	if d.http != nil {
		_ = d.http.Close()
	}
	_ = d.listener.Close()
	if n.started {
		<-d.done
	}
}
