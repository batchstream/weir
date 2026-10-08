package app

import (
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTP/1 only, one request per connection, with read/write progress deadlines.
// Each handler owns its gather until it returns; there is no detached gather or
// handler concurrency gate.
type diagnostics struct {
	listener    net.Listener
	http        *http.Server
	handlers    atomic.Int64
	connections atomic.Int64
	rejections  *prometheus.CounterVec
	done        chan struct{}
}

const diagnosticTimeout = time.Second

func (n *Node) openDiagnostics(address string, timeout time.Duration) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return errors.New("diagnostic listener startup failed")
	}
	opts := prometheus.CounterOpts{
		Name: "weir_diagnostic_rejections_total",
		Help: "Invalid diagnostic requests.",
	}
	d := &diagnostics{
		listener:   listener,
		rejections: prometheus.NewCounterVec(opts, []string{"reason"}),
		done:       make(chan struct{}),
	}
	n.diagnostics = d
	for _, reason := range []string{"request"} {
		d.rejections.WithLabelValues(reason)
	}
	if err := n.registry.Register(d); err != nil {
		return err
	}

	options := promhttp.HandlerOpts{DisableCompression: true}
	metrics := promhttp.HandlerFor(n.registry, options)
	handler := func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Connection", "close")
		d.handlers.Add(1)
		defer d.handlers.Add(-1)
		if request.Method != http.MethodGet || request.ContentLength != 0 ||
			len(request.TransferEncoding) != 0 || request.URL.RawQuery != "" {
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
	d.http = &http.Server{
		Handler:           http.HandlerFunc(handler),
		Protocols:         protocols,
		ReadHeaderTimeout: timeout,
		ReadTimeout:       timeout,
		WriteTimeout:      timeout,
		IdleTimeout:       timeout,
		MaxHeaderBytes:    4 << 10,
	}
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

func (d *diagnostics) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(d, ch)
}

func (d *diagnostics) Collect(ch chan<- prometheus.Metric) {
	values := map[string]int{
		"connections": int(d.connections.Load()),
		"handlers":    int(d.handlers.Load()),
	}
	for name, value := range values {
		desc := prometheus.NewDesc("weir_diagnostic_"+name, "Independent diagnostic connection and handler occupancy.", nil, nil)
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
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.owner.connections.Add(1)
	connection := &diagnosticConn{Conn: conn, owner: l.owner}
	return connection, nil
}

func (c *diagnosticConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		c.owner.connections.Add(-1)
	})
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
	if n.stopGuard != nil {
		<-d.done
	}
}
