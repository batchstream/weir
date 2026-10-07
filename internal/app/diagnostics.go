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
	opts := prometheus.CounterOpts{
		Name: "weir_diagnostic_rejections_total",
		Help: "Diagnostics-only connection, concurrency or request rejection.",
	}
	d := &diagnostics{
		listener:    listener,
		slots:       make(chan struct{}, diagnosticHandlers),
		connections: make(chan struct{}, diagnosticConnections),
		rejections:  prometheus.NewCounterVec(opts, []string{"reason"}),
		done:        make(chan struct{}),
	}
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
		ReadHeaderTimeout: diagnosticTimeout,
		ReadTimeout:       diagnosticTimeout,
		WriteTimeout:      diagnosticTimeout,
		IdleTimeout:       diagnosticTimeout,
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
		"connections":       len(d.connections),
		"connections_limit": cap(d.connections),
		"handlers":          len(d.slots),
		"handlers_limit":    cap(d.slots),
	}
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
	c.once.Do(func() {
		c.err = c.Conn.Close()
		<-c.owner.connections
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
