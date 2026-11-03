package app

import (
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTP/1 only, one request per connection, with read/write progress deadlines.
type diagnostics struct {
	listener net.Listener
	http     *http.Server
}

const diagnosticTimeout = time.Second

func (n *Node) openDiagnostics(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return errors.New("diagnostic listener startup failed")
	}
	d := &diagnostics{listener: listener}
	n.diagnostics = d

	options := promhttp.HandlerOpts{DisableCompression: true}
	routes := http.NewServeMux()
	routes.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	routes.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !n.serving.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ready\n")
	})
	routes.Handle("GET /metrics", promhttp.HandlerFor(n.registry, options))

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	d.http = &http.Server{
		Handler:           routes,
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
	err := d.http.Serve(d.listener)
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

func (d *diagnostics) close() {
	_ = d.http.Close()
	_ = d.listener.Close()
}
