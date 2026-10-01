//go:build integration

package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/batchstream/weir/internal/app"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// This experimental adapter retains the real database, batch scheduler, queues,
// connection pool, and additive growth. Only confirmed congestion feedback is
// suppressed, isolating its effect without a production configuration switch.
type feedbackControl struct {
	*search.Adapter
	suppressed prometheus.Counter
}

func (a *feedbackControl) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) execution.Feedback {
	feedback := a.Adapter.Execute(ctx, plans, emit)
	if feedback == execution.Congested {
		a.suppressed.Inc()
		feedback = execution.Neutral
	}
	return feedback
}

func serveControl(ctx context.Context, config, routes string, suppress bool) error {
	cfg, err := app.Load(config, routes)
	if err != nil {
		return err
	}
	if len(cfg.Routing.Services) != 1 ||
		cfg.Routing.Services[0].Local == nil ||
		cfg.Routing.Services[0].Local.Search == nil ||
		len(cfg.Routing.Routes) != 1 ||
		cfg.Routing.Routes[0].Store != "records" {
		return errors.New("control server requires one owned search Store")
	}
	local := cfg.Routing.Services[0].Local
	limits := store.DefaultLimits()
	if local.MaxConcurrency != 0 {
		limits.Concurrency = local.MaxConcurrency
	}
	if local.MaxBatchOperations != 0 {
		limits.BatchOperations = local.MaxBatchOperations
	}
	backend := local.Search
	var connection *search.Connection
	if configured := backend.Connection; configured != nil {
		connection = &search.Connection{
			Username: configured.Username,
			Password: configured.Password,
			CAFile:   configured.CAFile,
		}
	}
	options := search.Config{
		Store:      "records",
		URL:        backend.URL,
		Pool:       limits.Concurrency,
		Connection: connection,
	}
	a, err := search.Open(ctx, options)
	if err != nil {
		return err
	}
	registry := prometheus.NewRegistry()
	var adapter execution.Adapter = a
	if suppress {
		metric := prometheus.CounterOpts{
			Name: "weir_test_suppressed_congestion_total",
			Help: "Explicit real database congestion samples suppressed by the experimental control.",
		}
		counter := prometheus.NewCounter(metric)
		registry.MustRegister(counter)
		control := &feedbackControl{Adapter: a, suppressed: counter}
		adapter = control
	}
	runtime, err := store.New(adapter, limits)
	if err != nil {
		return err
	}
	defer func() {
		drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = runtime.Close(drain)
	}()
	transport := server.DefaultLimits()
	transport.Sessions = cfg.Basic.Transport.MaxSessions
	transport.Connections = cfg.Basic.Transport.MaxConnections
	transport.RouteLifetime = time.Duration(cfg.Basic.Transport.Timeouts.Route)
	transport.Stall = time.Duration(cfg.Basic.Transport.Timeouts.Stall)
	admission, err := server.NewAdmission(transport)
	if err != nil {
		return err
	}
	service := server.Service{LocalStore: runtime}
	optionsServer := server.Config{Routes: map[string]server.Service{"records": service}, Limits: transport, Admission: admission}
	s, err := server.New(optionsServer)
	if err != nil {
		return err
	}
	processOptions := prometheus.ProcessCollectorOpts{}
	registry.MustRegister(runtime, s, prometheus.NewGoCollector(), prometheus.NewProcessCollector(processOptions))
	listener, err := net.Listen("tcp", cfg.Basic.Listeners.Application)
	if err != nil {
		return err
	}
	defer listener.Close()
	diagnostics, err := net.Listen("tcp", cfg.Basic.Diagnostics.Address)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	handlerOptions := promhttp.HandlerOpts{}
	mux.Handle("/metrics", promhttp.HandlerFor(registry, handlerOptions))
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	defer httpServer.Close()
	go func() { _ = httpServer.Serve(diagnostics) }()
	done := make(chan error, 1)
	go func() { done <- s.Serve(listener) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.Shutdown(drain)
	}
}
