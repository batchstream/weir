//go:build integration

package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
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

func (a *feedbackControl) Execute(ctx context.Context, plans []*execution.Plan) ([]*pb.BulkResult, execution.Feedback) {
	results, feedback := a.Adapter.Execute(ctx, plans)
	if feedback == execution.Congested {
		a.suppressed.Inc()
		feedback = execution.Neutral
	}
	return results, feedback
}

func serveControl(ctx context.Context, config string, suppress bool) error {
	f, err := os.Open(config)
	if err != nil {
		return err
	}
	cfg, err := app.Decode(f)
	_ = f.Close()
	if err != nil {
		return err
	}
	if len(cfg.Services) != 1 || cfg.Services[0].Local == nil || cfg.Services[0].Local.Search == nil || len(cfg.Routes) != 1 || cfg.Routes[0].Store != "records" {
		return errors.New("control server requires one owned search Store")
	}
	local := cfg.Services[0].Local
	limits := store.DefaultLimits()
	if local.Concurrency != 0 {
		limits.Concurrency = local.Concurrency
	}
	if local.BatchOperations != 0 {
		limits.BatchOperations = local.BatchOperations
	}
	backend := local.Search
	options := search.Config{Store: "records", URL: backend.URL, Index: backend.Index, Profile: backend.Profile, Pool: limits.Concurrency, Connection: backend.Connection}
	a, err := search.Open(ctx, options)
	if err != nil {
		return err
	}
	registry := prometheus.NewRegistry()
	var adapter execution.Adapter = a
	if suppress {
		metric := prometheus.CounterOpts{Name: "weir_test_suppressed_congestion_total", Help: "Explicit real database congestion samples suppressed by the experimental control."}
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
	transport.Sessions = cfg.Limits.Sessions
	transport.Connections = cfg.Limits.Connections
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
	listener, err := net.Listen("tcp", cfg.Application)
	if err != nil {
		return err
	}
	defer listener.Close()
	diagnostics, err := net.Listen("tcp", cfg.Diagnostics)
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
