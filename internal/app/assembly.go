package app

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

// Open validates the configuration and assembles a node without serving.
func Open(ctx context.Context, cfg Config) (*Node, error) {
	return open(ctx, cfg, server.DefaultLimits())
}

// open takes the transport limits separately so package tests can shorten them.
func open(ctx context.Context, cfg Config, transport server.Limits) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	admission := server.NewAdmission()
	node := &Node{
		shutdownTimeout: cfg.shutdownTimeout(),
		admission:       admission,
		stores:          make(map[string]*store.Runtime, len(cfg.Stores)),
		Errors:          make(chan error, 2),
		state:           "constructed",
		registry:        prometheus.NewRegistry(),
	}
	overloadTargets := []overload.Target{admission}

	drainOpts := prometheus.CounterOpts{
		Name: "weir_node_drains_total",
		Help: "First Close calls, including partial startup cleanup.",
	}
	durationOpts := prometheus.HistogramOpts{
		Name:    "weir_node_drain_seconds",
		Help:    "Node data drain and owned backend cleanup duration.",
		Buckets: []float64{.01, .1, 1, 5, 10},
	}
	node.drains = prometheus.NewCounter(drainOpts)
	node.drainDuration = prometheus.NewHistogram(durationOpts)

	complete := false
	defer func() {
		if !complete {
			cleanup, cancel := context.WithTimeout(context.Background(), store.DefaultLimits().CleanupTimeout)
			defer cancel()
			_ = node.Close(cleanup)
		}
	}()

	for _, definition := range cfg.Stores {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		runtime, err := openLocal(ctx, definition)
		if err != nil {
			return nil, err
		}
		overloadTargets = append(overloadTargets, runtime)
		node.stores[definition.Name] = runtime
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var err error
	node.listener, err = net.Listen("tcp", cfg.Listeners.Application)
	if err != nil {
		return nil, errors.New("listener startup failed")
	}
	options := server.Config{Stores: node.stores, Limits: transport, Admission: admission}
	node.server, err = server.New(options)
	if err != nil {
		return nil, err
	}

	node.guard = overload.New(overloadTargets, overload.DefaultLimits())
	if err := node.registerMetrics(); err != nil {
		return nil, err
	}
	if cfg.Listeners.Diagnostics != "" {
		if err := node.openDiagnostics(cfg.Listeners.Diagnostics); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	complete = true
	return node, nil
}

func openLocal(ctx context.Context, definition StoreConfig) (*store.Runtime, error) {
	name := definition.Name
	var adapter execution.Adapter
	var err error
	if definition.MongoDB != nil {
		adapter, err = mongodb.Open(ctx, definition.mongoConfig())
	} else {
		adapter, err = search.Open(ctx, definition.searchConfig())
	}
	if err != nil {
		// Both adapters redact addresses, credentials and backend response bodies
		// before returning startup errors. Keep their actionable qualification reason.
		return nil, fmt.Errorf("local Store %q startup qualification failed: %w", name, err)
	}

	return store.New(adapter, definition.runtimeLimits())
}
