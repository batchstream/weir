package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
	"github.com/batchstream/weir/internal/overload"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

func Open(ctx context.Context, cfg Config) (*Node, error) {
	var err error
	cfg, err = cfg.Resolve()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	limits := cfg.Basic.Transport.serverLimits()
	admission := server.NewAdmission()
	node := &Node{
		shutdownTimeout: time.Duration(cfg.Basic.Lifecycle.ShutdownTimeout),
		admission:       admission,
		stores:          make(map[string]*store.Runtime, len(cfg.Routing.Stores)),
		Errors:          make(chan error, 3),
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
			cleanup, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Basic.Lifecycle.CleanupTimeout))
			defer cancel()
			_ = node.Close(cleanup)
		}
	}()

	for _, definition := range cfg.Routing.Stores {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		options := localOptions{definition: definition, lua: cfg.Basic.Lua.limits(), cleanup: time.Duration(cfg.Basic.Lifecycle.CleanupTimeout)}
		runtime, err := openLocal(ctx, options)
		if err != nil {
			return nil, err
		}
		overloadTargets = append(overloadTargets, runtime)
		node.stores[definition.Name] = runtime
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	node.listener, err = net.Listen("tcp", cfg.Basic.Listeners.Application)
	if err != nil {
		return nil, errors.New("listener startup failed")
	}
	options := server.Config{Stores: node.stores, Limits: limits, Admission: admission}
	node.server, err = server.New(options)
	if err != nil {
		return nil, err
	}

	node.guard = overload.New(overloadTargets, cfg.Basic.Overload.Memory.limits())
	if err := node.registerMetrics(); err != nil {
		return nil, err
	}
	if cfg.Basic.Diagnostics.Address != "" {
		if err := node.openDiagnostics(cfg.Basic.Diagnostics.Address, time.Duration(cfg.Basic.Diagnostics.Timeout)); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	complete = true
	return node, nil
}

type localOptions struct {
	definition StoreConfig
	lua        luaengine.Limits
	cleanup    time.Duration
}

func openLocal(ctx context.Context, options localOptions) (*store.Runtime, error) {
	name, cfg := options.definition.Name, options.definition.Local
	limits := cfg.runtimeLimits()
	limits.CleanupTimeout = options.cleanup
	var adapter execution.Adapter
	var err error
	if cfg.Backend.MongoDB != nil {
		config := cfg.mongoConfig(name)
		config.Options.Lua = options.lua
		config.Options.CleanupTimeout = options.cleanup
		adapter, err = mongodb.Open(ctx, config)
	} else {
		config := cfg.searchConfig(name)
		config.Options.Lua = options.lua
		config.Options.CleanupTimeout = options.cleanup
		adapter, err = search.Open(ctx, config)
	}
	if err != nil {
		// Both adapters redact addresses, credentials and backend response bodies
		// before returning startup errors. Keep their actionable qualification reason.
		return nil, fmt.Errorf("local Store %q startup qualification failed: %w", name, err)
	}

	return store.New(adapter, limits)
}
