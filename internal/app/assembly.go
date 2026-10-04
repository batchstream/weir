package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/directory"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

func Open(ctx context.Context, cfg Config) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	limits := cfg.Basic.Transport.serverLimits()
	admission, err := server.NewAdmission(limits)
	if err != nil {
		return nil, err
	}
	node := &Node{
		admission: admission,
		Errors:    make(chan error, 3),
		state:     "constructed",
		registry:  prometheus.NewRegistry(),
	}
	overloadTargets := []overload.Target{admission}

	drainOpts := prometheus.CounterOpts{
		Name: "weir_node_drains_total",
		Help: "First Close calls, including partial startup cleanup.",
	}
	durationOpts := prometheus.HistogramOpts{
		Name:    "weir_node_drain_seconds",
		Help:    "Node data drain and owned backend/directory cleanup duration.",
		Buckets: []float64{.01, .1, 1, 5, 10},
	}
	node.drains = prometheus.NewCounter(drainOpts)
	node.drainDuration = prometheus.NewHistogram(durationOpts)

	complete := false
	defer func() {
		if !complete {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = node.Close(cleanup)
		}
	}()

	stores := make(map[string]*store.Runtime)
	for _, definition := range cfg.Routing.Stores {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		runtime, err := openLocal(ctx, definition.Name, definition.Local)
		if err != nil {
			return nil, err
		}
		node.runtimes = append(node.runtimes, runtime)
		node.localNames = append(node.localNames, definition.Name)
		overloadTargets = append(overloadTargets, runtime)
		stores[definition.Name] = runtime
	}

	applicationAddress, peerAddress := "", ""
	for i, address := range []string{cfg.Basic.Listeners.Application, cfg.Basic.Listeners.Peer} {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if address == "" {
			continue
		}
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return nil, errors.New("listener startup failed")
		}
		node.listeners = append(node.listeners, listener)
		if i == 0 {
			applicationAddress = listener.Addr().String()
		} else {
			peerAddress = listener.Addr().String()
		}
	}

	discovery := cfg.Basic.Discovery
	if discovery.PeerAddress != "" {
		peerAddress = discovery.PeerAddress
	}
	targets := discovery.Advertise
	if len(targets) == 0 && len(node.localNames) != 0 {
		targets = []string{applicationAddress}
	}
	directoryConfig := directory.Config{
		Group:       discovery.Group,
		PeerAddress: peerAddress,
		Targets:     targets,
		Stores:      node.localNames,
		Seeds:       discovery.Seeds,
	}
	node.directory, err = directory.New(directoryConfig)
	if err != nil {
		return nil, err
	}

	for i, address := range []string{cfg.Basic.Listeners.Application, cfg.Basic.Listeners.Peer} {
		if address == "" {
			continue
		}
		options := server.Config{
			Stores:    stores,
			Directory: node.directory,
			Limits:    limits,
			Admission: admission,
			Peer:      i == 1,
		}
		listenerServer, err := server.New(options)
		if err != nil {
			return nil, err
		}
		node.servers = append(node.servers, listenerServer)
	}

	node.guard = overload.New(overloadTargets, uint64(cfg.Basic.Memory))
	if err := node.registerMetrics(cfg); err != nil {
		return nil, err
	}
	if cfg.Basic.Diagnostics.Address != "" {
		if err := node.openDiagnostics(cfg.Basic.Diagnostics.Address); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	complete = true
	return node, nil
}

func openLocal(ctx context.Context, name string, cfg *Local) (*store.Runtime, error) {
	limits := cfg.runtimeLimits()
	var adapter execution.Adapter
	var err error
	if cfg.MongoDB != nil {
		config := cfg.mongoConfig(name)
		adapter, err = mongodb.Open(ctx, config)
	} else {
		config := cfg.searchConfig(name)
		adapter, err = search.Open(ctx, config)
	}
	if err != nil {
		// Both adapters redact addresses, credentials and backend response bodies
		// before returning startup errors. Keep their actionable qualification reason.
		return nil, fmt.Errorf("local Store %q startup qualification failed: %w", name, err)
	}

	return store.New(adapter, limits)
}
