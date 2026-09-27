package app

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
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
	limits := cfg.Limits.serverLimits()
	admission, err := server.NewAdmission(limits)
	if err != nil {
		return nil, err
	}
	node := &Node{admission: admission, budget: cfg.MemoryMiB << 20, Errors: make(chan error, 3), state: "constructed", registry: prometheus.NewRegistry()}
	node.targets = append(node.targets, admission)
	drainOpts := prometheus.CounterOpts{Name: "weir_node_drains_total", Help: "First Close calls, including partial startup cleanup."}
	durationOpts := prometheus.HistogramOpts{Name: "weir_node_drain_seconds", Help: "Node data drain and owned backend/remote cleanup duration.", Buckets: []float64{.01, .1, 1, 5, 10}}
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
	services := make(map[string]server.Service)
	for _, definition := range cfg.Services {
		var service server.Service
		if definition.Remote != nil {
			remoteConfig := server.RemoteConfig{Endpoints: definition.Remote.Endpoints, Relays: definition.Remote.Relays}
			service.RemoteWeir, err = server.NewRemote(remoteConfig)
			if err != nil {
				return nil, err
			}
			node.remotes = append(node.remotes, service.RemoteWeir)
			node.remoteNames = append(node.remoteNames, definition.Name)
		} else {
			var name string
			for _, route := range cfg.Routes {
				if route.Service == definition.Name {
					name = route.Store
					break
				}
			}
			service.LocalStore, err = openLocal(ctx, name, definition.Local)
			if err != nil {
				return nil, err
			}
			node.runtimes = append(node.runtimes, service.LocalStore)
			node.localNames = append(node.localNames, name)
			node.targets = append(node.targets, service.LocalStore)
		}
		services[definition.Name] = service
	}
	routes := make(map[string]server.Service)
	for _, route := range cfg.Routes {
		routes[route.Store] = services[route.Service]
	}
	for i, address := range []string{cfg.Application, cfg.Peer} {
		if address == "" {
			continue
		}
		options := server.Config{Routes: routes, Limits: limits, Admission: admission, InitialForwards: cfg.InitialForwards}
		options.Peer = i == 1
		listenerServer, err := server.New(options)
		if err != nil {
			return nil, err
		}
		node.servers = append(node.servers, listenerServer)
	}
	// Construct and validate both transports before binding either address.
	for _, address := range []string{cfg.Application, cfg.Peer} {
		if address == "" {
			continue
		}
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return nil, errors.New("listener startup failed")
		}
		node.listeners = append(node.listeners, listener)
	}
	node.guard = overload.New(node.targets, node.budget)
	if err := node.registerMetrics(cfg); err != nil {
		return nil, err
	}
	if cfg.Diagnostics != "" {
		if err := node.openDiagnostics(cfg.Diagnostics); err != nil {
			return nil, err
		}
	}
	complete = true
	return node, nil
}
func openLocal(ctx context.Context, name string, cfg *Local) (*store.Runtime, error) {
	limits := cfg.runtimeLimits()
	var adapter execution.Adapter
	var err error
	if cfg.Mongo != nil {
		config := mongodb.Config{URI: cfg.Mongo.URI, Store: name, Database: cfg.Mongo.Database, Collection: cfg.Mongo.Collection, Pool: uint64(limits.Concurrency)}
		adapter, err = mongodb.Open(ctx, config)
	} else {
		config := search.Config{Store: name, URL: cfg.Search.URL, Index: cfg.Search.Index, Profile: cfg.Search.Profile, Pool: limits.Concurrency}
		adapter, err = search.Open(ctx, config)
	}
	if err != nil {
		return nil, errors.New("local Store startup qualification failed")
	}
	return store.New(adapter, limits)
}
