// Package app owns immutable assembly and the complete process lifecycle.
package app

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/mongostore"
	"github.com/batchstream/weir/internal/overload"
	"github.com/batchstream/weir/internal/searchstore"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

type Node struct {
	mu                      sync.Mutex
	closed                  bool
	started                 bool
	state                   string
	registry                *prometheus.Registry
	diagnostics             *diagnostics
	guard                   *overload.Guard
	localNames, remoteNames []string
	drains                  prometheus.Counter
	drainDuration           prometheus.Histogram
	admission               *server.Admission
	runtimes                []*store.Runtime
	remotes                 []*server.RemoteWeir
	servers                 []*server.Server
	listeners               []net.Listener
	targets                 []overload.Target
	budget                  uint64
	stopGuard               context.CancelFunc
	guardDone               chan struct{}
	start                   sync.Once
	once                    sync.Once
	closeErr                error
	Errors                  chan error
}

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
			remoteConfig := server.RemoteConfig{Endpoint: definition.Remote.Endpoint, Relays: definition.Remote.Relays}
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
	limits := store.DefaultLimits()
	if cfg.BatchOperations != 0 {
		limits.BatchOperations = cfg.BatchOperations
	}
	var adapter execution.Adapter
	var err error
	if cfg.Mongo != nil {
		config := mongostore.Config{URI: cfg.Mongo.URI, Store: name, Database: cfg.Mongo.Database, Collection: cfg.Mongo.Collection, Pool: uint64(limits.Concurrency)}
		adapter, err = mongostore.Open(ctx, config)
	} else {
		config := searchstore.Config{Store: name, URL: cfg.Search.URL, Index: cfg.Search.Index, Profile: cfg.Search.Profile, Pool: limits.Concurrency}
		adapter, err = searchstore.Open(ctx, config)
	}
	if err != nil {
		return nil, errors.New("local Store startup qualification failed")
	}
	return store.New(adapter, limits)
}
func (n *Node) Start() {
	n.start.Do(func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.closed {
			return
		}
		n.started = true
		ctx, cancel := context.WithCancel(context.Background())
		n.stopGuard = cancel
		n.guardDone = make(chan struct{})
		go func() { defer close(n.guardDone); n.guard.Run(ctx) }()
		for i, srv := range n.servers {
			listener := n.listeners[i]
			go func() { n.listenerEnded(srv.Serve(listener)) }()
			<-srv.Serving()
		}
		n.state = "serving"
		if n.diagnostics != nil {
			go func() { n.listenerEnded(n.diagnostics.serve()) }()
		}
	})
}
func (n *Node) Addresses() []string {
	addresses := make([]string, len(n.listeners))
	for i, listener := range n.listeners {
		addresses[i] = listener.Addr().String()
	}
	return addresses
}
func (n *Node) Close(ctx context.Context) error {
	n.once.Do(func() {
		drain, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		n.mu.Lock()
		n.closed = true
		n.state = "draining"
		started := time.Now()
		n.drains.Inc()
		n.admission.BeginDrain()
		n.mu.Unlock()
		if n.stopGuard != nil {
			n.stopGuard()
			<-n.guardDone
		}
		for _, runtime := range n.runtimes {
			runtime.BeginDrain()
		}
		finished := make(chan error, len(n.servers)+len(n.runtimes))
		for _, srv := range n.servers {
			go func() { finished <- srv.Shutdown(drain) }()
		}
		for _, runtime := range n.runtimes {
			go func() { finished <- runtime.Close(drain) }()
		}
		for range len(n.servers) + len(n.runtimes) {
			n.closeErr = errors.Join(n.closeErr, <-finished)
		}
		for _, listener := range n.listeners {
			_ = listener.Close()
		}
		for _, remote := range n.remotes {
			n.closeErr = errors.Join(n.closeErr, remote.Close())
		}
		n.drainDuration.Observe(time.Since(started).Seconds())
		n.mu.Lock()
		n.state = "closed"
		n.mu.Unlock()
		n.closeDiagnostics()
	})
	return n.closeErr
}
