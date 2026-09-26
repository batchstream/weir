// Package app owns immutable assembly and the complete process lifecycle.
package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/mongostore"
	"github.com/batchstream/weir/internal/overload"
	"github.com/batchstream/weir/internal/searchstore"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
)

type Node struct {
	mu        sync.Mutex
	closed    bool
	admission *server.Admission
	runtimes  []*store.Runtime
	remotes   []*server.RemoteWeir
	servers   []*server.Server
	listeners []net.Listener
	targets   []overload.Target
	budget    uint64
	stopGuard context.CancelFunc
	guardDone chan struct{}
	start     sync.Once
	once      sync.Once
	closeErr  error
	Errors    chan error
}

func Open(ctx context.Context, cfg Config) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var identity *tls.Config
	var err error
	if cfg.Identity != nil {
		identity, err = loadIdentity(*cfg.Identity)
		if err != nil {
			return nil, err
		}
	}
	limits := cfg.Limits.serverLimits()
	admission, err := server.NewAdmission(limits)
	if err != nil {
		return nil, err
	}
	node := &Node{admission: admission, budget: cfg.MemoryMiB << 20, Errors: make(chan error, 2)}
	node.targets = append(node.targets, admission)
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
			tlsConfig := identity.Clone()
			tlsConfig.ServerName = definition.Remote.ServerName
			remoteConfig := server.RemoteConfig{Endpoint: definition.Remote.Endpoint, TLS: tlsConfig, Relays: definition.Remote.Relays}
			service.RemoteWeir, err = server.NewRemote(remoteConfig)
			if err != nil {
				return nil, err
			}
			node.remotes = append(node.remotes, service.RemoteWeir)
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
			node.targets = append(node.targets, service.LocalStore)
		}
		services[definition.Name] = service
	}
	routes := make(map[string]server.Service)
	for _, route := range cfg.Routes {
		routes[route.Store] = services[route.Service]
	}
	for _, address := range []string{cfg.Application, cfg.Peer} {
		if address == "" {
			continue
		}
		options := server.Config{Routes: routes, Limits: limits, Admission: admission, InitialForwards: cfg.InitialForwards}
		if address == cfg.Peer {
			policy := &server.PeerPolicy{TLS: identity, Allow: make(map[string]map[string]server.Permission)}
			for _, grant := range cfg.Allow {
				if policy.Allow[grant.Identity] == nil {
					policy.Allow[grant.Identity] = make(map[string]server.Permission)
				}
				permission, _ := permissions(grant.Operations)
				policy.Allow[grant.Identity][grant.Store] = permission
			}
			options.Peer = policy
		}
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
func loadIdentity(paths Identity) (*tls.Config, error) {
	certificate, err := readIdentityFile(paths.Certificate)
	if err != nil {
		return nil, errors.New("peer certificate unavailable")
	}
	key, err := readIdentityFile(paths.PrivateKey)
	if err != nil {
		return nil, errors.New("peer private key unavailable")
	}
	pair, err := tls.X509KeyPair(certificate, key)
	if err != nil {
		return nil, errors.New("invalid peer certificate/key pair")
	}
	ca, err := readIdentityFile(paths.CA)
	if err != nil {
		return nil, errors.New("peer CA unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid peer CA")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil || len(leaf.DNSNames) != 1 || !validIdentity(leaf.DNSNames[0]) {
		return nil, errors.New("peer requires one explicit DNS SAN identity")
	}
	intermediates := x509.NewCertPool()
	for _, raw := range pair.Certificate[1:] {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return nil, errors.New("invalid peer certificate chain")
		}
		intermediates.AddCert(cert)
	}
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		options := x509.VerifyOptions{Roots: roots, Intermediates: intermediates, DNSName: leaf.DNSNames[0], KeyUsages: []x509.ExtKeyUsage{usage}}
		if _, err := leaf.Verify(options); err != nil {
			return nil, errors.New("peer identity is not valid for mutual TLS")
		}
	}
	config := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair}, RootCAs: roots, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert, NextProtos: []string{"h2"}}
	return config, nil
}
func readIdentityFile(name string) ([]byte, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if err != nil || len(raw) > 64<<10 {
		return nil, errors.New("identity file exceeds bound")
	}
	return raw, nil
}
func (n *Node) Start() {
	n.start.Do(func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.closed {
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		n.stopGuard = cancel
		n.guardDone = make(chan struct{})
		go func() { defer close(n.guardDone); overload.Guard(ctx, n.targets, n.budget) }()
		for i, srv := range n.servers {
			listener := n.listeners[i]
			go func() { n.Errors <- srv.Serve(listener) }()
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
		n.admission.BeginDrain()
		if n.stopGuard != nil {
			n.stopGuard()
			<-n.guardDone
		}
		n.mu.Unlock()
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
	})
	return n.closeErr
}
