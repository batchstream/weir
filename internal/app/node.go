// Package app owns immutable assembly and the complete process lifecycle.
package app

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/batchstream/weir/internal/overload"
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

func (n *Node) ready() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == "serving"
}
func (n *Node) listenerEnded(err error) {
	n.mu.Lock()
	if !n.closed {
		n.state = "failed"
	}
	n.mu.Unlock()
	n.Errors <- err
}
