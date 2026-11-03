// Package app owns immutable assembly and the complete process lifecycle.
package app

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/batchstream/weir/internal/overload"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

type Node struct {
	shutdownTimeout time.Duration
	serving         atomic.Bool
	registry        *prometheus.Registry
	diagnostics     *diagnostics
	guard           *overload.Guard
	stores          map[string]*store.Runtime
	listener        net.Listener
	server          *server.Server
	stopGuard       context.CancelFunc
	guardDone       chan struct{}
	listeners       sync.WaitGroup
	once            sync.Once
	closeErr        error
	Errors          chan error
}

// Start serves on the listeners Open bound, which already queue connections.
func (n *Node) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	n.stopGuard = cancel
	n.guardDone = make(chan struct{})
	go func() {
		defer close(n.guardDone)
		n.guard.Run(ctx)
	}()
	n.serving.Store(true)
	n.listeners.Go(func() { n.listenerEnded(n.server.Serve(n.listener)) })
	if n.diagnostics != nil {
		n.listeners.Go(func() { n.listenerEnded(n.diagnostics.serve()) })
	}
}

// Address returns the bound application listener address.
func (n *Node) Address() string {
	return n.listener.Addr().String()
}

func (n *Node) Close(ctx context.Context) error {
	n.once.Do(func() {
		drain, cancel := context.WithTimeout(ctx, n.shutdownTimeout)
		defer cancel()

		n.serving.Store(false)
		n.server.BeginDrain()
		if n.stopGuard != nil {
			n.stopGuard()
			<-n.guardDone
		}
		for _, runtime := range n.stores {
			runtime.BeginDrain()
		}

		finished := make(chan error, len(n.stores))
		for _, runtime := range n.stores {
			go func() { finished <- runtime.Close(drain) }()
		}
		n.server.Shutdown(drain)
		for range n.stores {
			n.closeErr = errors.Join(n.closeErr, <-finished)
		}

		if n.listener != nil {
			_ = n.listener.Close()
		}
		// Diagnostics outlive the drain, so it stays observable.
		if n.diagnostics != nil {
			n.diagnostics.close()
		}
		n.listeners.Wait()
	})

	return n.closeErr
}

func (n *Node) listenerEnded(err error) {
	n.serving.Store(false)
	n.Errors <- err
}
