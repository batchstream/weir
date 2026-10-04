// Package app owns immutable assembly and the complete process lifecycle.
package app

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/batchstream/weir/internal/directory"
	"github.com/batchstream/weir/internal/overload"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

type Node struct {
	mu            sync.Mutex
	closed        bool
	started       bool
	state         string
	registry      *prometheus.Registry
	diagnostics   *diagnostics
	guard         *overload.Guard
	localNames    []string
	drains        prometheus.Counter
	drainDuration prometheus.Histogram
	admission     *server.Admission
	runtimes      []*store.Runtime
	directory     *directory.Directory
	servers       []*server.Server
	listeners     []net.Listener
	stopGuard     context.CancelFunc
	guardDone     chan struct{}
	start         sync.Once
	startErr      error
	serving       sync.WaitGroup
	once          sync.Once
	closeErr      error
	Errors        chan error
}

func (n *Node) Start(startup context.Context) error {
	n.start.Do(func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.closed {
			n.startErr = errors.New("node closed")
			return
		}
		if n.startErr = startup.Err(); n.startErr != nil {
			return
		}

		n.started = true
		ctx, cancel := context.WithCancel(context.Background())
		n.stopGuard = cancel
		n.guardDone = make(chan struct{})
		go func() {
			defer close(n.guardDone)
			n.guard.Run(ctx)
		}()

		for i, srv := range n.servers {
			listener := n.listeners[i]
			n.serving.Go(func() { n.listenerEnded(srv.Serve(listener)) })
			<-srv.Serving()
		}

		n.directory.Start(ctx)

		if n.diagnostics != nil {
			n.serving.Go(func() { n.listenerEnded(n.diagnostics.serve()) })
		}

		if n.startErr = startup.Err(); n.startErr != nil {
			return
		}
		n.state = "serving"
	})

	return n.startErr
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

		remaining := len(n.servers) + len(n.runtimes)
		if n.directory != nil {
			remaining++
		}
		finished := make(chan error, remaining)
		if n.directory != nil {
			go func() { finished <- n.directory.Close(drain) }()
		}
		for _, srv := range n.servers {
			go func() { finished <- srv.Shutdown(drain) }()
		}
		for _, runtime := range n.runtimes {
			go func() { finished <- runtime.Close(drain) }()
		}
		for range remaining {
			n.closeErr = errors.Join(n.closeErr, <-finished)
		}

		for _, listener := range n.listeners {
			_ = listener.Close()
		}

		n.drainDuration.Observe(time.Since(started).Seconds())
		n.mu.Lock()
		n.state = "closed"
		n.mu.Unlock()
		n.closeDiagnostics()
		n.serving.Wait()
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
