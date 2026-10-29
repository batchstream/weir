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
	shutdownTimeout time.Duration
	mu              sync.Mutex
	state           string
	registry        *prometheus.Registry
	diagnostics     *diagnostics
	guard           *overload.Guard
	admission       *server.Admission
	stores          map[string]*store.Runtime
	listener        net.Listener
	server          *server.Server
	stopGuard       context.CancelFunc
	guardDone       chan struct{}
	start           sync.Once
	startErr        error
	serving         sync.WaitGroup
	once            sync.Once
	closeErr        error
	Errors          chan error
}

func (n *Node) Start(startup context.Context) error {
	n.start.Do(func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.state == "draining" || n.state == "closed" {
			n.startErr = errors.New("node closed")
			return
		}
		if n.startErr = startup.Err(); n.startErr != nil {
			return
		}

		ctx, cancel := context.WithCancel(context.Background())
		n.stopGuard = cancel
		n.guardDone = make(chan struct{})
		go func() {
			defer close(n.guardDone)
			n.guard.Run(ctx)
		}()

		n.serving.Go(func() { n.listenerEnded(n.server.Serve(n.listener)) })
		<-n.server.Serving()

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

// Address returns the bound application listener address.
func (n *Node) Address() string {
	return n.listener.Addr().String()
}

func (n *Node) Close(ctx context.Context) error {
	n.once.Do(func() {
		drain, cancel := context.WithTimeout(ctx, n.shutdownTimeout)
		defer cancel()

		n.mu.Lock()
		n.state = "draining"
		n.admission.BeginDrain()
		n.mu.Unlock()

		if n.stopGuard != nil {
			n.stopGuard()
			<-n.guardDone
		}
		for _, runtime := range n.stores {
			runtime.BeginDrain()
		}

		finished := make(chan error, len(n.stores)+1)
		remaining := 0
		if n.server != nil {
			remaining++
			go func() { finished <- n.server.Shutdown(drain) }()
		}
		for _, runtime := range n.stores {
			remaining++
			go func() { finished <- runtime.Close(drain) }()
		}
		for range remaining {
			n.closeErr = errors.Join(n.closeErr, <-finished)
		}

		if n.listener != nil {
			_ = n.listener.Close()
		}

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
	if n.state != "draining" && n.state != "closed" {
		n.state = "failed"
	}
	n.mu.Unlock()
	n.Errors <- err
}
