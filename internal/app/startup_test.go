package app

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil/testmetrics"
)

func TestCanceledOpenAndValidationPrecedence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := emptyConfig(t)
	if node, err := Open(ctx, cfg); node != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled Open acquired a node", node, err)
	}
	cfg.Basic.Memory = 0
	if node, err := Open(ctx, cfg); node != nil || err == nil || errors.Is(err, context.Canceled) {
		t.Fatal("cancellation hid a real configuration failure", node, err)
	}
}

func TestCanceledStartNeverReadyAndClosesOnce(t *testing.T) {
	for _, concurrent := range []bool{false, true} {
		cfg := emptyConfig(t)
		cfg.Basic.Listeners.Peer, cfg.Basic.Diagnostics.Address = "127.0.0.1:0", "127.0.0.1:0"
		node, err := Open(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = node.Close(context.Background()) })
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Exact Open-success / before-Start boundary.
		if !concurrent {
			if err := node.Start(ctx); !errors.Is(err, context.Canceled) {
				t.Fatal("Start ignored cancellation", err)
			}
			if node.ready() || node.stopGuard != nil {
				t.Fatal("canceled Start published serving resources")
			}
		}
		drain, stop := context.WithTimeout(context.Background(), time.Second)
		var group sync.WaitGroup
		for range 3 {
			group.Go(func() { _ = node.Start(ctx) })
			group.Go(func() {
				if err := node.Close(drain); err != nil {
					t.Error(err)
				}
			})
		}
		group.Wait()
		stop()
		if node.ready() || node.stopGuard != nil {
			t.Fatal("canceled or closed node became ready")
		}
		families := testmetrics.Registry(t, node.registry)
		if testmetrics.Sum(families, "weir_node_drains_total") != 1 || node.state != "closed" {
			t.Fatal("partial node not closed exactly once")
		}
		for _, address := range append(node.Addresses(), node.DiagnosticAddress()) {
			listener, err := net.Listen("tcp", address)
			if err != nil {
				t.Fatal("partial listener retained", err)
			}
			_ = listener.Close()
		}
	}
}

func TestCloseBeforeStartNeverPublishesServingResources(t *testing.T) {
	cfg := emptyConfig(t)
	cfg.Basic.Listeners.Peer, cfg.Basic.Diagnostics.Address = "127.0.0.1:0", "127.0.0.1:0"
	node, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close(context.Background()) })
	if err := node.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := node.Start(t.Context()); err == nil || err.Error() != "node closed" {
		t.Fatal("closed node accepted Start", err)
	}
	if node.ready() || node.stopGuard != nil || node.state != "closed" || len(node.Errors) != 0 {
		t.Fatal("Start after Close published serving resources")
	}
}

func TestDrainingListenerReportsPreserveShutdownState(t *testing.T) {
	seed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = seed.Close() })
	if err := seed.(*net.TCPListener).SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	cfg := emptyConfig(t)
	cfg.Basic.Listeners.Peer = "127.0.0.1:0"
	cfg.Basic.Discovery.Seeds = []string{seed.Addr().String()}
	node, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close(context.Background()) })
	if err := node.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	initial, err := seed.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = initial.Close() })
	closed := make(chan error, 1)
	go func() { closed <- node.Close(t.Context()) }()
	// The seed withholds its HTTP/2 handshake, keeping withdrawal pending while
	// the real data listeners finish and report their shutdown to the node.
	withdrawal, err := seed.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = withdrawal.Close() })
	select {
	case err := <-node.Errors:
		if err != nil {
			t.Fatal("normal listener shutdown failed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("draining listener did not report shutdown")
	}
	node.mu.Lock()
	state := node.state
	node.mu.Unlock()
	if state != "draining" || node.ready() {
		t.Fatal("listener shutdown replaced the draining state", state)
	}
	_ = seed.Close()
	_ = withdrawal.Close()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("node did not finish shutdown after seed release")
	}
	if node.state != "closed" {
		t.Fatal("node did not publish closed state", node.state)
	}
}

func TestCloseJoinsListenerReports(t *testing.T) {
	cfg := emptyConfig(t)
	cfg.Basic.Listeners.Peer, cfg.Basic.Diagnostics.Address = "127.0.0.1:0", "127.0.0.1:0"
	node, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close(context.Background()) })
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(node.Errors) != 3 {
		t.Fatal("Close returned before all listener reports", len(node.Errors))
	}
	for range 3 {
		if err := <-node.Errors; err != nil {
			t.Fatal("normal shutdown listener failed", err)
		}
	}
}
