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
	cfg := remoteConfig(t)
	if node, err := Open(ctx, cfg); node != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled Open acquired a node", node, err)
	}
	cfg.MemoryMiB = 0
	if node, err := Open(ctx, cfg); node != nil || err == nil || errors.Is(err, context.Canceled) {
		t.Fatal("cancellation hid a real configuration failure", node, err)
	}
}

func TestCanceledStartNeverReadyAndClosesOnce(t *testing.T) {
	for _, concurrent := range []bool{false, true} {
		cfg := remoteConfig(t)
		cfg.Peer, cfg.Diagnostics = "127.0.0.1:0", "127.0.0.1:0"
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
			if node.started || node.ready() || node.stopGuard != nil {
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
		if node.ready() || node.started {
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

func TestCloseJoinsListenerReports(t *testing.T) {
	cfg := remoteConfig(t)
	cfg.Peer, cfg.Diagnostics = "127.0.0.1:0", "127.0.0.1:0"
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
