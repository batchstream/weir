package app

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
)

func TestCanceledOpenAndValidationPrecedence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := emptyConfig(t)
	if node, err := Open(ctx, cfg); node != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled Open acquired a node", node, err)
	}

	cfg.Listeners.Application = "invalid-listener"
	if node, err := Open(ctx, cfg); node != nil || err == nil || errors.Is(err, context.Canceled) {
		t.Fatal("cancellation hid a real configuration failure", node, err)
	}
}

func TestCanceledStartNeverReadyAndClosesOnce(t *testing.T) {
	for _, concurrent := range []bool{false, true} {
		cfg := emptyConfig(t)
		cfg.Listeners.Diagnostics = "127.0.0.1:0"
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
		for _, address := range []string{node.Address(), node.DiagnosticAddress()} {
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
	cfg.Listeners.Diagnostics = "127.0.0.1:0"
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

// closeBlockingAdapter keeps its Store's Close pending until release closes.
type closeBlockingAdapter struct {
	release chan struct{}
}

func (*closeBlockingAdapter) PrepareNative(context.Context, *pb.Command) (*execution.Plan, *pb.Failure) {
	return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "close fixture")
}

func (*closeBlockingAdapter) PrepareRecord(context.Context, *execution.Record) (*execution.Plan, *pb.Failure) {
	return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "close fixture")
}

func (*closeBlockingAdapter) ExecuteRecords(context.Context, []*execution.Plan) []*pb.Event {
	return nil
}

func (*closeBlockingAdapter) ExecuteNative(context.Context, *execution.Plan, execution.Emit) *pb.NativeEnd {
	failure := protocol.Fail(pb.FailureCode_UNSUPPORTED, "close fixture")
	return protocol.NativeFailure(false, failure)
}

func (a *closeBlockingAdapter) Close(context.Context) error {
	<-a.release
	return nil
}

func TestDrainingListenerReportsPreserveShutdownState(t *testing.T) {
	cfg := emptyConfig(t)
	node, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	// A Store whose Close is pending keeps the node draining after the
	// listener itself has stopped and reported.
	adapter := &closeBlockingAdapter{release: make(chan struct{})}
	limits := store.DefaultLimits()
	runtime, err := store.New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	node.stores["records"] = runtime
	var release sync.Once
	t.Cleanup(func() {
		release.Do(func() { close(adapter.release) })
		_ = node.Close(context.Background())
	})
	if err := node.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- node.Close(t.Context()) }()
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
	release.Do(func() { close(adapter.release) })
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("node did not finish shutdown after the Store closed")
	}
	if node.state != "closed" {
		t.Fatal("node did not publish closed state", node.state)
	}
}

func TestCloseJoinsListenerReports(t *testing.T) {
	cfg := emptyConfig(t)
	cfg.Listeners.Diagnostics = "127.0.0.1:0"
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
	if len(node.Errors) != 2 {
		t.Fatal("Close returned before all listener reports", len(node.Errors))
	}
	for range 2 {
		if err := <-node.Errors; err != nil {
			t.Fatal("normal shutdown listener failed", err)
		}
	}
}
