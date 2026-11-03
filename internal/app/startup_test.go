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

func TestCloseBeforeStartReleasesListenersOnce(t *testing.T) {
	cfg := emptyConfig(t)
	cfg.Listeners.Diagnostics = "127.0.0.1:0"
	node, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	drain, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	var group sync.WaitGroup
	for range 3 {
		group.Go(func() {
			if err := node.Close(drain); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	families := testmetrics.Registry(t, node.registry)
	if testmetrics.Sum(families, "weir_node_ready") != 0 || len(node.Errors) != 0 {
		t.Fatal("node that never started reported serving")
	}
	for _, address := range []string{node.Address(), node.DiagnosticAddress()} {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatal("listener retained", err)
		}
		_ = listener.Close()
	}
}

// closeBlockingAdapter keeps its Store's Close pending until release closes.
type closeBlockingAdapter struct {
	release chan struct{}
}

func (*closeBlockingAdapter) PrepareRecord(context.Context, *execution.Record) (*execution.Plan, *pb.Failure) {
	return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "close fixture")
}

func (*closeBlockingAdapter) ExecuteRecords(context.Context, []*execution.Plan) []*pb.Event {
	return nil
}

func (*closeBlockingAdapter) ExecuteNative(context.Context, *pb.Command, execution.Emit) *pb.NativeEnd {
	failure := protocol.Fail(pb.FailureCode_UNSUPPORTED, "close fixture")
	return protocol.NativeFailure(false, failure)
}

func (a *closeBlockingAdapter) Close(context.Context) error {
	<-a.release
	return nil
}

func TestDrainingListenerReportsWhileStoreCloses(t *testing.T) {
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
	node.Start()
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
	if node.serving.Load() {
		t.Fatal("draining node reported serving")
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
}

func TestCloseJoinsListenerReports(t *testing.T) {
	cfg := emptyConfig(t)
	cfg.Listeners.Diagnostics = "127.0.0.1:0"
	node, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close(context.Background()) })
	node.Start()
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
