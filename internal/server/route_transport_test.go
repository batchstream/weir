package server

import (
	"context"
	"io"
	"runtime"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBatchDeadlineAndCancellation(t *testing.T) {
	adapter, local := peerLocal(t, "records")
	release := make(chan struct{})
	adapter.block = release
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	srv, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	ctx, cancel := context.WithTimeout(t.Context(), 700*time.Millisecond)
	deadline, _ := ctx.Deadline()
	done := make(chan error, 1)
	go func() { _, err := routeMutate(client, ctx, testMutation("value")); done <- err }()
	select {
	case observed := <-adapter.seen:
		if actual, ok := observed.Deadline(); !ok || actual.After(deadline.Add(10*time.Millisecond)) {
			t.Fatal("caller deadline was reset", actual, deadline)
		}
	case <-ctx.Done():
		t.Fatal("backend execution was not reached")
	}
	cancel()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatal("cancellation did not reach record stream caller", err)
	}
	close(release)
	// Cancellation can orphan a native DATA buffer. Its cleanup returns byte
	// credit only after the actual storage becomes unreachable, unlike the
	// normal successful response path which releases it through BufferPool.Put.
	until := time.Now().Add(3 * time.Second)
	for (srv.Snapshot().ActiveRPCs != 0 || local.Snapshot().Retained != 0 || srv.admission.wireBytes.Load() != 0) && time.Now().Before(until) {
		runtime.GC()
		time.Sleep(time.Millisecond)
	}
	waitPeerIdle(t, srv)
	until = time.Now().Add(time.Second)
	for local.Snapshot().Retained != 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if local.Snapshot().Retained != 0 {
		t.Fatal("cancelled batch retained results", local.Snapshot())
	}
}

func TestBatchGracefulDrainPreservesAdmittedMutation(t *testing.T) {
	adapter, local := peerLocal(t, "records")
	release := make(chan struct{})
	adapter.block = release
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	srv, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	resultDone := make(chan error, 1)
	go func() {
		result, err := routeMutate(client, ctx, testMutation("applied"))
		if err == nil && result.Outcome != pb.MutationOutcome_APPLIED {
			err = io.ErrUnexpectedEOF
		}
		resultDone <- err
	}()
	select {
	case <-adapter.seen:
	case <-ctx.Done():
		t.Fatal("mutation was not admitted")
	}
	shutdown := make(chan error, 1)
	go func() { shutdown <- srv.Shutdown(ctx) }()
	select {
	case <-srv.draining:
	case <-ctx.Done():
		t.Fatal("server did not begin drain")
	}
	close(release)
	if err := <-resultDone; err != nil {
		t.Fatal("admitted mutation lost its confirmation during graceful drain", err)
	}
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	waitPeerIdle(t, srv)
}
