package store

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type nativeSink struct{}

func (*nativeSink) Head(*pb.NativeHead) error { return nil }
func (*nativeSink) Chunk([]byte) error        { return nil }
func (*nativeSink) Interrupt()                {}
func TestNativeSharesLedgerSessionAndC1(t *testing.T) {
	a := &scanTestAdapter{fetching: make(chan struct{}, 1)}
	limits := DefaultLimits()
	limits.Concurrency = 1
	limits.BackendTimeout = 80 * time.Millisecond
	r, err := New(a, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	sink := &nativeSink{}
	exchange := &execution.NativeExchange{Source: io.NopCloser(strings.NewReader("")), Sink: sink}
	open := &pb.NativeOpen{}
	ticket, f := r.StartNative(context.Background(), open, exchange)
	if f != nil {
		t.Fatal(f)
	}
	<-a.fetching
	req := &pb.ScanRequest{}
	if _, f := r.StartScan(context.Background(), req); f == nil {
		t.Fatal("separate Scan budget")
	}
	if _, f := r.StartNative(context.Background(), open, exchange); f == nil {
		t.Fatal("separate Native budget")
	}
	p := plan(1, "ordinary", true)
	ordinary, f, _ := r.Submit(context.Background(), p, nil)
	if f != nil {
		t.Fatal(f)
	}
	select {
	case <-ordinary.ready:
		t.Fatal("Native permit released while exchange active")
	case <-time.After(20 * time.Millisecond):
	}
	end := ticket.WaitNative()
	if end.Completion != pb.NativeCompletion_RESPONSE_INCOMPLETE {
		t.Fatal(end)
	}
	if r.Snapshot().NativeSessions != 1 {
		t.Fatal("released before output ack")
	}
	ticket.Ack()
	if _, err := ordinary.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	ordinary.Ack()
	if r.controller.epoch != 0 {
		t.Fatal("upload timeout treated as DB congestion")
	}
	scan, f := r.StartScan(context.Background(), req)
	if f != nil {
		t.Fatal(f)
	}
	if _, f := r.StartNative(context.Background(), open, exchange); f == nil {
		t.Fatal("Native did not share Scan budget")
	}
	scan.CloseScan()
	if snap := r.Snapshot(); snap.LiveSessions != 0 || snap.Retained != 0 || snap.Pending != 0 || snap.ResultBytes != 0 {
		t.Fatal(snap)
	}
}
func TestNativeQueuedCancelAndRejectionDoNotRead(t *testing.T) {
	a := &scanTestAdapter{}
	limits := DefaultLimits()
	limits.PendingOperations = 1
	r := newRuntime(a, limits)
	p := plan(0, "first", true)
	first, f, _ := r.Submit(context.Background(), p, nil)
	if f != nil {
		t.Fatal(f)
	}
	open := &pb.NativeOpen{}
	exchange := &execution.NativeExchange{}
	if _, f := r.StartNative(context.Background(), open, exchange); f == nil {
		t.Fatal("queue limit")
	}
	r.mu.Lock()
	b := r.selectLocked(time.Now())
	r.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	ticket, f := r.StartNative(ctx, open, exchange)
	if f != nil {
		t.Fatal(f)
	}
	cancel()
	r.mu.Lock()
	r.cancelQueuedLocked()
	r.mu.Unlock()
	end := ticket.WaitNative()
	if end.Completion != pb.NativeCompletion_NATIVE_NOT_STARTED || end.Failure == nil || a.fetches.Load() != 0 {
		t.Fatal(end)
	}
	ticket.Ack()
	r.mu.Lock()
	finish(r, b)
	r.mu.Unlock()
	first.Ack()
	if snap := r.Snapshot(); snap.LiveSessions != 0 || snap.PendingBytes != 0 || snap.Retained != 0 {
		t.Fatal(snap)
	}
}
