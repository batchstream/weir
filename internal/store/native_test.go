package store

import (
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestAllCallKindsShareWorkingSetAdmission(t *testing.T) {
	adapter := &scanTestAdapter{}
	limits := DefaultLimits()
	limits.WorkingBytes = 24 << 20
	runtime := newRuntime(adapter, limits)
	session := runtime.NewSession()
	defer session.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	open := &pb.NativeOpen{Resource: "records"}
	native := &pb.NativeRequest{Open: open}
	variant := &pb.Command_Native{Native: native}
	call := &pb.Command{Version: 1, Operation: variant}
	first, _ := runtime.PrepareCommand(1, call)
	ticket, failure, _ := runtime.Submit(ctx, first, session)
	if failure != nil {
		t.Fatal(failure)
	}
	scan := &pb.ScanRequest{Resource: "records"}
	scanVariant := &pb.Command_Scan{Scan: scan}
	scanCall := &pb.Command{Version: 1, Operation: scanVariant}
	second, _ := runtime.PrepareCommand(2, scanCall)
	secondTicket, failure, _ := runtime.Submit(ctx, second, session)
	if failure != nil {
		t.Fatal(failure)
	}
	runtime.mu.Lock()
	firstBatch := runtime.selectLocked(time.Now())
	if firstBatch == nil {
		runtime.mu.Unlock()
		t.Fatal("native singleton did not dispatch")
	}
	if other := runtime.selectLocked(time.Now()); other != nil {
		runtime.mu.Unlock()
		t.Fatal("Scan bypassed Native execution working budget")
	}
	finish(runtime, firstBatch)
	secondBatch := runtime.selectLocked(time.Now())
	if secondBatch == nil {
		runtime.mu.Unlock()
		t.Fatal("released native budget did not unblock Scan")
	}
	finish(runtime, secondBatch)
	runtime.mu.Unlock()
	secondTicket.Ack()
	ticket.Abandon()
	runtime.mu.Lock()
	runtime.cancelQueuedLocked()
	runtime.mu.Unlock()
	if snapshot := runtime.Snapshot(); snapshot.WorkingBytes != 0 || snapshot.Retained != 0 {
		t.Fatal("queued cancellation leaked reservation", snapshot)
	}
}

func TestCanceledSingletonDoesNotRunOrPoisonPeer(t *testing.T) {
	adapter := &scanTestAdapter{pages: 1}
	limits := DefaultLimits()
	runtime, err := New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	session := runtime.NewSession()
	defer session.Close()
	canceled, stop := context.WithCancel(context.Background())
	stop()
	first := plan(1, "first", false)
	if _, failure, _ := runtime.Submit(canceled, first, session); failure == nil {
		t.Fatal("admitted canceled operation")
	}
	work := plan(2, "second", false)
	ticket, failure, _ := runtime.Submit(context.Background(), work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = ticket.Wait(ctx)
	if err != nil {
		t.Fatal("peer was canceled", err)
	}
	ticket.Ack()
}
