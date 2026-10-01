package store

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
)

type scanTestAdapter struct {
	fetches, cleanups atomic.Int32
	pages             int
	started           chan struct{}
}

func (a *scanTestAdapter) PrepareCall(id uint64, call *pb.Call) (*execution.Plan, *pb.Failure) {
	work := &execution.Plan{ID: id, Call: call, Key: "scan", Singleton: true, CleanupRequired: true, Bytes: 1024, ResultBytes: protocol.MaxDocument + 512, WorkingBytes: 24 << 20}
	return work, nil
}
func (a *scanTestAdapter) Execute(ctx context.Context, works []*execution.Plan, emit execution.Emit) execution.Feedback {
	if a.started != nil {
		select {
		case a.started <- struct{}{}:
		default:
		}
	}
	work := works[0]
	if work.Operation != nil {
		result := protocol.ResultError(work.Operation, pb.MutationOutcome_APPLIED, nil)
		_ = emit(work, resultEvent(result))
		return execution.Healthy
	}
	count := a.fetches.Add(1)
	pages := a.pages
	if pages == 0 {
		pages = 6
	}
	document := &pb.Document{MediaType: "application/json", Data: []byte(`{"value":1}`)}
	value := &pb.Event_Document{Document: document}
	event := &pb.Event{Version: 1, Value: value}
	_ = emit(work, event)
	work.Continue = int(count) < pages
	if !work.Continue {
		end := &pb.ScanEnd{DocumentCount: uint64(pages)}
		variant := &pb.Event_ScanEnd{ScanEnd: end}
		terminal := &pb.Event{Version: 1, Value: variant}
		_ = emit(work, terminal)
	}
	return execution.Healthy
}
func (a *scanTestAdapter) ClosePlan(context.Context, *execution.Plan) *pb.Failure {
	a.cleanups.Add(1)
	return nil
}
func (*scanTestAdapter) Close() error { return nil }

func scanCall() *pb.Call {
	request := &pb.ScanRequest{Resource: "records"}
	variant := &pb.Call_Scan{Scan: request}
	call := &pb.Call{Version: 1, Operation: variant}
	return call
}
func waitReleased(t *testing.T, runtime *Runtime) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot := runtime.Snapshot()
		if snapshot.Retained == 0 && snapshot.Publishers == 0 && snapshot.Active == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("reservations did not release", runtime.Snapshot())
}

func TestUnifiedStreamingBackpressureAndReservation(t *testing.T) {
	adapter := &scanTestAdapter{pages: 20}
	limits := DefaultLimits()
	limits.Concurrency = 1
	runtime, err := New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	session := runtime.NewSession()
	defer session.Close()
	work, failure := runtime.PrepareCall(1, scanCall())
	if failure != nil {
		t.Fatal(failure)
	}
	ticket, failure, _ := runtime.Submit(context.Background(), work, session)
	if failure != nil {
		t.Fatal(failure)
	}
	first := <-session.Events
	time.Sleep(10 * time.Millisecond)
	if count := adapter.fetches.Load(); count != 1 {
		t.Fatalf("fetch progressed before application borrow release: %d", count)
	}
	snapshot := runtime.Snapshot()
	if snapshot.Active != 0 || snapshot.Retained != 1 || snapshot.WorkingBytes != 0 || snapshot.ResultBytes != protocol.MaxDocument+512 {
		t.Fatal("stream continuation lost reservation or execution permit", snapshot)
	}
	first.Release()
	documents := 1
	for {
		select {
		case emission := <-session.Events:
			emission.Release()
			if emission.End {
				ticket.Ack()
				goto finished
			}
			if emission.Event.GetDocument() != nil {
				documents++
			}
		case <-time.After(time.Second):
			t.Fatal("stream did not finish")
		}
	}
finished:
	if documents != 20 || adapter.cleanups.Load() != 1 {
		t.Fatal("stream lost documents or cleanup", documents, adapter.cleanups.Load())
	}
	waitReleased(t, runtime)
}

func TestBlockedScanReleasesOnlyExecutionPermitAtConcurrencyOne(t *testing.T) {
	adapter := &scanTestAdapter{pages: 5}
	limits := DefaultLimits()
	limits.Concurrency = 1
	runtime, err := New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	session := runtime.NewSession()
	defer session.Close()
	work, _ := runtime.PrepareCall(1, scanCall())
	_, failure, _ := runtime.Submit(context.Background(), work, session)
	if failure != nil {
		t.Fatal(failure)
	}
	blocked := <-session.Events
	ordinary := plan(2, "records/s:independent", false)
	ticket, failure, _ := runtime.Submit(context.Background(), ordinary, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := ticket.Wait(ctx)
	if err != nil || result.GetMutation().Outcome != pb.MutationOutcome_APPLIED {
		t.Fatal("slow scan monopolized the one execution permit", err, result)
	}
	ticket.Ack()
	if adapter.fetches.Load() != 1 {
		t.Fatal("blocked consumer did not stop scan fetch")
	}
	session.Close()
	blocked.Release()
	waitReleased(t, runtime)
	if adapter.cleanups.Load() != 1 {
		t.Fatal("abandoned cursor cleanup count", adapter.cleanups.Load())
	}
}

func TestUnifiedStreamCancellationAndShutdownJoin(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		adapter := &scanTestAdapter{pages: 1000}
		limits := DefaultLimits()
		runtime, err := New(adapter, limits)
		if err != nil {
			t.Fatal(err)
		}
		session := runtime.NewSession()
		ctx, cancel := context.WithCancel(context.Background())
		work, _ := runtime.PrepareCall(1, scanCall())
		ticket, failure, _ := runtime.Submit(ctx, work, session)
		if failure != nil {
			t.Fatal(failure)
		}
		select {
		case <-session.Events:
		case <-time.After(time.Second):
			t.Fatal("stream not started")
		}
		if shutdown {
			closeContext, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
			_ = runtime.Close(closeContext)
			stop()
		} else {
			cancel()
			session.Close()
			closeContext, stop := context.WithTimeout(context.Background(), time.Second)
			if err := runtime.Close(closeContext); err != nil {
				t.Fatal(err)
			}
			stop()
		}
		cancel()
		session.Close()
		ticket.Abandon()
		waitReleased(t, runtime)
		snapshot := runtime.Snapshot()
		if snapshot.WorkingBytes != 0 || adapter.cleanups.Load() != 1 {
			t.Fatal("cancel/shutdown did not join streaming cleanup", shutdown, snapshot, adapter.cleanups.Load())
		}
	}
}
