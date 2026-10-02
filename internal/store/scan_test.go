package store

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir/api/protocol"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type scanTestAdapter struct {
	fetches, cleanups atomic.Int32
	pages             int
	started           chan struct{}
	nextToken         []byte
	cleanupFailure    *pb.Failure
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
		end := &pb.ScanEnd{DocumentCount: uint64(pages), Exhausted: len(a.nextToken) == 0, NextContinuationToken: bytes.Clone(a.nextToken)}
		variant := &pb.Event_ScanEnd{ScanEnd: end}
		terminal := &pb.Event{Version: 1, Value: variant}
		_ = emit(work, terminal)
	}
	return execution.Healthy
}
func (a *scanTestAdapter) ClosePlan(context.Context, *execution.Plan) *pb.Failure {
	a.cleanups.Add(1)
	return a.cleanupFailure
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

func TestScanPageCompletionAndCleanupFailureReleaseReservations(t *testing.T) {
	cleanupFailure := &pb.Failure{Code: pb.FailureCode_UNAVAILABLE, Message: "scan cleanup failed"}
	cases := []struct {
		name    string
		token   []byte
		failure *pb.Failure
	}{
		{name: "exhausted"},
		{name: "resumable", token: []byte("continuation")},
		{name: "exhausted cleanup failure", failure: cleanupFailure},
		{name: "resumable cleanup failure", token: []byte("continuation"), failure: cleanupFailure},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			adapter := &scanTestAdapter{pages: 1, nextToken: test.token, cleanupFailure: test.failure}
			limits := DefaultLimits()
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
			var end *pb.ScanEnd
			documents := 0
			for {
				select {
				case emission := <-session.Events:
					if emission.End {
						emission.Release()
						ticket.Ack()
						goto finished
					}
					if emission.Event.GetDocument() != nil {
						documents++
					}
					if terminal := emission.Event.GetScanEnd(); terminal != nil {
						end = terminal
						_, err := protocol.MarshalEvent(emission.Event)
						if err != nil {
							emission.Release()
							t.Fatal("cleanup result cannot be sent through Route", err, terminal)
						}
					}
					emission.Release()
				case <-time.After(time.Second):
					t.Fatal("completed scan page retained its session")
				}
			}
		finished:
			if end == nil || end.DocumentCount != 1 || documents != 1 || adapter.cleanups.Load() != 1 {
				t.Fatal("scan page lost its terminal event or cleanup", end, documents, adapter.cleanups.Load())
			}
			if test.failure != nil {
				if end.GetFailure().GetCode() != test.failure.Code || end.Exhausted || len(end.NextContinuationToken) != 0 {
					t.Fatal("cleanup failure retained a success continuation", end)
				}
			} else if end.Failure != nil || end.Exhausted != (len(test.token) == 0) || !bytes.Equal(end.NextContinuationToken, test.token) {
				t.Fatal("completed scan page lost its continuation", end)
			}
			session.Close()
			waitReleased(t, runtime)
			if snapshot := runtime.Snapshot(); snapshot.Pending != 0 || snapshot.ResultBytes != 0 || snapshot.WorkingBytes != 0 {
				t.Fatal("completed scan page retained memory or backend work", snapshot)
			}
		})
	}
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
