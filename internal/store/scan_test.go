package store

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"google.golang.org/protobuf/proto"
)

type scanTestAdapter struct {
	fetches, cleanups atomic.Int32
	pages             int
	documents         int
	documentBytes     int
	rejected          atomic.Bool
	started           chan struct{}
	nextToken         []byte
	cleanupFailure    *pb.Failure
}

func (a *scanTestAdapter) PrepareCommand(id uint64, call *pb.Command) (*execution.Plan, *pb.Failure) {
	work := &execution.Plan{ID: id, Command: call, Key: "scan", Singleton: true, CleanupRequired: true, Bytes: 1024, ResultBytes: protocol.MaxDocument + 512, WorkingBytes: 24 << 20}
	if a.documents > 1 {
		work.ResultBytes = execution.ScanResultBytes
	}
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
	if work.Command.GetRead() != nil || work.Command.GetMutate() != nil {
		result := execution.FailedEvent(work.Command, pb.MutationOutcome_APPLIED, nil)
		output := result
		_ = emit(work, output)
		return execution.Healthy
	}
	count := a.fetches.Add(1)
	pages := a.pages
	if pages == 0 {
		pages = 6
	}
	documents := max(a.documents, 1)
	emitted := 0
	for range documents {
		data := []byte(`{"value":1}`)
		if a.documentBytes > 0 {
			data = append([]byte(`{"pad":"`), bytes.Repeat([]byte("x"), a.documentBytes-10)...)
			data = append(data, []byte(`"}`)...)
		}
		document := &pb.Document{ContentType: "application/json", Data: data}
		value := &pb.Event_Document{Document: document}
		event := &pb.Event{Value: value}
		output := event
		if err := emit(work, output); err != nil {
			a.rejected.Store(true)
			break
		}
		emitted++
	}
	work.Continue = int(count) < pages
	if a.rejected.Load() {
		work.Continue = false
	}
	if !work.Continue {
		end := &pb.ScanEnd{DocumentCount: uint64((int(count)-1)*documents + emitted), Exhausted: len(a.nextToken) == 0, NextContinuationToken: bytes.Clone(a.nextToken)}
		if a.rejected.Load() {
			end.Failure = protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "test adapter exceeded Scan output budget")
			end.Exhausted = false
			end.NextContinuationToken = nil
		}
		variant := &pb.Event_ScanEnd{ScanEnd: end}
		terminal := &pb.Event{Value: variant}
		terminalOutput := terminal
		_ = emit(work, terminalOutput)
	}
	return execution.Healthy
}
func (a *scanTestAdapter) ClosePlan(context.Context, *execution.Plan) *pb.Failure {
	a.cleanups.Add(1)
	return a.cleanupFailure
}
func (*scanTestAdapter) Close() error { return nil }

func scanCall() *pb.Command {
	request := &pb.ScanRequest{Resource: "records"}
	variant := &pb.Command_Scan{Scan: request}
	call := &pb.Command{Operation: variant}
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
			work, failure := runtime.PrepareCommand(1, scanCall())
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
					if emission.Event == nil {
						emission.Release()
						ticket.Ack()
						goto finished
					}
					if emission.Event.GetDocument() != nil {
						documents++
					}
					if terminal := emission.Event.GetScanEnd(); terminal != nil {
						end = terminal
						_, err := proto.Marshal(emission.Event)
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
	work, failure := runtime.PrepareCommand(1, scanCall())
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
			if emission.Event == nil {
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
	adapter := &scanTestAdapter{pages: 5, documents: execution.ScanBatchDocuments}
	limits := DefaultLimits()
	limits.Concurrency = 1
	runtime, err := New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	session := runtime.NewSession()
	defer session.Close()
	work, _ := runtime.PrepareCommand(1, scanCall())
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
	if err != nil || result.GetMutationResult().Outcome != pb.MutationOutcome_APPLIED {
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

func TestScanBatchPublicationBounds(t *testing.T) {
	cases := []struct {
		name             string
		documents, bytes int
		reservation      int
		accepted         int
		rejected         bool
	}{
		{name: "full batch", documents: 128, bytes: 32 << 10, accepted: 128},
		{name: "document bound", documents: 129, accepted: 128, rejected: true},
		{name: "byte bound", documents: 3, bytes: 2 << 20, accepted: 2, rejected: true},
		{name: "reservation bound", documents: 2, bytes: 1024, reservation: execution.ResultOverheadBytes, rejected: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			adapter := &scanTestAdapter{pages: 1, documents: test.documents, documentBytes: test.bytes}
			limits := DefaultLimits()
			runtime, err := New(adapter, limits)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close(context.Background())
			session := runtime.NewSession()
			defer session.Close()
			work, failure := runtime.PrepareCommand(1, scanCall())
			if failure != nil {
				t.Fatal(failure)
			}
			if test.reservation != 0 {
				work.ResultBytes = test.reservation
			}
			ticket, failure, _ := runtime.Submit(context.Background(), work, session)
			if failure != nil {
				t.Fatal(failure)
			}
			documents, retained := 0, 0
			var end *pb.ScanEnd
			for {
				select {
				case emission := <-session.Events:
					if emission.Event == nil {
						emission.Release()
						ticket.Ack()
						goto finished
					}
					if document := emission.Event.GetDocument(); document != nil {
						documents++
						retained += len(document.Data)
					}
					if terminal := emission.Event.GetScanEnd(); terminal != nil {
						end = terminal
					}
					emission.Release()
				case <-time.After(3 * time.Second):
					t.Fatal("bounded Scan batch did not finish")
				}
			}
		finished:
			if documents != test.accepted || retained > execution.ScanBatchBytes || end == nil || int(end.DocumentCount) != documents || adapter.rejected.Load() != test.rejected {
				t.Fatal("Scan batch publication exceeded bounds", documents, retained, end, adapter.rejected.Load())
			}
			if test.rejected && end.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
				t.Fatal("Scan overflow was not reported", end)
			}
			session.Close()
			waitReleased(t, runtime)
			if snapshot := runtime.Snapshot(); snapshot.ResultBytes != 0 || snapshot.WorkingBytes != 0 {
				t.Fatal("Scan batch retained reservations", snapshot)
			}
		})
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
		work, _ := runtime.PrepareCommand(1, scanCall())
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

func (a *scanTestAdapter) PrepareRecord(record *execution.Record) (*execution.Plan, *pb.Failure) {
	command := record.Command()
	prepared := &execution.Plan{ID: record.Index(), Command: command, Key: record.Key(), BatchKey: "records", Bytes: 1024, ResultBytes: execution.ResultOverheadBytes, WorkingBytes: 1024}
	return prepared, nil
}
