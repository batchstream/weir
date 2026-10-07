package store

import (
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type nonScanContinuationAdapter struct {
	lifecycleAdapter
}

func (*nonScanContinuationAdapter) Execute(_ context.Context, plans []*execution.Plan, emit execution.Emit) bool {
	for _, work := range plans {
		var event *pb.Event
		if work.Command.GetNative() != nil {
			end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
			value := &pb.Event_NativeEnd{NativeEnd: end}
			event = &pb.Event{Value: value}
		} else {
			event = execution.FailedEvent(work.Command, pb.MutationOutcome_APPLIED, nil)
		}
		_ = emit(work, event)
	}
	return true
}

func TestOnlyScanContinuationCanScheduleAnotherInvocation(t *testing.T) {
	for _, native := range []bool{false, true} {
		name := "record"
		if native {
			name = "native"
		}
		t.Run(name, func(t *testing.T) {
			adapter := &nonScanContinuationAdapter{}
			runtime := newRuntime(adapter, DefaultLimits())
			work := plan(1, name, false)
			var session *Session
			if native {
				work.Command = nativeCall()
				session = runtime.NewSession()
				defer session.Close()
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			ticket, failure, _ := runtime.Submit(ctx, work, session)
			if failure != nil {
				t.Fatal(failure)
			}
			runtime.mu.Lock()
			batch := runtime.selectLocked(time.Now())
			runtime.mu.Unlock()
			done := make(chan struct{})
			go func() {
				runtime.execute(batch)
				close(done)
			}()
			if native {
				for {
					select {
					case emission := <-session.Events:
						terminal := emission.Event == nil
						if !terminal && emission.Event.GetNativeEnd().GetCompletion() != pb.NativeCompletion_RESPONSE_COMPLETE {
							t.Fatal("Native evidence changed", emission.Event)
						}
						emission.Release()
						if terminal {
							goto finished
						}
					case <-ctx.Done():
						t.Fatal("Native continuation retained the caller")
					}
				}
			}
		finished:
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("adapter did not complete")
			}
			if !native && recordEvent(t, ticket).GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal("invalid continuation changed write evidence")
			}
			ticket.Ack()
			waitReleased(t, runtime)
			if snapshot := runtime.Snapshot(); snapshot.Pending != 0 {
				t.Fatal("non-Scan continuation queued another invocation", snapshot)
			}
		})
	}
}

type terminalContinuationAdapter struct {
	scanTestAdapter
	duplicateRejected bool
}

func (a *terminalContinuationAdapter) Execute(_ context.Context, plans []*execution.Plan, emit execution.Emit) bool {
	work := plans[0]
	end := &pb.ScanEnd{DocumentCount: 7, NextContinuationToken: []byte("checkpoint")}
	value := &pb.Event_ScanEnd{ScanEnd: end}
	event := &pb.Event{Value: value}
	_ = emit(work, event)
	a.duplicateRejected = emit(work, event) != nil
	return true
}

func TestScanTerminalEvidenceStopsContinuationAndRejectsDuplicate(t *testing.T) {
	adapter := &terminalContinuationAdapter{}
	runtime := newRuntime(adapter, DefaultLimits())
	session := runtime.NewSession()
	defer session.Close()
	work, failure := runtime.PrepareCommand(1, scanCall())
	if failure != nil {
		t.Fatal(failure)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	ticket, failure, _ := runtime.Submit(ctx, work, session)
	if failure != nil {
		t.Fatal(failure)
	}
	runtime.mu.Lock()
	batch := runtime.selectLocked(time.Now())
	runtime.mu.Unlock()
	runtime.execute(batch)
	select {
	case emission := <-session.Events:
		end := emission.Event.GetScanEnd()
		if end.GetDocumentCount() != 7 || string(end.GetNextContinuationToken()) != "checkpoint" || end.GetFailure() != nil {
			t.Fatal("accepted terminal evidence changed", end)
		}
		emission.Release()
	case <-ctx.Done():
		t.Fatal("terminal Scan result was not published")
	}
	select {
	case emission := <-session.Events:
		if emission.Event != nil {
			t.Fatal("duplicate terminal result was published", emission.Event)
		}
		emission.Release()
	case <-ctx.Done():
		t.Fatal("terminal Scan incorrectly continued")
	}
	ticket.Ack()
	waitReleased(t, runtime)
	if !adapter.duplicateRejected || adapter.cleanups.Load() != 1 || runtime.Snapshot().Pending != 0 {
		t.Fatal("terminal Scan repeated or skipped cleanup", adapter.duplicateRejected, adapter.cleanups.Load(), runtime.Snapshot())
	}
}
