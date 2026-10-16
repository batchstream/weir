package store

import (
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type singleExecutionAdapter struct {
	lifecycleAdapter
}

func (*singleExecutionAdapter) Execute(_ context.Context, plans []*execution.Plan, emit execution.Emit) {
	for _, work := range plans {
		var event *pb.Event
		if work.Command.GetNative() != nil {
			head := &pb.NativeHead{BodyContentType: "application/opaque"}
			headValue := &pb.Event_Head{Head: head}
			headEvent := &pb.Event{Value: headValue}
			_ = emit(work, headEvent)
			end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
			value := &pb.Event_NativeEnd{NativeEnd: end}
			event = &pb.Event{Value: value}
		} else {
			event = execution.FailedEvent(work.Command, pb.MutationOutcome_APPLIED, nil)
		}
		_ = emit(work, event)
	}
}

func TestCommandCompletionReleasesReservations(t *testing.T) {
	for _, native := range []bool{false, true} {
		name := "record"
		if native {
			name = "native"
		}
		t.Run(name, func(t *testing.T) {
			adapter := &singleExecutionAdapter{}
			limits := DefaultLimits()
			limits.BatchWait = 0
			limits.MaxInFlight = 0
			runtime := newRuntime(adapter, limits)
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
			batch, _ := runtime.selectLocked(time.Now())
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
						if !terminal && emission.Event.GetHead() == nil && emission.Event.GetNativeEnd().GetCompletion() != pb.NativeCompletion_RESPONSE_COMPLETE {
							t.Fatal("Native evidence changed", emission.Event)
						}
						emission.Release()
						if terminal {
							goto finished
						}
					case <-ctx.Done():
						t.Fatal("Native completion retained the caller")
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
				t.Fatal("execution changed write evidence")
			}
			ticket.Ack()
			waitReleased(t, runtime)
			if snapshot := runtime.Snapshot(); snapshot.Pending != 0 {
				t.Fatal("completed command queued another invocation", snapshot)
			}
		})
	}
}
