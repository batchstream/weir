package store

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type nativeAccountingAdapter struct {
	lifecycleAdapter
	events []*pb.Event
}

func (a *nativeAccountingAdapter) Execute(_ context.Context, plans []*execution.Plan, emit execution.Emit) {
	for _, event := range a.events {
		if err := emit(plans[0], event); err != nil {
			return
		}
	}
	return
}

func TestLargeNativePublicationAccountingAndCancellation(t *testing.T) {
	for _, kind := range []string{"metadata", "failure"} {
		t.Run(kind, func(t *testing.T) {
			metadata := &pb.Document{ContentType: "application/octet-stream", Data: bytes.Repeat([]byte("m"), 3<<20)}
			head := &pb.NativeHead{Metadata: metadata, BodyContentType: "application/octet-stream"}
			headValue := &pb.Event_Head{Head: head}
			headEvent := &pb.Event{Value: headValue}
			failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, strings.Repeat("f", 4<<20))
			end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_INCOMPLETE, Failure: failure}
			endValue := &pb.Event_NativeEnd{NativeEnd: end}
			endEvent := &pb.Event{Value: endValue}
			adapter := &nativeAccountingAdapter{events: []*pb.Event{headEvent, endEvent}}
			limits := DefaultLimits()
			limits.MaxInFlight = 1
			runtime, err := New(adapter, limits, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close(context.Background())
			session := runtime.NewSession()
			defer session.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			work := plan(1, "native", false)
			work.Command = nativeCall()
			work.ResultBytes = protocol.NativeChunk + execution.ResultOverheadBytes
			ticket, rejected, _ := runtime.Submit(ctx, work, session)
			if rejected != nil {
				t.Fatal(rejected)
			}
			deadline, stop := context.WithTimeout(t.Context(), 2*time.Second)
			defer stop()
			var paused *Emission
			for {
				select {
				case paused = <-session.Events:
					if kind == "failure" && paused.Event.GetHead() != nil {
						paused.Release()
						continue
					}
				case <-deadline.Done():
					t.Fatal("Native publication did not start")
				}
				break
			}
			actual := proto.Size(paused.Event) + execution.ResultOverheadBytes
			if snapshot := runtime.Snapshot(); snapshot.ResultBytes < actual {
				t.Fatal("large paused Native event was discounted", snapshot.ResultBytes, actual)
			}
			cancel()
			if _, err := ticket.Wait(deadline); err != nil {
				t.Fatal("cancellation retained Native execution", err)
			}
			paused.Release()
			ticket.Ack()
			waitReleased(t, runtime)
			if snapshot := runtime.Snapshot(); snapshot.ResultBytes != 0 || snapshot.PendingBytes != 0 || snapshot.WorkingBytes != 0 {
				t.Fatal("Native accounting survived cancellation", snapshot)
			}
		})
	}
}

func TestNativeOversizedTerminalUsesFixedFailureInsideFullSharedBudget(t *testing.T) {
	cases := []struct {
		name       string
		completion pb.NativeCompletion
		want       pb.NativeCompletion
		label      string
	}{
		{name: "not_started", completion: pb.NativeCompletion_NATIVE_NOT_STARTED, want: pb.NativeCompletion_NATIVE_NOT_STARTED, label: "not_started"},
		{name: "complete", completion: pb.NativeCompletion_RESPONSE_COMPLETE, want: pb.NativeCompletion_RESPONSE_INCOMPLETE, label: "response_incomplete"},
		{name: "incomplete", completion: pb.NativeCompletion_RESPONSE_INCOMPLETE, want: pb.NativeCompletion_RESPONSE_INCOMPLETE, label: "response_incomplete"},
		{name: "unknown", completion: pb.NativeCompletion(999), want: pb.NativeCompletion_RESPONSE_INCOMPLETE, label: "response_incomplete"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			memory := testExecutionMemory(768 << 20)
			other := plan(0, "held", false)
			lease, failure, _ := memory.admit(other)
			if failure != nil {
				t.Fatal(failure)
			}
			defer lease.release()
			large := protocol.Fail(pb.FailureCode_UNAVAILABLE, strings.Repeat("f", 2<<20))
			end := &pb.NativeEnd{Completion: test.completion, Failure: large}
			if test.completion == pb.NativeCompletion_RESPONSE_COMPLETE {
				// Future protobuf fields can enlarge a valid complete terminal
				// without introducing an illegal RESPONSE_COMPLETE with Failure.
				end.Failure = nil
				unknown := protowire.AppendTag(nil, 100, protowire.BytesType)
				unknown = protowire.AppendBytes(unknown, bytes.Repeat([]byte("f"), 2<<20))
				end.ProtoReflect().SetUnknown(unknown)
			}
			value := &pb.Event_NativeEnd{NativeEnd: end}
			event := &pb.Event{Value: value}
			adapter := &nativeAccountingAdapter{events: []*pb.Event{event}}
			limits := DefaultLimits()
			runtime, err := New(adapter, limits, memory)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close(context.Background())
			session := runtime.NewSession()
			defer session.Close()
			work := plan(1, "native", false)
			work.Command = nativeCall()
			// The other owner's actual response leaves only the fixed event windows
			// and IO reserve. The terminal itself must not mint additional capacity.
			publication := uint64(max(work.ResultBytes, 2*protocol.NativeChunk+execution.ResultOverheadBytes) - nativeQueuedWindow)
			if started, _ := lease.start(int(memory.budget-memory.inputReserved-2*publication-1024), defaultNativeIOReserve); !started {
				t.Fatal("could not fill the other owner's reservation")
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			ticket, failure, _ := runtime.Submit(ctx, work, session)
			if failure != nil {
				t.Fatal(failure)
			}
			select {
			case emission := <-session.Events:
				terminal := emission.Event.GetNativeEnd()
				if terminal.GetCompletion() != test.want || terminal.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || proto.Size(emission.Event) >= protocol.NativeChunk {
					t.Fatal("oversized terminal escaped fixed bounded failure", terminal)
				}
				emission.Release()
			case <-ctx.Done():
				t.Fatal("bounded terminal was not delivered")
			}
			if snapshot := memory.Snapshot(); snapshot.UsedBytes > snapshot.BudgetBytes {
				t.Fatal("terminal broke the shared ownership bound", snapshot)
			}
			session.Close()
			for range 3 {
				ticket.Ack()
				want := map[string]float64{test.label: 1}
				assertNativeCompletions(t, runtime, want)
				assertRecordFailures(t, runtime, "read", 0)
			}
			waitReleased(t, runtime)
		})
	}
}
