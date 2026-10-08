package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func plan(index uint64, key string, read bool) *execution.Plan {
	command := &pb.Command{}
	bytes := execution.ResultOverheadBytes
	if read {
		request := &pb.ReadRequest{Resource: key}
		command.Operation = &pb.Command_Read{Read: request}
		bytes += protocol.MaxDocument
	} else {
		empty := &pb.Empty{}
		action := &pb.MutateRequest_Delete{Delete: empty}
		request := &pb.MutateRequest{Resource: key, Action: action}
		command.Operation = &pb.Command_Mutate{Mutate: request}
	}
	work := &execution.Plan{ID: index, Command: command, Key: key, Bytes: 1024, ResultBytes: bytes}
	return work
}
func recordEvent(t testing.TB, ticket *Ticket) *pb.Event {
	t.Helper()
	event, err := ticket.Wait(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return event
}
func finish(r *Runtime, b *batch) {
	b.cancel()
	r.workingBytes -= b.workingBytes
	delete(r.batches, b)
	for _, t := range b.items {
		var result *pb.Event
		if t.plan.Command.GetRead() != nil || t.plan.Command.GetMutate() != nil {
			result = execution.FailedEvent(t.plan.Command, pb.MutationOutcome_APPLIED, nil)
		}
		r.completeLocked(t, result)
	}
}
func TestAdmissionBoundsAndReservation(t *testing.T) {
	l := DefaultLimits()
	l.QueueBytes = 2 * l.BatchBytes
	l.BatchOperations = 1
	r := newRuntime(nil, l)
	defer func() {
		for t := range r.live {
			if t.stopWatch != nil {
				t.stopWatch()
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p := plan(0, "a", false)
	p.Bytes = l.QueueBytes / 2
	a, f, _ := r.Submit(ctx, p, nil)
	if f != nil {
		t.Fatal(f)
	}
	_, f, _ = r.Submit(ctx, p, nil)
	if f != nil {
		t.Fatal(f)
	}
	_, f, _ = r.Submit(ctx, p, nil)
	if f == nil || f.Code != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("queue full", f)
	}
	r.mu.Lock()
	b := r.selectLocked(time.Now())
	finish(r, b)
	r.mu.Unlock()
	_, f, _ = r.Submit(ctx, p, nil)
	if f != nil {
		t.Fatal("dispatch did not release waiting queue capacity", f)
	}
	a.Ack()
	r.SetOverloaded(true)
	if _, f, _ = r.Submit(ctx, p, nil); f == nil || f.Code != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("overload")
	}
	r.SetOverloaded(false)
	r.BeginDrain()
	if _, f, _ = r.Submit(ctx, p, nil); f == nil || f.Code != pb.FailureCode_UNAVAILABLE {
		t.Fatal("drain")
	}
}
func TestSameStreamOrderIndependentReadAndCancellation(t *testing.T) {
	l := DefaultLimits()
	l.BatchOperations = 1
	r := newRuntime(nil, l)
	s := r.NewSession()
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := plan(0, "key", false)
	a, _, _ := r.Submit(ctx, first, s)
	cancelled, stop := context.WithCancel(ctx)
	next := plan(1, "key", false)
	b, _, _ := r.Submit(cancelled, next, s)
	last := plan(2, "key", false)
	_, _, _ = r.Submit(ctx, last, s)
	independent := plan(0, "key", true)
	read, _, _ := r.Submit(ctx, independent, nil)
	r.mu.Lock()
	flight := r.selectLocked(time.Now())
	r.mu.Unlock()
	if flight.items[0] != a {
		t.Fatal("order")
	}
	stop()
	r.mu.Lock()
	r.cancelQueuedLocked()
	overlap := r.selectLocked(time.Now())
	r.mu.Unlock()
	if overlap == nil || overlap.items[0] != read {
		t.Fatal("independent same-key read was serialized or cancelled successor released active key")
	}
	if recordEvent(t, b).GetMutationResult().Outcome != pb.MutationOutcome_NOT_STARTED {
		t.Fatal("queued cancellation")
	}
	r.mu.Lock()
	if r.selectLocked(time.Now()) != nil {
		t.Fatal("same stream successor ran early")
	}
	finish(r, flight)
	finish(r, overlap)
	successor := r.selectLocked(time.Now())
	if successor == nil {
		t.Fatal("successor not released")
	}
	finish(r, successor)
	r.mu.Unlock()
	read.Ack()
}
func TestDispatchedCancellationPreservesBackendOutcome(t *testing.T) {
	for i := 0; i < 100; i++ {
		l := DefaultLimits()
		l.BatchOperations = 1
		r := newRuntime(nil, l)
		ctx, cancel := context.WithCancel(context.Background())
		p := plan(0, "a", false)
		ticket, _, _ := r.Submit(ctx, p, nil)
		r.mu.Lock()
		b := r.selectLocked(time.Now())
		r.mu.Unlock()
		cancel()
		r.mu.Lock()
		r.cancelQueuedLocked()
		if ticket.state != 1 {
			t.Fatal("dispatched operation moved backwards")
		}
		f := protocol.Fail(pb.FailureCode_CANCELLED, "lost acknowledgement")
		result := execution.FailedEvent(p.Command, pb.MutationOutcome_UNKNOWN, f)
		r.completeLocked(ticket, result)
		b.cancel()
		r.mu.Unlock()
		if recordEvent(t, ticket).GetMutationResult().Outcome != pb.MutationOutcome_UNKNOWN {
			t.Fatal("false NOT_STARTED")
		}
		ticket.Ack()
	}
}
func TestMicrobatchDeadlinesAndBounds(t *testing.T) {
	l := DefaultLimits()
	l.BatchOperations = 3
	r := newRuntime(nil, l)
	short, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	long, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		ctx := long
		if i == 0 {
			ctx = short
		}
		p := plan(uint64(i), fmt.Sprint(i), false)
		if _, f, _ := r.Submit(ctx, p, nil); f != nil {
			t.Fatal(f)
		}
	}
	r.mu.Lock()
	b := r.selectLocked(time.Now())
	r.mu.Unlock()
	defer b.cancel()
	deadline, _ := b.ctx.Deadline()
	if len(b.items) != 3 || time.Until(deadline) < 900*time.Millisecond {
		t.Fatal("earliest deadline poisoned shared batch")
	}
	stop()
	if b.ctx.Err() != nil {
		t.Fatal("participant cancelled batch")
	}
	r.mu.Lock()
	finish(r, b)
	r.mu.Unlock()
	for _, ticket := range b.items {
		ticket.Ack()
	}
}

func TestShutdownPreservesSynchronousResultEvidenceUntilAck(t *testing.T) {
	limits := DefaultLimits()
	adapter := &lifecycleAdapter{}
	runtime, err := New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	work := plan(1, "record", false)
	ticket, failure, _ := runtime.Submit(context.Background(), work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if runtime.Snapshot().Ready == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if runtime.Snapshot().Ready != 1 {
		t.Fatal("backend did not complete")
	}
	closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.Close(closeContext); err != nil {
		t.Fatal(err)
	}
	result, err := ticket.Wait(context.Background())
	if err != nil || result == nil || result.GetMutationResult().GetOutcome() != pb.MutationOutcome_UNKNOWN {
		t.Fatal("shutdown discarded write evidence", result, err)
	}
	if snapshot := runtime.Snapshot(); snapshot.Retained != 1 || snapshot.Active != 0 || snapshot.WorkingBytes != 0 {
		t.Fatal("consumer result was not separately retained", snapshot)
	}
	ticket.Ack()
	if snapshot := runtime.Snapshot(); snapshot.Retained != 0 || snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 {
		t.Fatal("synchronous consumer acknowledgment did not free result", snapshot)
	}
}

func TestCommandAdmissionRequiresNonzeroMetadataAndTerminalCharge(t *testing.T) {
	for _, resource := range []string{"input", "result"} {
		for _, streaming := range []bool{false, true} {
			t.Run(resource+fmt.Sprint(streaming), func(t *testing.T) {
				runtime := newRuntime(nil, DefaultLimits())
				work := plan(1, "command", false)
				work.Command = nil
				work.Command = scanCall()
				work.Command = scanCall()
				if streaming {
					work.Command = nativeCall()
				}
				if resource == "input" {
					work.Bytes = execution.EntryOverheadBytes - 1
				} else {
					work.ResultBytes = execution.ResultOverheadBytes - 1
				}
				session := runtime.NewSession()
				defer session.Close()
				if _, failure, _ := runtime.Submit(t.Context(), work, session); failure.GetCode() != pb.FailureCode_INVALID_ARGUMENT {
					t.Fatal("command bypassed metadata accounting", failure)
				}
				if snapshot := runtime.Snapshot(); snapshot.Pending != 0 || snapshot.Retained != 0 || snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 {
					t.Fatal("invalid metadata plan retained resources", snapshot)
				}
			})
		}
	}
}
