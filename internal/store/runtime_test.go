package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
)

func plan(index uint64, key string, read bool) *execution.Plan {
	op := &pb.BulkOperation{Index: index}
	token := "write"
	bytes := protocol.ResultOverhead
	if read {
		r := &pb.ReadRequest{Resource: key}
		op.Operation = &pb.BulkOperation_Read{Read: r}
		token = "read:" + key
		bytes += protocol.MaxDocument
	} else {
		e := &pb.Empty{}
		a := &pb.MutateRequest_Delete{Delete: e}
		m := &pb.MutateRequest{Resource: key, Action: a}
		op.Operation = &pb.BulkOperation_Mutate{Mutate: m}
	}
	p := &execution.Plan{Operation: op, Key: key, Token: token, Bytes: 1024, ResultBytes: bytes, Batchable: !read}
	return p
}
func finish(r *Runtime, b *batch) {
	b.cancel()
	r.active--
	delete(r.batches, b)
	for _, t := range b.items {
		result := protocol.ResultError(t.plan.Operation, pb.MutationOutcome_APPLIED, nil)
		r.completeLocked(t, result)
	}
}
func TestAdmissionBoundsAndReservation(t *testing.T) {
	l := DefaultLimits()
	l.PendingOperations = 2
	l.ResultOperations = 2
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
	if f == nil {
		t.Fatal("unacked result must retain credit")
	}
	a.Ack()
	if _, f, _ = r.Submit(ctx, p, nil); f != nil {
		t.Fatal(f)
	}
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
	last := plan(2, "key", true)
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
	if b.Result().GetMutation().Outcome != pb.MutationOutcome_NOT_STARTED {
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
func TestDispatchCancellationIsNeverNotStarted(t *testing.T) {
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
		result := protocol.ResultError(p.Operation, pb.MutationOutcome_UNKNOWN, f)
		r.completeLocked(ticket, result)
		b.cancel()
		r.mu.Unlock()
		if ticket.Result().GetMutation().Outcome != pb.MutationOutcome_UNKNOWN {
			t.Fatal("false NOT_STARTED")
		}
		ticket.Ack()
	}
}
func TestMicrobatchDeadlinesAndBounds(t *testing.T) {
	l := DefaultLimits()
	l.Collect = 0
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
	if len(b.items) != 3 || time.Until(b.backendDeadline) < 900*time.Millisecond {
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
func TestSlowConsumerRetainedBound(t *testing.T) {
	l := DefaultLimits()
	l.BatchOperations = 1
	l.SessionOutstanding = 2
	r := newRuntime(nil, l)
	s := r.NewSession()
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		p := plan(uint64(i), fmt.Sprint(i), true)
		_, f, _ := r.Submit(ctx, p, s)
		if f != nil {
			t.Fatal(f)
		}
		r.mu.Lock()
		b := r.selectLocked(time.Now())
		finish(r, b)
		r.mu.Unlock()
	}
	p := plan(2, "third", true)
	for i := 0; i < 10000; i++ {
		if _, f, _ := r.Submit(ctx, p, s); f == nil {
			t.Fatal("unbounded retained results")
		}
	}
	snap := r.Snapshot()
	if snap.Retained != 2 || snap.Active != 0 || snap.Pending != 0 {
		t.Fatal(snap)
	}
	s.Close()
	if r.Snapshot().Retained != 0 {
		t.Fatal("result credits leaked")
	}
}
func TestAIMDEpochAndFloor(t *testing.T) {
	c := controller{window: 4}
	b := &batch{epoch: 0, saturated: true}
	now := time.Now()
	c.observe(b, execution.Congested, 8, now)
	if c.window != 2 {
		t.Fatal(c)
	}
	c.observe(b, execution.Congested, 8, now)
	if c.window != 2 {
		t.Fatal("old flight counted twice")
	}
	for i := 0; i < 4; i++ {
		b.epoch = c.epoch
		c.observe(b, execution.Congested, 8, now)
	}
	if c.window != 1 {
		t.Fatal("Cmin")
	}
	b.epoch = c.epoch
	for i := 0; i < 4; i++ {
		c.observe(b, execution.Healthy, 8, now.Add(time.Second))
	}
	if c.window != 2 {
		t.Fatal("no healthy growth", c)
	}
}
