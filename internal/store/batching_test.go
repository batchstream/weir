package store

import (
	"context"
	"testing"
	"time"

	"github.com/batchstream/weir/api/protocol"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

var recordActions = []string{"read", "put", "create", "replace", "delete", "expression", "program"}

func recordActionPlan(index uint64, key, action string) *execution.Plan {
	p := plan(index, key, action == "read")
	if action == "read" || action == "delete" {
		return p
	}
	m := p.Operation.GetMutate()
	doc := &pb.Document{MediaType: "application/json", Data: []byte(`{"n":1}`)}
	switch action {
	case "put":
		m.Action = &pb.MutateRequest_Put{Put: doc}
	case "create":
		m.Action = &pb.MutateRequest_Create{Create: doc}
	case "replace":
		m.Action = &pb.MutateRequest_Replace{Replace: doc}
	case "expression":
		form := &pb.Transform_BackendExpression{BackendExpression: doc}
		transform := &pb.Transform{Form: form}
		m.Action = &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	case "program":
		program := &pb.ProgramTransform{Runtime: "lua.v1", Source: []byte(`return weir.keep()`)}
		form := &pb.Transform_Program{Program: program}
		transform := &pb.Transform{Form: form}
		m.Action = &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	default:
		panic("unknown record action")
	}
	return p
}

func TestAllRecordActionsShareBatch(t *testing.T) {
	limits := DefaultLimits()
	limits.Collect = 0
	r := newRuntime(nil, limits)
	session := r.NewSession()
	defer session.Close()
	for i, action := range recordActions {
		p := recordActionPlan(uint64(i), action+"/record", action)
		var s *Session
		if i%2 == 0 {
			s = session
		}
		if _, failure, _ := r.Submit(context.Background(), p, s); failure != nil {
			t.Fatal(failure)
		}
	}
	r.mu.Lock()
	b := r.selectLocked(time.Now())
	r.mu.Unlock()
	if b == nil || len(b.items) != len(recordActions) {
		t.Fatalf("all reads and mutations should share one batch: %v", b)
	}
	for i, ticket := range b.items {
		if ticket.plan.Operation.Index != uint64(i) {
			t.Fatal("mixed batch changed operation identity")
		}
	}
	r.mu.Lock()
	finish(r, b)
	r.mu.Unlock()
	for _, ticket := range b.items {
		ticket.Ack()
	}
	if snapshot := r.Snapshot(); snapshot.Retained != 0 || snapshot.Pending != 0 || snapshot.Active != 0 {
		t.Fatal("mixed batch leaked admission credits", snapshot)
	}
}

func TestMixedBatchRespectsBytesKeysAndSessionOrder(t *testing.T) {
	limits := DefaultLimits()
	limits.Collect = 0
	limits.BatchOperations = 3
	r := newRuntime(nil, limits)
	session := r.NewSession()
	defer session.Close()
	read := recordActionPlan(0, "same", "read")
	replace := recordActionPlan(1, "same", "replace")
	expression := recordActionPlan(2, "other", "expression")
	program := recordActionPlan(3, "last", "program")
	for _, p := range []*execution.Plan{read, replace, expression, program} {
		p.Bytes = limits.BatchBytes / 2
		if _, failure, _ := r.Submit(context.Background(), p, session); failure != nil {
			t.Fatal(failure)
		}
	}
	independent := recordActionPlan(0, "same", "read")
	independent.Bytes = limits.BatchBytes / 2
	ticket, failure, _ := r.Submit(context.Background(), independent, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	r.mu.Lock()
	first := r.selectLocked(time.Now())
	r.mu.Unlock()
	if first == nil || len(first.items) != 2 || first.items[0].plan != read || first.items[1].plan != expression {
		t.Fatal("record duplicate or byte limit ignored", first)
	}
	r.mu.Lock()
	second := r.selectLocked(time.Now())
	r.mu.Unlock()
	if second == nil || len(second.items) != 2 || second.items[0].plan != program || second.items[1] != ticket {
		t.Fatal("active session successor ran or independent record was blocked", second)
	}
	r.mu.Lock()
	premature := r.selectLocked(time.Now())
	r.mu.Unlock()
	if premature != nil {
		t.Fatal("Replace bypassed its earlier same-session Read")
	}
	r.mu.Lock()
	finish(r, first)
	third := r.selectLocked(time.Now())
	r.mu.Unlock()
	if third == nil || len(third.items) != 1 || third.items[0].plan != replace {
		t.Fatal("same-session successor not released", third)
	}
	r.mu.Lock()
	finish(r, second)
	finish(r, third)
	r.mu.Unlock()
	ticket.Ack()
}

func TestReadsCollectAndAnyParticipantDeadlineFlushes(t *testing.T) {
	limits := DefaultLimits()
	limits.Collect = 10 * time.Millisecond
	for _, expiringParticipant := range []bool{false, true} {
		r := newRuntime(nil, limits)
		base := time.Now()
		ctx, cancel := context.WithDeadline(context.Background(), base.Add(time.Second))
		first := recordActionPlan(0, "first", "read")
		a, failure, _ := r.Submit(context.Background(), first, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		second := recordActionPlan(1, "second", "read")
		b, failure, _ := r.Submit(ctx, second, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		now := base
		if expiringParticipant {
			now = base.Add(time.Second - 5*time.Millisecond)
		}
		r.mu.Lock()
		batch := r.selectLocked(now)
		r.mu.Unlock()
		if !expiringParticipant {
			if batch != nil {
				t.Fatal("reads bypassed the collection window")
			}
			r.mu.Lock()
			batch = r.selectLocked(now.Add(limits.Collect))
			r.mu.Unlock()
		}
		if batch == nil || len(batch.items) != 2 {
			t.Fatal("reads did not aggregate or later deadline did not flush", batch)
		}
		if batch.backendDeadline.Before(now.Add(limits.BackendTimeout)) {
			t.Fatal("short caller deadline constrained an independent caller")
		}
		r.mu.Lock()
		finish(r, batch)
		r.mu.Unlock()
		a.Ack()
		b.Ack()
		cancel()
	}
}

type recordBatchAdapter struct {
	scanTestAdapter
	started chan []*execution.Plan
	phases  chan *execution.Plan
	gate    <-chan struct{}
}

func (a *recordBatchAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) execution.Feedback {
	a.started <- plans
	select {
	case <-a.gate:
	case <-ctx.Done():
	}
	results := make([]*pb.Result, len(plans))
	for i, p := range plans {
		outcome := pb.MutationOutcome_APPLIED
		var failure *pb.Failure
		if p.Context.Err() != nil {
			outcome = pb.MutationOutcome_NOT_STARTED
			failure = protocol.ContextFailure(p.Context)
		} else if a.phases != nil {
			a.phases <- p
		}
		results[i] = protocol.ResultError(p.Operation, outcome, failure)
	}
	for i, result := range results {
		_ = emit(plans[i], resultEvent(result))
	}
	return execution.Healthy
}

func TestDispatchContextsDoNotMutateReusablePlans(t *testing.T) {
	gate := make(chan struct{})
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}()
	adapter := &recordBatchAdapter{started: make(chan []*execution.Plan, 2), gate: gate}
	limits := DefaultLimits()
	limits.Collect = 0
	r := newRuntime(adapter, limits)
	p := recordActionPlan(0, "same", "replace")
	firstContext, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	secondContext, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	first, failure, _ := r.Submit(firstContext, p, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	second, failure, _ := r.Submit(secondContext, p, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	r.mu.Lock()
	a := r.selectLocked(time.Now())
	b := r.selectLocked(time.Now())
	r.mu.Unlock()
	go r.execute(a)
	dispatchedFirst := <-adapter.started
	go r.execute(b)
	dispatchedSecond := <-adapter.started
	if p.Context != nil || dispatchedFirst[0] == p || dispatchedSecond[0] == p || dispatchedFirst[0] == dispatchedSecond[0] {
		t.Fatal("dispatch modified a reusable prepared plan")
	}
	if dispatchedFirst[0].Context != first.ctx || dispatchedSecond[0].Context != second.ctx {
		t.Fatal("caller contexts were crossed between dispatches")
	}
	cancelFirst()
	if dispatchedSecond[0].Context.Err() != nil || a.ctx.Err() != nil || b.ctx.Err() != nil {
		t.Fatal("one caller cancellation poisoned independent execution")
	}
	close(gate)
	if first.Result().GetMutation().Outcome != pb.MutationOutcome_NOT_STARTED || second.Result().GetMutation().Outcome != pb.MutationOutcome_APPLIED {
		t.Fatal("caller cancellation lost per-item identity")
	}
	first.Ack()
	second.Ack()
}

func TestAbandonAndSessionCloseCancelFuturePhasesOnly(t *testing.T) {
	for _, closeSession := range []bool{false, true} {
		name := "abandon"
		if closeSession {
			name = "session_close"
		}
		t.Run(name, func(t *testing.T) {
			gate := make(chan struct{})
			defer func() {
				select {
				case <-gate:
				default:
					close(gate)
				}
			}()
			adapter := &recordBatchAdapter{started: make(chan []*execution.Plan, 1), phases: make(chan *execution.Plan, 2), gate: gate}
			limits := DefaultLimits()
			limits.Collect = 0
			r := newRuntime(adapter, limits)
			original, cancel := context.WithCancel(context.Background())
			defer cancel()
			var session *Session
			if closeSession {
				session = r.NewSession()
				defer session.Close()
			}
			first := recordActionPlan(0, "peer", "create")
			peer, failure, _ := r.Submit(original, first, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			second := recordActionPlan(1, "abandoned", "replace")
			abandoned, failure, _ := r.Submit(original, second, session)
			if failure != nil {
				t.Fatal(failure)
			}
			r.mu.Lock()
			batch := r.selectLocked(time.Now())
			r.mu.Unlock()
			go r.execute(batch)
			dispatched := <-adapter.started
			if len(dispatched) != 2 {
				t.Fatal("test operations did not share a batch")
			}
			if closeSession {
				session.Close()
			} else {
				abandoned.Abandon()
			}
			if dispatched[1].Context.Err() != context.Canceled || dispatched[0].Context.Err() != nil || original.Err() != nil || batch.ctx.Err() != nil {
				t.Fatal("abandonment did not cancel only its own future phases")
			}
			if snapshot := r.Snapshot(); snapshot.Active != 1 || snapshot.Retained != 2 {
				t.Fatal("abandonment released an in-flight reservation", snapshot)
			}
			close(gate)
			if peer.Result().GetMutation().Outcome != pb.MutationOutcome_APPLIED {
				t.Fatal("abandonment prevented a peer's already admitted work")
			}
			<-abandoned.ready
			peer.Ack()
			if len(adapter.phases) != 1 || (<-adapter.phases).Operation.Index != first.Operation.Index {
				t.Fatal("abandoned item started its next phase")
			}
			if snapshot := r.Snapshot(); snapshot.Active != 0 || snapshot.Retained != 0 || snapshot.ResultBytes != 0 {
				t.Fatal("terminal abandoned record leaked credits", snapshot)
			}
		})
	}
}

func TestEveryRecordActionSharesConcurrencyLimit(t *testing.T) {
	gate := make(chan struct{})
	adapter := &recordBatchAdapter{started: make(chan []*execution.Plan, len(recordActions)), gate: gate}
	limits := DefaultLimits()
	limits.Concurrency = 1
	limits.BatchOperations = 1
	r, err := New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
		_ = r.Close(context.Background())
	}()
	var tickets []*Ticket
	for i, action := range recordActions {
		p := recordActionPlan(uint64(i), action, action)
		ticket, failure, _ := r.Submit(context.Background(), p, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	select {
	case <-adapter.started:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not dispatch the first operation")
	}
	select {
	case <-adapter.started:
		t.Fatal("operation bypassed the shared concurrency limit")
	case <-time.After(20 * time.Millisecond):
	}
	if snapshot := r.Snapshot(); snapshot.Active != 1 || snapshot.Pending != len(recordActions)-1 {
		t.Fatal("record ledger does not bound every operation", snapshot)
	}
	close(gate)
	for _, ticket := range tickets {
		wait, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := ticket.Wait(wait)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		ticket.Ack()
	}
}
