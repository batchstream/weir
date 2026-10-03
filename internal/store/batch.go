package store

import (
	"context"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

// PreparedBatch owns one validated request. Its record plans never become
// independent admission entries, publishers, cancellation watchers or RPCs.
type PreparedBatch struct {
	plans        []*execution.Plan
	bytes        int
	resultBytes  int
	workingBytes int
}

func (r *Runtime) PrepareBatch(operations []*pb.Operation) (*PreparedBatch, *pb.Failure) {
	prepared := &PreparedBatch{plans: make([]*execution.Plan, len(operations))}
	if len(operations) == 0 {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "empty record batch")
	}
	for position, operation := range operations {
		plan, failure := r.adapter.PrepareOperation(operation)
		if failure != nil {
			r.metrics.rejections.WithLabelValues("prepare").Inc()
			return nil, failure
		}
		if plan == nil || plan.Operation == nil || plan.Streaming || plan.CleanupRequired || plan.ID != uint64(position+1) {
			return nil, protocol.Fail(pb.FailureCode_INTERNAL, "invalid prepared record")
		}
		prepared.plans[position] = plan
		prepared.bytes += plan.Bytes
		// Terminal envelopes always have space; only actual copied read data
		// increases this charge. The read size limit is not a reservation.
		prepared.resultBytes += protocol.ResultOverhead
		prepared.workingBytes = max(prepared.workingBytes, plan.WorkingBytes)
		if prepared.bytes > r.limits.PendingBytes || prepared.resultBytes > min(r.limits.ResultBytes, protocol.MaxBatchResponseBytes) {
			return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "record batch exceeds Store memory bounds")
		}
	}
	return prepared, nil
}

func (r *Runtime) SubmitBatch(ctx context.Context, prepared *PreparedBatch) (*Ticket, *pb.Failure, <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	changed := r.changed
	if r.draining || r.closed {
		return nil, protocol.Fail(pb.FailureCode_UNAVAILABLE, "store draining"), changed
	}
	if r.overloaded {
		return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "process overloaded"), changed
	}
	if ctx.Err() != nil {
		return nil, protocol.ContextFailure(ctx), changed
	}
	if prepared == nil || len(prepared.plans) == 0 || prepared.workingBytes > r.limits.WorkingBytes {
		return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "record batch cannot fit bounded execution"), changed
	}
	if len(r.queue) >= r.limits.PendingOperations || prepared.bytes > r.limits.PendingBytes-r.pendingBytes || len(r.live) >= r.limits.ResultOperations || prepared.resultBytes > r.limits.ResultBytes-r.resultBytes {
		return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "admission capacity exhausted"), changed
	}
	ticketContext, cancel := context.WithCancel(ctx)
	ticket := &Ticket{runtime: r, ctx: ticketContext, cancel: cancel, bulk: prepared, ready: make(chan struct{}), queuedAt: time.Now(), results: make([]*pb.Result, len(prepared.plans)), resultCharge: prepared.resultBytes}
	ticket.stopWatch = context.AfterFunc(ticketContext, r.signal)
	r.queue = append(r.queue, ticket)
	r.live[ticket] = struct{}{}
	r.pendingBytes += prepared.bytes
	r.resultBytes += prepared.resultBytes
	r.signal()
	return ticket, nil, changed
}

func (t *Ticket) WaitBatch(ctx context.Context) ([]*pb.Result, error) {
	select {
	case <-t.ready:
		t.runtime.mu.Lock()
		results := t.results
		t.runtime.mu.Unlock()
		return results, nil
	case <-ctx.Done():
		t.Abandon()
		return nil, ctx.Err()
	}
}

func (t *Ticket) retainResults(bytes int) bool {
	r := t.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.released || bytes > r.limits.ResultBytes-r.resultBytes {
		return false
	}
	r.resultBytes += bytes
	t.resultCharge += bytes
	return true
}

func (r *Runtime) completeBatchLocked(t *Ticket, failure *pb.Failure) {
	for i, plan := range t.bulk.plans {
		if t.results[i] == nil {
			outcome := pb.MutationOutcome_UNKNOWN
			if t.state == 0 {
				outcome = pb.MutationOutcome_NOT_STARTED
			}
			if failure == nil {
				failure = protocol.Fail(pb.FailureCode_INTERNAL, "adapter returned no record result")
			}
			t.results[i] = protocol.ResultError(plan.Operation, outcome, failure)
		}
		r.terminalResultLocked(plan, t.results[i])
	}
	t.state = 2
	if t.stopWatch != nil {
		t.stopWatch()
	}
	close(t.ready)
	if t.abandoned {
		r.releaseLocked(t)
	}
}

// Record groups respect actual native request bytes, target namespaces and
// duplicate mutation order. Different keys remain batched in the same wave.
func (r *Runtime) recordGroups(plans []*execution.Plan) [][]*execution.Plan {
	var groups [][]*execution.Plan
	var group []*execution.Plan
	seen := make(map[string]bool)
	bytes := 0
	var seed *execution.Plan
	flush := func() {
		if len(group) != 0 {
			groups = append(groups, group)
		}
		group = nil
		seed = nil
		bytes = 0
		clear(seen)
	}
	for _, plan := range plans {
		duplicate := plan.Operation.GetMutate() != nil && seen[plan.Key]
		if len(group) != 0 && (seed.Singleton || plan.Singleton || seed.BatchKey != plan.BatchKey || duplicate || len(group) >= r.limits.BatchOperations || plan.Bytes > r.limits.BatchBytes-bytes) {
			flush()
		}
		if seed == nil {
			seed = plan
		}
		group = append(group, plan)
		seen[plan.Key] = true
		bytes += plan.Bytes
	}
	flush()
	return groups
}

func (r *Runtime) executeBatch(ctx context.Context, ticket *Ticket) execution.Feedback {
	budget := &execution.ResultBudget{Limit: protocol.MaxBatchResponseBytes, Used: ticket.resultCharge, Retain: ticket.retainResults}
	plans := make([]*execution.Plan, len(ticket.bulk.plans))
	for i, original := range ticket.bulk.plans {
		plan := *original
		plan.Context = ticket.ctx
		plan.BackendTimeout = r.limits.BackendTimeout
		plan.Results = budget
		plans[i] = &plan
	}
	feedback := execution.Healthy
	for _, group := range r.recordGroups(plans) {
		if ctx.Err() != nil || ticket.ctx.Err() != nil {
			caller := ctx
			if ticket.ctx.Err() != nil {
				caller = ticket.ctx
			}
			for _, plan := range group {
				ticket.results[plan.ID-1] = protocol.ResultError(plan.Operation, pb.MutationOutcome_NOT_STARTED, protocol.ContextFailure(caller))
			}
			feedback = execution.Neutral
			continue
		}
		r.metrics.executions.WithLabelValues("route").Inc()
		r.metrics.batch.Observe(float64(len(group)))
		emit := func(plan *execution.Plan, output *execution.Output) error {
			if output == nil || output.Result == nil || output.Event != nil || plan.ID == 0 || plan.ID > uint64(len(plans)) || output.Result.Index != plan.ID || ticket.results[plan.ID-1] != nil {
				return context.Canceled
			}
			ticket.results[plan.ID-1] = output.Result
			return nil
		}
		sample := r.adapter.Execute(ctx, group, emit)
		if sample == execution.Congested || feedback != execution.Congested && sample == execution.Neutral {
			feedback = sample
		}
	}
	return feedback
}

func (r *Runtime) selectBatchLocked(ticket *Ticket, now time.Time) *batch {
	deadline := now.Add(r.limits.BackendTimeout)
	owned := true
	if callerDeadline, ok := ticket.ctx.Deadline(); ok && callerDeadline.Before(deadline) {
		deadline = callerDeadline
		owned = false
	}
	ctx, cancel := context.WithDeadline(ticket.ctx, deadline)
	items := []*Ticket{ticket}
	b := &batch{ctx: ctx, cancel: cancel, items: items, backendDeadline: deadline, timeoutOwned: owned, workingBytes: ticket.bulk.workingBytes}
	for i, queued := range r.queue {
		if queued == ticket {
			copy(r.queue[i:], r.queue[i+1:])
			r.queue[len(r.queue)-1] = nil
			r.queue = r.queue[:len(r.queue)-1]
			break
		}
	}
	r.metrics.queue.WithLabelValues("route").Observe(now.Sub(ticket.queuedAt).Seconds())
	ticket.state = 1
	r.active++
	r.workingBytes += b.workingBytes
	r.batches[b] = struct{}{}
	r.notifyLocked()
	return b
}

func (r *Runtime) runBatch(b *batch) {
	ticket := b.items[0]
	started := time.Now()
	feedback := r.executeBatch(b.ctx, ticket)
	r.metrics.duration.WithLabelValues("route").Observe(time.Since(started).Seconds())
	if ticket.ctx.Err() != nil {
		feedback = execution.Neutral
	} else if b.ctx.Err() != nil {
		if b.timeoutOwned {
			feedback = execution.Congested
		}
	}
	b.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active--
	r.workingBytes -= b.workingBytes
	delete(r.batches, b)
	if r.closed {
		feedback = execution.Neutral
	}
	var failure *pb.Failure
	if ticket.ctx.Err() != nil {
		failure = protocol.ContextFailure(ticket.ctx)
	}
	r.completeBatchLocked(ticket, failure)
	r.metrics.feedback, r.metrics.observed = feedback, true
	r.notifyLocked()
}
