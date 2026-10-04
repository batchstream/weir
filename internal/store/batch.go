package store

import (
	"context"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

// PreparedBatch owns one bounded record window. Its plans never become
// independent admission entries, publishers, cancellation watchers or RPCs.
type PreparedBatch struct {
	plans        []*execution.Plan
	bytes        int
	resultBytes  int
	workingBytes int
}

// PendingByteLimit is immutable for the lifetime of a Store. Preflight uses it
// before retaining decoded paths and constructing backend plans.
func (r *Runtime) PendingByteLimit() int { return r.limits.PendingBytes }

func (r *Runtime) PrepareBatch(records []*execution.Record) (*PreparedBatch, *pb.Failure) {
	prepared := &PreparedBatch{plans: make([]*execution.Plan, len(records))}
	if len(records) == 0 {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "empty record batch")
	}
	for position, record := range records {
		if record == nil || record.Operation() == nil || record.Operation().Index != uint64(position+1) {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid record batch ordinal")
		}
		plan, failure := r.adapter.PrepareRecord(record)
		if failure != nil {
			r.metrics.rejections.WithLabelValues("prepare").Inc()
			return nil, failure
		}
		if plan == nil || plan.Operation == nil || plan.Streaming || plan.CleanupRequired || plan.ID != uint64(position+1) {
			return nil, protocol.Fail(pb.FailureCode_INTERNAL, "invalid prepared record")
		}
		// Core metadata includes each RPC's ticket, context, watcher and result
		// table. An adapter's native declaration cannot discount that floor.
		plan.Bytes = max(plan.Bytes, record.Bytes())
		prepared.plans[position] = plan
		prepared.bytes += plan.Bytes
		// Reserve the configured maximum before database work starts. Shared
		// result pressure delays admission rather than failing a copied document.
		prepared.resultBytes += max(plan.ResultBytes, execution.ResultOverheadBytes)
		prepared.workingBytes = max(prepared.workingBytes, plan.WorkingBytes)
		if prepared.bytes > r.limits.PendingBytes || prepared.resultBytes > r.limits.ResultBytes {
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
	if prepared.bytes > r.limits.PendingBytes-r.pendingBytes || prepared.resultBytes > r.limits.ResultBytes-r.resultBytes {
		return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "admission capacity exhausted"), changed
	}
	ticketContext, cancel := context.WithCancel(ctx)
	ticket := &Ticket{runtime: r, ctx: ticketContext, cancel: cancel, bulk: prepared, ready: make(chan struct{}), queuedAt: time.Now(), results: make([]*execution.Result, len(prepared.plans)), resultCharge: prepared.resultBytes}
	ticket.stopWatch = context.AfterFunc(ticketContext, r.signal)
	r.queue = append(r.queue, ticket)
	r.live[ticket] = struct{}{}
	r.pendingBytes += prepared.bytes
	r.resultBytes += prepared.resultBytes
	r.signal()
	return ticket, nil, changed
}

func (t *Ticket) WaitBatch(ctx context.Context) ([]*execution.Result, error) {
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
			t.results[i] = execution.FailedResult(plan.Operation, outcome, failure)
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
		duplicate := plan.Operation.Mutate != nil && seen[plan.Key]
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

func (r *Runtime) executeBatch(ctx context.Context, tickets []*Ticket) execution.Feedback {
	count := 0
	for _, ticket := range tickets {
		count += len(ticket.bulk.plans)
	}
	plans := make([]*execution.Plan, 0, count)
	owners := make(map[*execution.Plan]*Ticket, count)
	for _, ticket := range tickets {
		budget := &execution.ResultBudget{Limit: ticket.resultCharge, Used: len(ticket.bulk.plans) * execution.ResultOverheadBytes}
		for _, original := range ticket.bulk.plans {
			plan := *original
			plan.Context = ticket.ctx
			plan.BackendTimeout = r.limits.BackendTimeout
			plan.Results = budget
			plans = append(plans, &plan)
			owners[&plan] = ticket
		}
	}
	feedback := execution.Healthy
	for _, group := range r.recordGroups(plans) {
		ready := group[:0]
		for _, plan := range group {
			ticket := owners[plan]
			caller := ctx
			if ticket.ctx.Err() != nil {
				caller = ticket.ctx
			}
			if caller.Err() != nil {
				ticket.results[plan.ID-1] = execution.FailedResult(plan.Operation, pb.MutationOutcome_NOT_STARTED, protocol.ContextFailure(caller))
				continue
			}
			ready = append(ready, plan)
		}
		if len(ready) == 0 {
			feedback = execution.Neutral
			continue
		}
		r.metrics.executions.WithLabelValues("execution").Inc()
		r.metrics.batch.Observe(float64(len(ready)))
		emit := func(plan *execution.Plan, output *execution.Output) error {
			ticket := owners[plan]
			if ticket == nil || output == nil || output.Result == nil || output.Event != nil || plan.ID == 0 || plan.ID > uint64(len(ticket.results)) || output.Result.Index != plan.ID || ticket.results[plan.ID-1] != nil {
				return context.Canceled
			}
			ticket.results[plan.ID-1] = output.Result
			return nil
		}
		sample := r.adapter.Execute(ctx, ready, emit)
		if sample == execution.Congested || feedback != execution.Congested && sample == execution.Neutral {
			feedback = sample
		}
	}
	return feedback
}

// Compatible bounded windows from different streams share database execution.
func (r *Runtime) batchKey(prepared *PreparedBatch) (string, bool) {
	if len(prepared.plans) > r.limits.BatchOperations || prepared.bytes > r.limits.BatchBytes {
		return "", false
	}
	key := prepared.plans[0].BatchKey
	for _, plan := range prepared.plans {
		if plan.Singleton || plan.BatchKey != key {
			return "", false
		}
	}
	return key, true
}

func (r *Runtime) selectBatchLocked(ticket *Ticket, now time.Time) *batch {
	items := []*Ticket{ticket}
	selected := map[*Ticket]bool{ticket: true}
	workingBytes := ticket.bulk.workingBytes
	key, merge := r.batchKey(ticket.bulk)
	if merge && len(ticket.bulk.plans) < r.limits.BatchOperations {
		count, bytes := len(ticket.bulk.plans), ticket.bulk.bytes
		for _, queued := range r.queue {
			if queued == ticket || queued.bulk == nil || queued.state != 0 || queued.ctx.Err() != nil || queued.abandoned {
				continue
			}
			candidate := queued.bulk
			candidateKey, compatible := r.batchKey(candidate)
			if !compatible || candidateKey != key || len(candidate.plans) > r.limits.BatchOperations-count || candidate.bytes > r.limits.BatchBytes-bytes || candidate.workingBytes > r.limits.WorkingBytes-r.workingBytes {
				continue
			}
			items = append(items, queued)
			selected[queued] = true
			count += len(candidate.plans)
			bytes += candidate.bytes
			workingBytes = max(workingBytes, candidate.workingBytes)
			if count == r.limits.BatchOperations {
				break
			}
		}
	}
	deadline := now.Add(r.limits.BackendTimeout)
	latest := now
	for _, item := range items {
		callerDeadline, ok := item.ctx.Deadline()
		if !ok {
			callerDeadline = deadline
		}
		if callerDeadline.After(latest) {
			latest = callerDeadline
		}
	}
	owned := !deadline.After(latest)
	if latest.Before(deadline) {
		deadline = latest
	}
	// The execution belongs to all callers; a seed cancellation or an earlier
	// caller deadline must not cancel another caller's database operation.
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	b := &batch{ctx: ctx, cancel: cancel, items: items, backendDeadline: deadline, timeoutOwned: owned, workingBytes: workingBytes}
	keep := r.queue[:0]
	for _, queued := range r.queue {
		if selected[queued] {
			r.metrics.queue.WithLabelValues("execution").Observe(now.Sub(queued.queuedAt).Seconds())
			queued.state = 1
		} else {
			keep = append(keep, queued)
		}
	}
	clear(r.queue[len(keep):])
	r.queue = keep
	r.active++
	r.workingBytes += b.workingBytes
	r.batches[b] = struct{}{}
	r.notifyLocked()
	return b
}

func (r *Runtime) runBatch(b *batch) {
	started := time.Now()
	feedback := r.executeBatch(b.ctx, b.items)
	r.metrics.duration.WithLabelValues("execution").Observe(time.Since(started).Seconds())
	interested := false
	for _, ticket := range b.items {
		interested = interested || ticket.ctx.Err() == nil
	}
	if !interested {
		feedback = execution.Neutral
	} else if b.ctx.Err() != nil {
		if b.timeoutOwned {
			feedback = execution.Congested
		}
	}
	var sharedFailure *pb.Failure
	if b.ctx.Err() != nil {
		sharedFailure = protocol.ContextFailure(b.ctx)
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
	for _, ticket := range b.items {
		failure := sharedFailure
		if ticket.ctx.Err() != nil {
			failure = protocol.ContextFailure(ticket.ctx)
		}
		r.completeBatchLocked(ticket, failure)
	}
	r.metrics.feedback, r.metrics.observed = feedback, true
	r.notifyLocked()
}
