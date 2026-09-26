// Package store owns the single bounded admission ledger and scheduler for one Store.
// Documents and adapter compatibility tokens remain opaque here.
package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
)

type Limits struct {
	PendingOperations, PendingBytes, ResultOperations, ResultBytes int
	Concurrency, BatchOperations, BatchBytes, SessionOutstanding   int
	Collect, BackendTimeout                                        time.Duration
}

func DefaultLimits() Limits {
	l := Limits{PendingOperations: 256, PendingBytes: 8 << 20, ResultOperations: 128, ResultBytes: 16 << 20, Concurrency: 4, BatchOperations: 16, BatchBytes: 1 << 20, SessionOutstanding: 8, Collect: time.Millisecond, BackendTimeout: 2 * time.Second}
	return l
}
func (l Limits) Validate() error {
	if l.PendingOperations < 1 || l.PendingOperations > 4096 || l.PendingBytes < protocol.MaxFrame || l.ResultOperations < 1 || l.ResultOperations > 4096 || l.ResultBytes < protocol.MaxDocument+protocol.ResultOverhead || l.Concurrency < 1 || l.Concurrency > 32 || l.BatchOperations < 1 || l.BatchOperations > 128 || l.BatchBytes < protocol.MaxFrame || l.BatchBytes > 8<<20 || l.SessionOutstanding < 1 || l.SessionOutstanding > 32 || l.Collect < 0 || l.Collect > 10*time.Millisecond || l.BackendTimeout <= 0 || l.BackendTimeout > 10*time.Second {
		return fmt.Errorf("invalid runtime bounds")
	}
	return nil
}

type Runtime struct {
	mu                           sync.Mutex
	adapter                      execution.Adapter
	limits                       Limits
	queue                        []*Ticket
	live                         map[*Ticket]struct{}
	batches                      map[*batch]struct{}
	keys                         map[string]bool
	pendingBytes, resultBytes    int
	active                       int
	nextSession                  uint64
	draining, closed, overloaded bool
	wake                         chan struct{}
	changed                      chan struct{}
	done                         chan struct{}
	closeOnce                    sync.Once
	closeErr                     error
	controller                   controller
}
type Session struct {
	runtime     *Runtime
	id          uint64
	Results     chan *Ticket
	outstanding int
	closed      bool
}
type Ticket struct {
	runtime          *Runtime
	session          *Session
	ctx              context.Context
	plan             *execution.Plan
	result           *pb.BulkResult
	ready            chan struct{}
	sequence         string
	eligible         time.Time
	state            uint8
	abandoned, acked bool
	stopWatch        func() bool
}
type batch struct {
	ctx             context.Context
	items           []*Ticket
	cancel          context.CancelFunc
	epoch           uint64
	saturated       bool
	backendDeadline time.Time
	timeoutOwned    bool
}
type Snapshot struct {
	Pending, PendingBytes, Active, Retained, ResultBytes, Window int
	Draining, Closed, Overloaded                                 bool
}

// New takes ownership of a constructed adapter, including cleanup on validation failure.
func New(a execution.Adapter, limits Limits) (*Runtime, error) {
	if a == nil {
		return nil, fmt.Errorf("missing adapter")
	}
	if err := limits.Validate(); err != nil {
		_ = a.Close()
		return nil, err
	}
	r := newRuntime(a, limits)
	go r.loop()
	return r, nil
}
func newRuntime(a execution.Adapter, l Limits) *Runtime {
	r := &Runtime{adapter: a, limits: l, live: make(map[*Ticket]struct{}), batches: make(map[*batch]struct{}), keys: make(map[string]bool), wake: make(chan struct{}, 1), changed: make(chan struct{}), done: make(chan struct{})}
	r.controller.window = 1
	return r
}
func (r *Runtime) NewSession() *Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextSession++
	s := &Session{runtime: r, id: r.nextSession, Results: make(chan *Ticket, r.limits.SessionOutstanding)}
	return s
}
func (r *Runtime) Prepare(op *pb.BulkOperation) (*execution.Plan, *pb.Failure) {
	return r.adapter.Prepare(op)
}
func (r *Runtime) Submit(ctx context.Context, p *execution.Plan, s *Session) (*Ticket, *pb.Failure, <-chan struct{}) {
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
	if s != nil && (s.runtime != r || s.closed) {
		return nil, protocol.Fail(pb.FailureCode_UNAVAILABLE, "session closed"), changed
	}
	if p.Bytes > r.limits.BatchBytes || p.Bytes > r.limits.PendingBytes {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "operation cannot fit singleton bound"), changed
	}
	if len(r.queue) >= r.limits.PendingOperations || r.pendingBytes+p.Bytes > r.limits.PendingBytes || len(r.live) >= r.limits.ResultOperations || r.resultBytes+p.ResultBytes > r.limits.ResultBytes || s != nil && s.outstanding >= r.limits.SessionOutstanding {
		return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "admission capacity exhausted"), changed
	}
	t := &Ticket{runtime: r, session: s, ctx: ctx, plan: p, ready: make(chan struct{})}
	if s != nil {
		s.outstanding++
		t.sequence = fmt.Sprintf("%d:%s", s.id, p.Key)
	}
	r.queue = append(r.queue, t)
	r.live[t] = struct{}{}
	r.pendingBytes += p.Bytes
	r.resultBytes += p.ResultBytes
	t.stopWatch = context.AfterFunc(ctx, r.signal)
	r.signal()
	return t, nil, changed
}
func (r *Runtime) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}
func (r *Runtime) notifyLocked() { close(r.changed); r.changed = make(chan struct{}); r.signal() }
func (t *Ticket) Wait(ctx context.Context) (*pb.BulkResult, error) {
	select {
	case <-t.ready:
		return t.result, nil
	case <-ctx.Done():
		t.Abandon()
		return nil, ctx.Err()
	}
}
func (t *Ticket) Result() *pb.BulkResult { <-t.ready; return t.result }
func (t *Ticket) Ack() {
	r := t.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.state == 2 {
		r.releaseLocked(t)
	}
}
func (t *Ticket) Abandon() {
	r := t.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	t.abandoned = true
	if t.state == 2 {
		r.releaseLocked(t)
	}
	r.signal()
}
func (s *Session) Close() {
	r := s.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	s.closed = true
	for t := range r.live {
		if t.session == s {
			t.abandoned = true
			if t.state == 2 {
				r.releaseLocked(t)
			}
		}
	}
	r.signal()
}
func (r *Runtime) releaseLocked(t *Ticket) {
	if t.acked {
		return
	}
	t.acked = true
	delete(r.live, t)
	r.resultBytes -= t.plan.ResultBytes
	if t.session != nil {
		t.session.outstanding--
	}
	t.plan = nil
	t.result = nil
	r.notifyLocked()
}
func (r *Runtime) completeLocked(t *Ticket, result *pb.BulkResult) {
	prior := t.state
	t.state = 2
	t.result = result
	if t.stopWatch != nil {
		t.stopWatch()
	}
	close(t.ready)
	if prior == 1 && t.sequence != "" {
		delete(r.keys, t.sequence)
	}
	if t.abandoned || t.session != nil && t.session.closed {
		r.releaseLocked(t)
	} else if t.session != nil {
		t.session.Results <- t
	}
	r.notifyLocked()
}
func (r *Runtime) loop() {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	defer close(r.done)
	for {
		select {
		case <-r.wake:
		case <-ticker.C:
		}
		r.mu.Lock()
		r.cancelQueuedLocked()
		for b := range r.batches {
			interested := false
			for _, t := range b.items {
				if t.ctx.Err() == nil && !t.abandoned {
					interested = true
					break
				}
			}
			if !interested {
				b.cancel()
			}
		}
		for !r.closed && r.active < r.controller.window && time.Now().After(r.controller.cooldown) {
			b := r.selectLocked(time.Now())
			if b == nil {
				break
			}
			go r.execute(b)
		}
		finish := r.draining && len(r.queue) == 0 && r.active == 0
		r.mu.Unlock()
		if finish {
			return
		}
	}
}
func (r *Runtime) cancelQueuedLocked() {
	keep := r.queue[:0]
	for _, t := range r.queue {
		if t.ctx.Err() != nil || t.abandoned || r.closed {
			r.pendingBytes -= t.plan.Bytes
			f := protocol.ContextFailure(t.ctx)
			if r.closed {
				f = protocol.Fail(pb.FailureCode_UNAVAILABLE, "shutdown deadline")
			}
			r.completeLocked(t, protocol.ResultError(t.plan.Operation, pb.MutationOutcome_NOT_STARTED, f))
		} else {
			keep = append(keep, t)
		}
	}
	for i := len(keep); i < len(r.queue); i++ {
		r.queue[i] = nil
	}
	r.queue = keep
}
func (r *Runtime) selectLocked(now time.Time) *batch {
	seen := make(map[string]bool)
	record := make(map[string]bool)
	selected := make(map[*Ticket]bool)
	var items []*Ticket
	token := ""
	bytes := 0
	var seed *Ticket
	for _, t := range r.queue {
		if t.sequence != "" {
			if seen[t.sequence] || r.keys[t.sequence] {
				continue
			}
			seen[t.sequence] = true
		}
		if t.eligible.IsZero() {
			t.eligible = now
		}
		if seed == nil {
			seed = t
			token = t.plan.Token
		}
		if token != t.plan.Token || record[t.plan.Key] || bytes+t.plan.Bytes > r.limits.BatchBytes {
			continue
		}
		items = append(items, t)
		selected[t] = true
		record[t.plan.Key] = true
		bytes += t.plan.Bytes
		if len(items) >= r.limits.BatchOperations || !seed.plan.Batchable {
			break
		}
	}
	if len(items) == 0 {
		return nil
	}
	for _, t := range items {
		if t.ctx.Err() != nil || t.abandoned {
			r.cancelQueuedLocked()
			return nil
		}
	}
	deadline, _ := seed.ctx.Deadline()
	if !r.draining && seed.plan.Batchable && len(items) < r.limits.BatchOperations && now.Sub(seed.eligible) < r.limits.Collect && (deadline.IsZero() || time.Until(deadline) > r.limits.Collect) {
		return nil
	}
	latest := now
	for _, t := range items {
		d, ok := t.ctx.Deadline()
		if !ok {
			d = now.Add(r.limits.BackendTimeout)
		}
		if d.After(latest) {
			latest = d
		}
	}
	backendDeadline := now.Add(r.limits.BackendTimeout)
	owned := backendDeadline.Before(latest)
	if latest.Before(backendDeadline) {
		backendDeadline = latest
	}
	ctx, cancel := context.WithDeadline(context.Background(), backendDeadline)
	b := &batch{ctx: ctx, items: items, cancel: cancel, epoch: r.controller.epoch, saturated: r.active+1 >= r.controller.window && len(r.queue) > len(items), backendDeadline: backendDeadline, timeoutOwned: owned}
	keep := r.queue[:0]
	for _, t := range r.queue {
		if selected[t] {
			r.pendingBytes -= t.plan.Bytes
			t.state = 1
			if t.sequence != "" {
				r.keys[t.sequence] = true
			}
		} else {
			keep = append(keep, t)
		}
	}
	for i := len(keep); i < len(r.queue); i++ {
		r.queue[i] = nil
	}
	r.queue = keep
	r.active++
	r.batches[b] = struct{}{}
	r.notifyLocked()
	// Context is held by the batch runner, never by an arbitrary participant.
	return b
}
func (r *Runtime) execute(b *batch) {
	plans := make([]*execution.Plan, len(b.items))
	for i, t := range b.items {
		plans[i] = t.plan
	}
	results, fb := r.adapter.Execute(b.ctx, plans)
	if b.timeoutOwned && b.ctx.Err() == context.DeadlineExceeded {
		fb = execution.Congested
	}
	b.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active--
	delete(r.batches, b)
	for i, t := range b.items {
		r.completeLocked(t, results[i])
	}
	r.controller.observe(b, fb, r.limits.Concurrency, time.Now())
	r.notifyLocked()
}
func (r *Runtime) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := Snapshot{Pending: len(r.queue), PendingBytes: r.pendingBytes, Active: r.active, Retained: len(r.live), ResultBytes: r.resultBytes, Window: r.controller.window, Draining: r.draining, Closed: r.closed, Overloaded: r.overloaded}
	return s
}
func (r *Runtime) SetOverloaded(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.overloaded != v {
		r.overloaded = v
		r.notifyLocked()
	}
}
func (r *Runtime) BeginDrain() { r.mu.Lock(); defer r.mu.Unlock(); r.draining = true; r.notifyLocked() }

// Close drains admitted work, then cancels execution. Adapter cleanup has its own
// fixed two-second cap and runs exactly once, even with concurrent callers.
func (r *Runtime) Close(ctx context.Context) error {
	r.BeginDrain()
	select {
	case <-r.done:
	case <-ctx.Done():
		r.mu.Lock()
		r.closed = true
		for b := range r.batches {
			b.cancel()
		}
		r.notifyLocked()
		r.mu.Unlock()
	}
	r.closeOnce.Do(func() { r.closeErr = r.adapter.Close() })
	// Driver calls obey their cancelled contexts; no new work can enter after drain.
	select {
	case <-r.done:
	case <-time.After(2 * time.Second):
		return fmt.Errorf("backend workers failed to stop")
	}
	r.mu.Lock()
	r.closed = true
	r.notifyLocked()
	r.mu.Unlock()
	return r.closeErr
}

type controller struct {
	window, credit       int
	epoch                uint64
	cooldown, lastGrowth time.Time
}

func (c *controller) observe(b *batch, fb execution.Feedback, max int, now time.Time) {
	if b.epoch != c.epoch {
		return
	}
	if fb == execution.Congested {
		c.window = maxInt(1, c.window/2)
		c.epoch++
		c.credit = 0
		c.cooldown = now.Add(time.Duration(100+rand.IntN(201)) * time.Millisecond)
		return
	}
	if fb != execution.Healthy || !b.saturated {
		return
	}
	c.credit++
	if c.credit >= maxInt(4, c.window) && now.Sub(c.lastGrowth) >= 250*time.Millisecond && c.window < max {
		c.window++
		c.epoch++
		c.credit = 0
		c.lastGrowth = now
	}
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (s *Session) Outstanding() int {
	r := s.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	return s.outstanding
}
