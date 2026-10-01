// Package store owns one bounded admission ledger and scheduler for each Store.
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
	WorkingBytes, BatchResultBytes                                 int
	Concurrency, BatchOperations, BatchBytes, SessionOutstanding   int
	Collect, BackendTimeout                                        time.Duration
}

func DefaultLimits() Limits {
	limits := Limits{
		PendingOperations: 256, PendingBytes: 32 << 20, ResultOperations: 128, ResultBytes: 32 << 20,
		WorkingBytes: 128 << 20, BatchResultBytes: 8 << 20,
		Concurrency: 4, BatchOperations: 16, BatchBytes: 8 << 20, SessionOutstanding: 8,
		Collect: time.Millisecond, BackendTimeout: 2 * time.Second,
	}
	return limits
}
func (l Limits) Validate() error {
	if l.PendingOperations < 1 || l.PendingOperations > 4096 || l.PendingBytes < protocol.MaxFrame ||
		l.ResultOperations < 1 || l.ResultOperations > 4096 || l.ResultBytes < protocol.MaxDocument+protocol.ResultOverhead ||
		l.WorkingBytes < 24<<20 || l.BatchResultBytes < protocol.MaxDocument+protocol.ResultOverhead ||
		l.Concurrency < 1 || l.Concurrency > 32 || l.BatchOperations < 1 || l.BatchOperations > 128 ||
		l.BatchBytes < protocol.MaxDocument+4096 || l.BatchBytes > 32<<20 ||
		l.SessionOutstanding < 1 || l.SessionOutstanding > 32 || l.Collect < 0 || l.Collect > 10*time.Millisecond ||
		l.BackendTimeout <= 0 || l.BackendTimeout > 10*time.Second {
		return fmt.Errorf("invalid runtime bounds")
	}
	return nil
}

type Runtime struct {
	mu                                      sync.Mutex
	adapter                                 execution.Adapter
	limits                                  Limits
	queue                                   []*Ticket
	live                                    map[*Ticket]struct{}
	batches                                 map[*batch]struct{}
	keys                                    map[string]*Ticket
	pendingBytes, resultBytes, workingBytes int
	active, publishers                      int
	nextSession                             uint64
	draining, closed, overloaded            bool
	wake, changed, done                     chan struct{}
	closeOnce                               sync.Once
	closeErr                                error
	controller                              controller
	metrics                                 runtimeMetrics
}
type Session struct {
	runtime     *Runtime
	id          uint64
	Events      chan *Emission
	outstanding int
	closed      bool
}
type Emission struct {
	Ticket *Ticket
	Event  *pb.Event
	End    bool
	done   chan struct{}
	once   sync.Once
}

func (e *Emission) Release() {
	if e.done != nil {
		e.once.Do(func() { close(e.done) })
	}
}

type Ticket struct {
	id                   uint64
	publishing, released bool
	runtime              *Runtime
	session              *Session
	ctx                  context.Context
	cancel               context.CancelFunc
	plan                 *execution.Plan
	result               *pb.Result
	ready                chan struct{}
	sequence             string
	eligible, queuedAt   time.Time
	state                uint8
	abandoned, acked     bool
	stopWatch            func() bool
}
type batch struct {
	ctx             context.Context
	items           []*Ticket
	cancel          context.CancelFunc
	epoch           uint64
	saturated       bool
	backendDeadline time.Time
	timeoutOwned    bool
	workingBytes    int
}
type Snapshot struct {
	Ready, ReadyBytes                                                                      int
	Cooldown                                                                               bool
	Feedback                                                                               string
	Pending, PendingBytes, Active, Retained, ResultBytes, WorkingBytes, Publishers, Window int
	Draining, Closed, Overloaded                                                           bool
}

func New(adapter execution.Adapter, limits Limits) (*Runtime, error) {
	if adapter == nil {
		return nil, fmt.Errorf("missing adapter")
	}
	if err := limits.Validate(); err != nil {
		_ = adapter.Close()
		return nil, err
	}
	runtime := newRuntime(adapter, limits)
	go runtime.loop()
	return runtime, nil
}
func newRuntime(adapter execution.Adapter, limits Limits) *Runtime {
	runtime := &Runtime{adapter: adapter, limits: limits, live: make(map[*Ticket]struct{}), batches: make(map[*batch]struct{}), keys: make(map[string]*Ticket), wake: make(chan struct{}, 1), changed: make(chan struct{}), done: make(chan struct{})}
	runtime.controller.window = limits.Concurrency
	runtime.metrics = newRuntimeMetrics()
	return runtime
}
func (r *Runtime) NewSession() *Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextSession++
	session := &Session{runtime: r, id: r.nextSession, Events: make(chan *Emission, 1)}
	return session
}
func (r *Runtime) PrepareCall(id uint64, call *pb.Call) (*execution.Plan, *pb.Failure) {
	plan, failure := r.adapter.PrepareCall(id, call)
	if failure != nil {
		r.metrics.rejections.WithLabelValues("prepare").Inc()
	}
	return plan, failure
}
func (r *Runtime) Submit(ctx context.Context, plan *execution.Plan, session *Session) (*Ticket, *pb.Failure, <-chan struct{}) {
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
	if session != nil && (session.runtime != r || session.closed) {
		return nil, protocol.Fail(pb.FailureCode_UNAVAILABLE, "session closed"), changed
	}
	if plan != nil && (plan.CleanupRequired || plan.Streaming) && session == nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "streaming plans require an event consumer"), changed
	}
	if plan == nil || plan.Bytes <= 0 || plan.ResultBytes < 0 || plan.WorkingBytes < 0 || plan.Bytes > r.limits.PendingBytes || plan.ResultBytes > r.limits.ResultBytes || plan.WorkingBytes > r.limits.WorkingBytes || (!plan.Singleton && (plan.Bytes > r.limits.BatchBytes || plan.ResultBytes > r.limits.BatchResultBytes)) {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "operation cannot fit bounded execution"), changed
	}
	if len(r.queue) >= r.limits.PendingOperations || r.pendingBytes+plan.Bytes > r.limits.PendingBytes || len(r.live) >= r.limits.ResultOperations || r.resultBytes+plan.ResultBytes > r.limits.ResultBytes || session != nil && session.outstanding >= r.limits.SessionOutstanding {
		if session == nil {
			r.metrics.rejections.WithLabelValues("capacity").Inc()
		}
		return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "admission capacity exhausted"), changed
	}
	ticketContext, cancel := context.WithCancel(ctx)
	ticket := &Ticket{runtime: r, session: session, ctx: ticketContext, cancel: cancel, plan: plan, id: plan.ID, ready: make(chan struct{}), queuedAt: time.Now()}
	if session != nil {
		session.outstanding++
	}
	// Ordering is defined within one RPC. Different RPCs retain the database's
	// own concurrency semantics and never imply external-write isolation.
	if session != nil && plan.Key != "" {
		ticket.sequence = fmt.Sprintf("%d:%s", session.id, plan.Key)
	}
	r.queue = append(r.queue, ticket)
	r.live[ticket] = struct{}{}
	r.pendingBytes += plan.Bytes
	r.resultBytes += plan.ResultBytes
	ticket.stopWatch = context.AfterFunc(ticketContext, r.signal)
	r.signal()
	return ticket, nil, changed
}
func (r *Runtime) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}
func (r *Runtime) notifyLocked() { close(r.changed); r.changed = make(chan struct{}); r.signal() }
func (t *Ticket) Wait(ctx context.Context) (*pb.Result, error) {
	select {
	case <-t.ready:
		r := t.runtime
		r.mu.Lock()
		result := t.result
		r.mu.Unlock()
		return result, nil
	case <-ctx.Done():
		t.Abandon()
		return nil, ctx.Err()
	}
}
func (t *Ticket) Result() *pb.Result {
	<-t.ready
	r := t.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	return t.result
}
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
	t.cancel()
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
			if t.session != nil {
				t.abandoned = true
			}
			t.cancel()
			if t.state == 2 && t.session != nil {
				r.releaseLocked(t)
			}
		}
	}
	for {
		select {
		case <-s.Events:
		default:
			r.signal()
			return
		}
	}
}
func (s *Session) Outstanding() int {
	r := s.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	return s.outstanding
}
func (r *Runtime) releaseLocked(t *Ticket) {
	if t.released {
		return
	}
	t.acked = true
	t.cancel()
	if t.publishing {
		return
	}
	t.released = true
	delete(r.live, t)
	r.pendingBytes -= t.plan.Bytes
	r.resultBytes -= t.plan.ResultBytes
	if t.session != nil {
		t.session.outstanding--
	}
	t.plan = nil
	t.result = nil
	r.notifyLocked()
}
func (r *Runtime) completeLocked(t *Ticket, result *pb.Result) {
	r.terminalLocked(t, result)
	t.state = 2
	t.result = result
	if t.stopWatch != nil {
		t.stopWatch()
	}
	close(t.ready)
	if t.sequence != "" && r.keys[t.sequence] == t {
		delete(r.keys, t.sequence)
	}
	if t.abandoned || t.session != nil && t.session.closed {
		r.releaseLocked(t)
	}
	r.notifyLocked()
}
func (r *Runtime) cancelQueuedLocked() {
	keep := r.queue[:0]
	for _, t := range r.queue {
		if t.ctx.Err() != nil || t.abandoned || r.closed {
			failure := protocol.ContextFailure(t.ctx)
			if r.closed {
				failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "shutdown deadline")
			}
			var result *pb.Result
			if t.plan.Operation != nil {
				result = protocol.ResultError(t.plan.Operation, pb.MutationOutcome_NOT_STARTED, failure)
			}
			if t.plan.CleanupRequired {
				t.state = 3
			} else {
				r.completeLocked(t, result)
			}
			if t.session != nil && !t.acked {
				var events []*pb.Event
				if result != nil {
					events = []*pb.Event{resultEvent(result)}
				}
				r.startPublisherLocked(t, events, false)
			}
		} else {
			keep = append(keep, t)
		}
	}
	for i := len(keep); i < len(r.queue); i++ {
		r.queue[i] = nil
	}
	r.queue = keep
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
				interested = interested || t.ctx.Err() == nil && !t.abandoned
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
		finish := r.draining && len(r.queue) == 0 && r.active == 0 && r.publishers == 0
		r.mu.Unlock()
		if finish {
			return
		}
	}
}
func (r *Runtime) selectLocked(now time.Time) *batch {
	seen := make(map[string]bool)
	records := make(map[string]bool)
	selected := make(map[*Ticket]bool)
	var items []*Ticket
	bytes, resultBytes := 0, 0
	var seed *Ticket
	for _, t := range r.queue {
		if t.state != 0 {
			continue
		}
		if t.sequence != "" {
			if seen[t.sequence] || (r.keys[t.sequence] != nil && r.keys[t.sequence] != t) {
				continue
			}
			seen[t.sequence] = true
		}
		if t.eligible.IsZero() {
			t.eligible = now
		}
		if seed == nil {
			seed = t
		}
		if len(items) > 0 && (seed.plan.Singleton || t.plan.Singleton || seed.plan.BatchKey != t.plan.BatchKey || records[t.plan.Key] || bytes+t.plan.Bytes > r.limits.BatchBytes || resultBytes+t.plan.ResultBytes > r.limits.BatchResultBytes) {
			continue
		}
		items = append(items, t)
		selected[t] = true
		records[t.plan.Key] = true
		bytes += t.plan.Bytes
		resultBytes += t.plan.ResultBytes
		if len(items) >= r.limits.BatchOperations || seed.plan.Singleton {
			break
		}
	}
	if len(items) == 0 {
		return nil
	}
	workingBytes := 0
	for _, item := range items {
		workingBytes = max(workingBytes, item.plan.WorkingBytes)
	}
	if r.workingBytes+workingBytes > r.limits.WorkingBytes {
		return nil
	}
	for _, t := range items {
		if t.ctx.Err() != nil || t.abandoned {
			r.cancelQueuedLocked()
			return nil
		}
	}
	collect := !r.draining && !seed.plan.Singleton && len(items) < r.limits.BatchOperations && now.Sub(seed.eligible) < r.limits.Collect
	for _, t := range items {
		if deadline, ok := t.ctx.Deadline(); ok && deadline.Sub(now) <= r.limits.Collect {
			collect = false
			break
		}
	}
	if collect {
		return nil
	}
	latest := now
	for _, t := range items {
		deadline, ok := t.ctx.Deadline()
		if !ok {
			deadline = now.Add(r.limits.BackendTimeout)
		}
		if deadline.After(latest) {
			latest = deadline
		}
	}
	backendDeadline := now.Add(r.limits.BackendTimeout)
	owned := backendDeadline.Before(latest)
	if latest.Before(backendDeadline) {
		backendDeadline = latest
	}
	ctx, cancel := context.WithDeadline(context.Background(), backendDeadline)
	// Only Native emits directly during execution. Its backend calls enforce
	// their own I/O cap, while output stalls follow the caller's lifetime. Scan
	// pages and Lua results publish after execution releases this bounded batch.
	if seed.plan.Streaming {
		cancel()
		ctx, cancel = context.WithCancel(seed.ctx)
		owned = false
		if deadline, ok := ctx.Deadline(); ok {
			backendDeadline = deadline
		}
	}
	b := &batch{ctx: ctx, items: items, cancel: cancel, epoch: r.controller.epoch, saturated: r.active+1 >= r.controller.window && len(r.queue) > len(items), backendDeadline: backendDeadline, timeoutOwned: owned, workingBytes: workingBytes}
	keep := r.queue[:0]
	for _, t := range r.queue {
		if selected[t] {
			r.metrics.queue.WithLabelValues("route").Observe(now.Sub(t.queuedAt).Seconds())
			t.state = 1
			if t.sequence != "" {
				r.keys[t.sequence] = t
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
	r.workingBytes += workingBytes
	r.batches[b] = struct{}{}
	r.notifyLocked()
	return b
}
func resultEvent(result *pb.Result) *pb.Event {
	if result == nil {
		return nil
	}
	value := &pb.Event_Result{Result: result}
	event := &pb.Event{Version: 1, Value: value}
	return event
}
func (t *Ticket) emit(event *pb.Event) error {
	if t.session == nil {
		return nil
	}
	emission := &Emission{Ticket: t, Event: event, done: make(chan struct{})}
	select {
	case t.session.Events <- emission:
	case <-t.ctx.Done():
		return t.ctx.Err()
	}
	select {
	case <-emission.done:
		return nil
	case <-t.ctx.Done():
		return t.ctx.Err()
	}
}
func (r *Runtime) startPublisherLocked(t *Ticket, events []*pb.Event, continuation bool) {
	r.publishers++
	t.publishing = true
	go func() {
		if !continuation && t.plan.CleanupRequired {
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			failure := r.adapter.ClosePlan(cleanup, t.plan)
			cancel()
			if failure != nil {
				for _, event := range events {
					if end := event.GetScanEnd(); end != nil && end.Failure == nil {
						end.Failure = failure
					}
				}
			}
			r.mu.Lock()
			r.completeLocked(t, t.result)
			r.mu.Unlock()
		}
		delivered := true
		for _, event := range events {
			if err := t.emit(event); err != nil {
				delivered = false
				break
			}
		}
		if continuation && delivered && t.ctx.Err() == nil {
			r.mu.Lock()
			if !t.abandoned && !r.closed && !t.session.closed {
				t.publishing = false
				r.publishers--
				t.state = 0
				t.eligible = time.Time{}
				t.queuedAt = time.Now()
				r.queue = append(r.queue, t)
				r.notifyLocked()
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
		}
		if continuation {
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = r.adapter.ClosePlan(cleanup, t.plan)
			cancel()
			r.mu.Lock()
			r.completeLocked(t, t.result)
			r.mu.Unlock()
		}
		if delivered && t.session != nil {
			emission := &Emission{Ticket: t, End: true, done: make(chan struct{})}
			select {
			case t.session.Events <- emission:
			case <-t.ctx.Done():
				delivered = false
			}
			if delivered {
				select {
				case <-emission.done:
				case <-t.ctx.Done():
				}
			}
		}
		r.mu.Lock()
		r.publishers--
		t.publishing = false
		if t.acked || t.abandoned || t.session != nil && t.session.closed {
			r.releaseLocked(t)
		}
		r.notifyLocked()
		r.mu.Unlock()
	}()
}
func (r *Runtime) execute(b *batch) {
	plans := make([]*execution.Plan, len(b.items))
	positions := make(map[*execution.Plan]*Ticket, len(b.items))
	events := make(map[*Ticket][]*pb.Event, len(b.items))
	for i, t := range b.items {
		plan := *t.plan
		plan.Context = t.ctx
		plan.BackendTimeout = r.limits.BackendTimeout
		plans[i] = &plan
		positions[&plan] = t
	}
	emit := func(plan *execution.Plan, event *pb.Event) error {
		ticket := positions[plan]
		if ticket == nil {
			return fmt.Errorf("adapter emitted unknown plan")
		}
		if event == nil {
			return fmt.Errorf("adapter emitted nil event")
		}
		if event.GetResult() != nil {
			ticket.result = event.GetResult()
		}
		if plan.Streaming {
			return ticket.emit(event)
		}
		if !plan.Singleton && len(events[ticket]) > 0 {
			return fmt.Errorf("record emitted multiple events")
		}
		if len(events[ticket]) >= 2 {
			return fmt.Errorf("adapter step exceeds two bounded events")
		}
		events[ticket] = append(events[ticket], event)
		return nil
	}
	r.metrics.executions.WithLabelValues("route").Inc()
	r.metrics.batch.Observe(float64(len(plans)))
	started := time.Now()
	feedback := r.adapter.Execute(b.ctx, plans, emit)
	r.metrics.duration.WithLabelValues("route").Observe(time.Since(started).Seconds())
	if b.timeoutOwned && b.ctx.Err() == context.DeadlineExceeded {
		feedback = execution.Congested
	}
	b.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active--
	r.workingBytes -= b.workingBytes
	delete(r.batches, b)
	for index, ticket := range b.items {
		ticket.plan.Continue = plans[index].Continue
		result := ticket.result
		if result == nil && ticket.plan.Operation != nil {
			failure := protocol.Fail(pb.FailureCode_INTERNAL, "adapter returned no result")
			result = protocol.ResultError(ticket.plan.Operation, pb.MutationOutcome_UNKNOWN, failure)
			events[ticket] = []*pb.Event{resultEvent(result)}
		}
		if ticket.plan.Continue || ticket.plan.CleanupRequired {
			ticket.state = 3
		} else {
			r.completeLocked(ticket, result)
		}
		if ticket.session != nil && !ticket.acked {
			r.startPublisherLocked(ticket, events[ticket], ticket.plan.Continue)
		}
	}
	r.observeLocked(b, feedback)
	r.notifyLocked()
}
func (r *Runtime) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot := Snapshot{Pending: len(r.queue), PendingBytes: r.pendingBytes, Active: r.active, Retained: len(r.live), ResultBytes: r.resultBytes, WorkingBytes: r.workingBytes, Publishers: r.publishers, Window: r.controller.window, Draining: r.draining, Closed: r.closed, Overloaded: r.overloaded, Cooldown: time.Now().Before(r.controller.cooldown), Feedback: "unobserved"}
	if r.metrics.observed {
		snapshot.Feedback = "neutral"
		if r.metrics.feedback == execution.Healthy {
			snapshot.Feedback = "healthy"
		}
		if r.metrics.feedback == execution.Congested {
			snapshot.Feedback = "congested"
		}
	}
	for t := range r.live {
		if t.state == 2 {
			snapshot.Ready++
			snapshot.ReadyBytes += t.plan.ResultBytes
		}
	}
	return snapshot
}
func (r *Runtime) SetOverloaded(value bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.overloaded != value {
		r.overloaded = value
		r.notifyLocked()
	}
}
func (r *Runtime) BeginDrain() { r.mu.Lock(); defer r.mu.Unlock(); r.draining = true; r.notifyLocked() }
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
		for t := range r.live {
			if t.session != nil {
				t.abandoned = true
			}
			t.cancel()
			if t.state == 2 && t.session != nil {
				r.releaseLocked(t)
			}
		}
		r.notifyLocked()
		r.mu.Unlock()
	}
	select {
	case <-r.done:
	case <-time.After(3 * time.Second):
		r.closeOnce.Do(func() { r.closeErr = r.adapter.Close() })
		return fmt.Errorf("backend executions failed to stop")
	}
	r.mu.Lock()
	r.closed = true
	for t := range r.live {
		if t.session != nil {
			t.abandoned = true
		}
		t.cancel()
		if t.state == 2 && t.session != nil {
			r.releaseLocked(t)
		}
	}
	r.notifyLocked()
	r.mu.Unlock()
	r.closeOnce.Do(func() { r.closeErr = r.adapter.Close() })
	for {
		r.mu.Lock()
		publishers := r.publishers
		changed := r.changed
		r.mu.Unlock()
		if publishers == 0 {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return r.closeErr
}

type controller struct {
	window, credit       int
	epoch                uint64
	cooldown, lastGrowth time.Time
}

func (c *controller) observe(b *batch, feedback execution.Feedback, maximum int, now time.Time) {
	if b.epoch != c.epoch {
		return
	}
	if feedback == execution.Congested {
		c.window = maxInt(1, c.window/2)
		c.epoch++
		c.credit = 0
		c.cooldown = now.Add(time.Duration(100+rand.IntN(201)) * time.Millisecond)
		return
	}
	if feedback != execution.Healthy || !b.saturated {
		return
	}
	c.credit++
	if c.credit >= maxInt(4, c.window) && now.Sub(c.lastGrowth) >= 250*time.Millisecond && c.window < maximum {
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

func (t *Ticket) ID() uint64 {
	return t.id
}
