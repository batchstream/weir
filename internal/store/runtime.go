// Package store owns one bounded admission ledger and scheduler for each Store.
package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"google.golang.org/protobuf/proto"
)

type Limits struct {
	PendingBytes, ResultBytes                int
	WorkingBytes                             int
	Concurrency, BatchOperations, BatchBytes int
	BackendTimeout                           time.Duration
}

func DefaultLimits() Limits {
	limits := Limits{
		PendingBytes: 32 << 20, ResultBytes: 32 << 20,
		WorkingBytes: 384 << 20,
		Concurrency:  2, BatchOperations: 32, BatchBytes: 8 << 20,
		BackendTimeout: 2 * time.Second,
	}
	return limits
}
func (l Limits) Validate() error {
	if l.PendingBytes < protocol.MaxExecuteRequestBytes ||
		l.ResultBytes < protocol.MaxDocument+execution.ResultOverheadBytes ||
		l.WorkingBytes < 24<<20 ||
		l.Concurrency < 1 || uint64(l.Concurrency) > (64<<30)/(2<<20) || l.BatchOperations < 1 ||
		l.BatchBytes < protocol.MaxDocument+4096 || l.BatchBytes > 32<<20 ||
		l.BackendTimeout <= 0 {
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
	keys                                    map[resourceKey]*Ticket
	pendingBytes, resultBytes, workingBytes int
	publishers                              int
	draining, closed, overloaded            bool
	wake, changed, done                     chan struct{}
	closeOnce                               sync.Once
	closeErr                                error
	metrics                                 runtimeMetrics
}
type Session struct {
	runtime *Runtime
	Events  chan *Emission
	closed  bool
}
type Emission struct {
	Event *pb.Event
	done  chan struct{}
	once  sync.Once
}

func (e *Emission) Release() {
	if e.done != nil {
		e.once.Do(func() { close(e.done) })
	}
}

type Ticket struct {
	publishing       bool
	runtime          *Runtime
	session          *Session
	ctx              context.Context
	cancel           context.CancelFunc
	plan             *execution.Plan
	event            *pb.Event
	ready            chan struct{}
	queuedAt         time.Time
	state            uint8
	abandoned, acked bool
	stopWatch        func() bool
}
type resourceKey struct {
	session  *Session
	resource string
}

type batch struct {
	ctx          context.Context
	items        []*Ticket
	cancel       context.CancelFunc
	timeoutOwned bool
	workingBytes int
}
type Snapshot struct {
	Ready, ReadyBytes                     int
	Pending, PendingBytes                 int
	Active, Retained                      int
	ResultBytes, WorkingBytes, Publishers int
	ConcurrencyLimit                      int
	Draining, Closed, Overloaded          bool
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
	runtime := &Runtime{adapter: adapter, limits: limits, live: make(map[*Ticket]struct{}), batches: make(map[*batch]struct{}), keys: make(map[resourceKey]*Ticket), wake: make(chan struct{}, 1), changed: make(chan struct{}), done: make(chan struct{})}
	runtime.metrics = newRuntimeMetrics()
	return runtime
}
func (r *Runtime) NewSession() *Session {
	session := &Session{runtime: r}
	return session
}

func (r *Runtime) PrepareRecord(record *execution.Record) (*execution.Plan, *pb.Failure) {
	plan, failure := r.adapter.PrepareRecord(record)
	if failure != nil {
		r.metrics.rejections.WithLabelValues("prepare").Inc()
		return nil, failure
	}
	if plan == nil || plan.Command != record.Command() || plan.ID != record.Index() || plan.Command.GetNative() != nil || plan.Command.GetScan() != nil {
		return nil, protocol.Fail(pb.FailureCode_INTERNAL, "invalid prepared record")
	}
	plan.Bytes = max(plan.Bytes, record.Bytes())
	plan.ResultBytes = max(plan.ResultBytes, execution.ResultOverheadBytes)
	return plan, nil
}

func (r *Runtime) PrepareCommand(id uint64, call *pb.Command) (*execution.Plan, *pb.Failure) {
	plan, failure := r.adapter.PrepareCommand(id, call)
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
	singleton := plan != nil && (plan.Command.GetScan() != nil || plan.Command.GetNative() != nil)
	if singleton && session == nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "streaming plans require an event consumer"), changed
	}
	if plan == nil || plan.Bytes < execution.EntryOverheadBytes || plan.ResultBytes < execution.ResultOverheadBytes || plan.WorkingBytes < 0 || plan.Bytes > r.limits.PendingBytes || plan.ResultBytes > r.limits.ResultBytes || plan.WorkingBytes > r.limits.WorkingBytes || (!singleton && plan.Bytes > r.limits.BatchBytes) {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "operation cannot fit bounded execution"), changed
	}
	if plan.Bytes > r.limits.PendingBytes-r.pendingBytes || plan.ResultBytes > r.limits.ResultBytes-r.resultBytes {
		if session == nil {
			r.metrics.rejections.WithLabelValues("capacity").Inc()
		}
		return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "admission capacity exhausted"), changed
	}
	ticketContext, cancel := context.WithCancel(ctx)
	ticket := &Ticket{runtime: r, session: session, ctx: ticketContext, cancel: cancel, plan: plan, ready: make(chan struct{}), queuedAt: time.Now()}
	if session != nil && (plan.Command.GetScan() != nil || plan.Command.GetNative() != nil) && session.Events == nil {
		session.Events = make(chan *Emission, 1)
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
func (t *Ticket) Wait(ctx context.Context) (*pb.Event, error) {
	select {
	case <-t.ready:
		r := t.runtime
		r.mu.Lock()
		event := t.event
		r.mu.Unlock()
		return event, nil
	case <-ctx.Done():
		t.Abandon()
		return nil, ctx.Err()
	}
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
			t.abandoned = true
			t.cancel()
			if t.state == 2 {
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
func (r *Runtime) releaseLocked(t *Ticket) {
	if t.plan == nil {
		return
	}
	t.acked = true
	t.cancel()
	if t.publishing {
		return
	}
	delete(r.live, t)
	r.pendingBytes -= t.plan.Bytes
	r.resultBytes -= t.plan.ResultBytes
	t.plan = nil
	t.event = nil
	r.notifyLocked()
}
func (r *Runtime) completeLocked(t *Ticket, event *pb.Event) {
	r.terminalLocked(t, event)
	t.state = 2
	t.event = event
	if t.stopWatch != nil {
		t.stopWatch()
	}
	close(t.ready)
	key := resourceKey{session: t.session, resource: t.plan.Key}
	if t.session != nil && t.plan.Command.GetMutate() != nil && r.keys[key] == t {
		delete(r.keys, key)
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
			var event *pb.Event
			if t.plan.Command.GetRead() != nil || t.plan.Command.GetMutate() != nil {
				event = execution.FailedEvent(t.plan.Command, pb.MutationOutcome_NOT_STARTED, failure)
			}
			if t.plan.Command.GetScan() != nil {
				t.state = 3
			} else {
				r.completeLocked(t, event)
			}
			if t.session != nil && t.session.Events != nil && !t.acked {
				var events []*pb.Event

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
	defer close(r.done)
	for {
		<-r.wake
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
		for !r.closed {
			b := r.selectLocked(time.Now())
			if b == nil {
				break
			}
			go r.execute(b)
		}
		finish := r.draining && len(r.queue) == 0 && len(r.batches) == 0 && r.publishers == 0
		r.mu.Unlock()
		if finish {
			return
		}
	}
}
func (r *Runtime) selectLocked(now time.Time) *batch {
	if len(r.batches) >= r.limits.Concurrency {
		return nil
	}
	seen := make(map[resourceKey]bool)
	records := make(map[string]bool)
	selected := make(map[*Ticket]bool)
	var items []*Ticket
	bytes := 0
	var seed *Ticket
	for _, t := range r.queue {
		if t.state != 0 {
			continue
		}
		key := resourceKey{session: t.session, resource: t.plan.Key}
		if t.session != nil && t.plan.Command.GetMutate() != nil && t.plan.Key != "" {
			if seen[key] || (r.keys[key] != nil && r.keys[key] != t) {
				continue
			}
			seen[key] = true
		}
		if t.plan.WorkingBytes > r.limits.WorkingBytes-r.workingBytes {
			continue
		}
		if seed == nil {
			seed = t
		}
		seedSingleton := seed.plan.Command.GetScan() != nil || seed.plan.Command.GetNative() != nil
		singleton := t.plan.Command.GetScan() != nil || t.plan.Command.GetNative() != nil
		previousWrite, duplicateResource := records[t.plan.Key]
		if len(items) > 0 && (seedSingleton || singleton || seed.plan.BatchKey != t.plan.BatchKey || (duplicateResource && (previousWrite || t.plan.Command.GetMutate() != nil)) || bytes+t.plan.Bytes > r.limits.BatchBytes) {
			continue
		}
		items = append(items, t)
		selected[t] = true
		records[t.plan.Key] = t.plan.Command.GetMutate() != nil
		bytes += t.plan.Bytes
		if len(items) >= r.limits.BatchOperations || seedSingleton {
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
	owned := !backendDeadline.After(latest)
	if latest.Before(backendDeadline) {
		backendDeadline = latest
	}
	ctx, cancel := context.WithDeadline(context.Background(), backendDeadline)
	// Only Native emits directly during execution. Its backend calls enforce
	// their own I/O cap, while output stalls follow the caller's lifetime. Scan
	// pages and Lua results publish after execution releases this bounded batch.
	if seed.plan.Command.GetNative() != nil {
		cancel()
		ctx, cancel = context.WithCancel(seed.ctx)
		owned = false
	}
	b := &batch{ctx: ctx, items: items, cancel: cancel, timeoutOwned: owned, workingBytes: workingBytes}
	keep := r.queue[:0]
	for _, t := range r.queue {
		if selected[t] {
			r.metrics.queue.WithLabelValues("execution").Observe(now.Sub(t.queuedAt).Seconds())
			t.state = 1
			if t.session != nil && t.plan.Command.GetMutate() != nil && t.plan.Key != "" {
				key := resourceKey{session: t.session, resource: t.plan.Key}
				r.keys[key] = t
			}
		} else {
			keep = append(keep, t)
		}
	}
	for i := len(keep); i < len(r.queue); i++ {
		r.queue[i] = nil
	}
	r.queue = keep
	r.workingBytes += workingBytes
	r.batches[b] = struct{}{}
	r.notifyLocked()
	return b
}

func (t *Ticket) emit(event *pb.Event) error {
	if t.session == nil {
		return nil
	}
	emission := &Emission{Event: event, done: make(chan struct{})}
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
		if !continuation && t.plan.Command.GetScan() != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			failure := r.adapter.ClosePlan(cleanup, t.plan)
			cancel()
			if failure != nil {
				for _, event := range events {
					if end := event.GetScanEnd(); end != nil && end.Failure == nil {
						end.Failure = failure
						end.Exhausted = false
						end.NextContinuationToken = nil
					}
				}
			}
			r.mu.Lock()
			r.completeLocked(t, t.event)
			r.mu.Unlock()
		}
		delivered := true
		for index, event := range events {
			if err := t.emit(event); err != nil {
				delivered = false
				break
			}
			events[index] = nil
		}
		if continuation && delivered && t.ctx.Err() == nil {
			r.mu.Lock()
			if !t.abandoned && !r.closed && !t.session.closed {
				t.publishing = false
				r.publishers--
				t.state = 0
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
			r.completeLocked(t, t.event)
			r.mu.Unlock()
		}
		if delivered && t.session != nil {
			emission := &Emission{done: make(chan struct{})}
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
	// Scan batches are singleton; Record and Native results bypass this buffer.
	var events []*pb.Event
	var eventBytes, documentBytes int
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
		if plan.Command.GetRead() != nil || plan.Command.GetMutate() != nil {
			if ticket.event != nil {
				return fmt.Errorf("record emitted multiple results")
			}
			if plan.Command.GetRead() != nil && event.GetReadResult() == nil || plan.Command.GetMutate() != nil && event.GetMutationResult() == nil {
				return fmt.Errorf("adapter emitted wrong record result")
			}
			if proto.Size(event) > plan.ResultBytes {
				return fmt.Errorf("record result exceeds reservation")
			}
			ticket.event = event
			return nil
		}
		if plan.Command.GetNative() != nil {
			return ticket.emit(event)
		}
		if plan.Command.GetScan() != nil {
			if event.GetDocument() == nil && event.GetScanEnd() == nil {
				return fmt.Errorf("adapter emitted wrong Scan result")
			}
			if len(events) != 0 && events[len(events)-1].GetScanEnd() != nil {
				return fmt.Errorf("adapter emitted after Scan terminal result")
			}
			if len(events) >= execution.ScanBatchDocuments+1 {
				return fmt.Errorf("Scan batch exceeds document bound")
			}
			if document := event.GetDocument(); document != nil {
				if len(events) >= execution.ScanBatchDocuments || len(document.Data) > execution.ScanBatchBytes-documentBytes {
					return fmt.Errorf("Scan batch exceeds retained output bound")
				}
				documentBytes += len(document.Data)
			}
			size := proto.Size(event)
			if size > plan.ResultBytes-eventBytes {
				return fmt.Errorf("Scan output exceeds result reservation")
			}
			eventBytes += size
		} else if len(events) >= 2 {
			return fmt.Errorf("adapter step exceeds two bounded events")
		}
		events = append(events, event)
		return nil
	}
	r.metrics.executions.WithLabelValues("execution").Inc()
	r.metrics.batch.Observe(float64(len(plans)))
	started := time.Now()
	continuation := r.adapter.Execute(b.ctx, plans, emit)
	// Only a singleton Scan can schedule another bounded fetch.
	continuation = continuation && len(plans) == 1 && plans[0].Command.GetScan() != nil
	if continuation && len(events) != 0 && events[len(events)-1].GetScanEnd() != nil {
		continuation = false
	}
	r.metrics.duration.WithLabelValues("execution").Observe(time.Since(started).Seconds())
	interested := false
	for _, ticket := range b.items {
		if ticket.ctx.Err() == nil {
			interested = true
		}
	}
	if interested && b.timeoutOwned && b.ctx.Err() == context.DeadlineExceeded {
		r.metrics.backendTimeouts.Inc()
	}
	b.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workingBytes -= b.workingBytes
	delete(r.batches, b)
	for _, ticket := range b.items {
		event := ticket.event
		if event == nil && (ticket.plan.Command.GetRead() != nil || ticket.plan.Command.GetMutate() != nil) {
			failure := protocol.Fail(pb.FailureCode_INTERNAL, "adapter returned no result")
			event = execution.FailedEvent(ticket.plan.Command, pb.MutationOutcome_UNKNOWN, failure)
		}
		if ticket.plan.Command.GetScan() != nil {
			ticket.state = 3
		} else {
			r.completeLocked(ticket, event)
		}
		if ticket.session != nil && ticket.session.Events != nil && !ticket.acked {
			r.startPublisherLocked(ticket, events, continuation && ticket.plan.Command.GetScan() != nil)
		}
	}
	r.notifyLocked()
}
func (r *Runtime) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot := Snapshot{Pending: len(r.queue), PendingBytes: r.pendingBytes, Active: len(r.batches), Retained: len(r.live), ResultBytes: r.resultBytes, WorkingBytes: r.workingBytes, Publishers: r.publishers, ConcurrencyLimit: r.limits.Concurrency, Draining: r.draining, Closed: r.closed, Overloaded: r.overloaded}
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

// Record results remain consumer-owned until Ack, including sessions used for
// resource ordering. Scan and Native use session-owned event publishers.
func (r *Runtime) cancelTicketsLocked() {
	for ticket := range r.live {
		record := ticket.plan.Command.GetRead() != nil || ticket.plan.Command.GetMutate() != nil
		discard := ticket.session != nil && !record
		if discard {
			ticket.abandoned = true
		}
		ticket.cancel()
		if ticket.state == 2 && discard {
			r.releaseLocked(ticket)
		}
	}
}

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
		r.cancelTicketsLocked()
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
	r.cancelTicketsLocked()
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
