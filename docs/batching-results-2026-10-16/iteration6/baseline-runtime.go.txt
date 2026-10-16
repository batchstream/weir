// Package store batches waiting operations and owns their execution and publication.
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

// Limits bounds waiting work, physical batch size and per-Store adapter invocations.
type Limits struct {
	CleanupTimeout, BatchWait   time.Duration
	QueueOperations, QueueBytes int
	BatchOperations, BatchBytes int
	MaxInFlight                 int // Zero allows unlimited adapter invocations.
}

func DefaultLimits() Limits {
	limits := Limits{QueueOperations: 1024, QueueBytes: 32 << 20, BatchOperations: 128, BatchBytes: 4 << 20, BatchWait: 20 * time.Millisecond, MaxInFlight: 16, CleanupTimeout: 2 * time.Second}
	return limits
}

func (limits Limits) Validate() error {
	if limits.QueueOperations < 1 || limits.QueueBytes < 1 ||
		limits.BatchOperations < 1 || limits.BatchBytes < 1 || limits.MaxInFlight < 0 || limits.CleanupTimeout < 0 || limits.BatchWait < 0 {
		return fmt.Errorf("invalid batch, execution or queue bounds")
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
	workingBytes int
}
type Snapshot struct {
	Ready, ReadyBytes                     int
	Pending, PendingBytes                 int
	Active, Retained                      int
	ResultBytes, WorkingBytes, Publishers int
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
	if limits.CleanupTimeout == 0 {
		limits.CleanupTimeout = DefaultLimits().CleanupTimeout
	}
	runtime := &Runtime{adapter: adapter, limits: limits, live: make(map[*Ticket]struct{}), batches: make(map[*batch]struct{}), keys: make(map[resourceKey]*Ticket), wake: make(chan struct{}, 1), changed: make(chan struct{}), done: make(chan struct{})}
	runtime.metrics = newRuntimeMetrics()
	return runtime
}
func (r *Runtime) NewSession() *Session {
	session := &Session{runtime: r}
	return session
}

func (r *Runtime) PrepareRecord(ctx context.Context, record *execution.Record) (*execution.Plan, *pb.Failure) {
	plan, failure := r.adapter.PrepareRecord(ctx, record)
	if failure != nil {
		r.metrics.rejections.WithLabelValues("prepare").Inc()
		return nil, failure
	}
	if plan == nil || plan.Command != record.Command() || plan.ID != record.Index() || plan.Command.GetNative() != nil {
		return nil, protocol.Fail(pb.FailureCode_INTERNAL, "invalid prepared record")
	}
	plan.Bytes = max(plan.Bytes, record.Bytes())
	plan.ResultBytes = max(plan.ResultBytes, execution.ResultOverheadBytes)
	return plan, nil
}

func (r *Runtime) PrepareCommand(ctx context.Context, id uint64, call *pb.Command) (*execution.Plan, *pb.Failure) {
	plan, failure := r.adapter.PrepareCommand(ctx, id, call)
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
	singleton := plan != nil && plan.Command.GetNative() != nil
	if singleton && session == nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "streaming plans require an event consumer"), changed
	}
	if plan == nil || plan.Bytes < execution.EntryOverheadBytes || plan.ResultBytes < execution.ResultOverheadBytes || plan.WorkingBytes < 0 {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid execution accounting"), changed
	}
	if !r.queueCapacityLocked(plan) {
		if session == nil {
			r.metrics.rejections.WithLabelValues("capacity").Inc()
		}
		return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "admission capacity exhausted"), changed
	}
	ticketContext, cancel := context.WithCancel(ctx)
	ticket := &Ticket{runtime: r, session: session, ctx: ticketContext, cancel: cancel, plan: plan, ready: make(chan struct{}), queuedAt: time.Now()}
	if session != nil && plan.Command.GetNative() != nil && session.Events == nil {
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

// A single large operation proceeds alone instead of being rejected by the
// batching window. Further waiting work resumes after dispatch releases credit.
func (r *Runtime) queueCapacityLocked(plan *execution.Plan) bool {
	return len(r.queue) < r.limits.QueueOperations && (len(r.queue) == 0 || plan.Bytes <= r.limits.QueueBytes-r.pendingBytes)
}

func (r *Runtime) retainResultLocked(ticket *Ticket, bytes int) {
	if _, live := r.live[ticket]; !live {
		return
	}
	if bytes > ticket.plan.ResultBytes {
		r.resultBytes += bytes - ticket.plan.ResultBytes
		ticket.plan.ResultBytes = bytes
	}
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
			r.pendingBytes -= t.plan.Bytes
			failure := protocol.ContextFailure(t.ctx)
			if r.closed {
				failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "shutdown deadline")
			}
			var event *pb.Event
			if t.plan.Command.GetRead() != nil || t.plan.Command.GetMutate() != nil {
				event = execution.FailedEvent(t.plan.Command, pb.MutationOutcome_NOT_STARTED, failure)
			}
			r.completeLocked(t, event)
			if t.session != nil && t.session.Events != nil && !t.acked {
				r.startPublisherLocked(t)
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
	var timer *time.Timer
	var tick <-chan time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		select {
		case <-r.wake:
		case <-tick:
		}
		r.mu.Lock()
		r.cancelQueuedLocked()
		for b := range r.batches {
			interested := false
			for _, t := range b.items {
				interested = interested || t.ctx.Err() == nil && !t.abandoned
			}
			if !interested {
				// Preserve a deadline's cause when the final caller expires.
				deadline, bounded := b.ctx.Deadline()
				if !bounded || time.Now().Before(deadline) {
					b.cancel()
				}
			}
		}
		var wakeAt time.Time
		for !r.closed {
			b, next := r.selectLocked(time.Now())
			if b == nil {
				wakeAt = next
				break
			}
			go r.execute(b)
		}
		finish := r.draining && len(r.queue) == 0 && len(r.batches) == 0 && r.publishers == 0
		r.mu.Unlock()
		if finish {
			return
		}
		if timer != nil {
			timer.Stop()
		}
		tick = nil
		if !wakeAt.IsZero() {
			if timer == nil {
				timer = time.NewTimer(time.Until(wakeAt))
			} else {
				timer.Reset(time.Until(wakeAt))
			}
			tick = timer.C
		}
	}
}

type batchCandidate struct {
	items     []*Ticket
	records   map[string]bool
	bytes     int
	singleton bool
	full      bool
}

type batchGroup struct {
	backend string
	read    bool
}

// selectLocked returns the earliest collection deadline when every eligible
// batch is still collecting. Reads, full batches and streaming commands bypass it.
func (r *Runtime) selectLocked(now time.Time) (*batch, time.Time) {
	var next time.Time
	if r.limits.MaxInFlight > 0 && len(r.batches) >= r.limits.MaxInFlight {
		return nil, next
	}
	seen := make(map[resourceKey]bool)
	groups := make(map[batchGroup]*batchCandidate)
	var candidates []*batchCandidate
	oldestReady := false
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
		singleton := t.plan.Command.GetNative() != nil
		// Separate completion and collection windows for reads and mutations.
		group := batchGroup{backend: t.plan.BatchKey, read: t.plan.Command.GetRead() != nil}
		candidate := groups[group]
		// Once the oldest candidate is ready, later groups cannot win this
		// dispatch. Still observe their session keys above to preserve ordering.
		if oldestReady && (singleton || candidate != candidates[0]) {
			continue
		}
		if candidate == nil || singleton {
			candidate = &batchCandidate{records: make(map[string]bool), singleton: singleton}
			candidates = append(candidates, candidate)
			if !singleton {
				groups[group] = candidate
			}
		}
		previousWrite, duplicate := candidate.records[t.plan.Key]
		if len(candidate.items) > 0 && (duplicate && (previousWrite || t.plan.Command.GetMutate() != nil)) {
			continue
		}
		if len(candidate.items) >= r.limits.BatchOperations || len(candidate.items) > 0 && t.plan.Bytes > r.limits.BatchBytes-candidate.bytes {
			candidate.full = true
			oldestReady = oldestReady || candidate == candidates[0]
			continue
		}
		candidate.items = append(candidate.items, t)
		candidate.records[t.plan.Key] = t.plan.Command.GetMutate() != nil
		candidate.bytes += t.plan.Bytes
		candidate.full = candidate.full || len(candidate.items) >= r.limits.BatchOperations || candidate.bytes >= r.limits.BatchBytes
		if candidate == candidates[0] {
			if !oldestReady {
				at := candidate.items[0].queuedAt.Add(r.limits.BatchWait)
				oldestReady = r.limits.BatchWait == 0 || candidate.full || singleton || t.plan.Command.GetRead() != nil || r.draining || !now.Before(at)
				if deadline, bounded := t.ctx.Deadline(); bounded && deadline.Before(at) {
					oldestReady = true
				}
			}
			if singleton || len(candidate.items) >= r.limits.BatchOperations {
				break
			}
		}
	}
	var items []*Ticket
	for _, candidate := range candidates {
		at := candidate.items[0].queuedAt.Add(r.limits.BatchWait)
		if r.limits.BatchWait == 0 || candidate.full || candidate.singleton || r.draining {
			at = now
		}
		for _, ticket := range candidate.items {
			if ticket.plan.Command.GetRead() != nil {
				at = now
				break
			}
			if deadline, bounded := ticket.ctx.Deadline(); bounded && deadline.Before(at) {
				at = now
			}
		}
		if now.Before(at) {
			if next.IsZero() || at.Before(next) {
				next = at
			}
			continue
		}
		items = candidate.items
		break
	}
	if len(items) == 0 {
		return nil, next
	}
	seed := items[0]
	selected := make(map[*Ticket]bool, len(items))
	for _, ticket := range items {
		selected[ticket] = true
	}
	workingBytes := 0
	var latest time.Time
	allBounded := true
	for _, ticket := range items {
		if ticket.ctx.Err() != nil || ticket.abandoned {
			r.cancelQueuedLocked()
			return nil, next
		}
		workingBytes = max(workingBytes, ticket.plan.WorkingBytes)
		deadline, bounded := ticket.ctx.Deadline()
		allBounded = allBounded && bounded
		if deadline.After(latest) {
			latest = deadline
		}
	}
	// A shared batch lives while any caller remains interested. An unbounded
	// caller must not inherit another caller's deadline. Per-plan contexts still
	// carry individual cancellation and deadline evidence into the adapters.
	var ctx context.Context
	var cancel context.CancelFunc
	if len(items) == 1 {
		ctx, cancel = context.WithCancel(seed.ctx)
	} else if allBounded {
		ctx, cancel = context.WithDeadline(context.Background(), latest)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	b := &batch{ctx: ctx, items: items, cancel: cancel, workingBytes: workingBytes}
	keep := r.queue[:0]
	for _, t := range r.queue {
		if selected[t] {
			r.pendingBytes -= t.plan.Bytes
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
	return b, next
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
func (r *Runtime) startPublisherLocked(t *Ticket) {
	r.publishers++
	t.publishing = true
	go func() {
		emission := &Emission{done: make(chan struct{})}
		select {
		case t.session.Events <- emission:
			select {
			case <-emission.done:
			case <-t.ctx.Done():
			}
		case <-t.ctx.Done():
		}
		r.mu.Lock()
		r.publishers--
		t.publishing = false
		if t.acked || t.abandoned || t.session.closed {
			r.releaseLocked(t)
		}
		r.notifyLocked()
		r.mu.Unlock()
	}()
}

func (r *Runtime) execute(b *batch) {
	plans := make([]*execution.Plan, len(b.items))
	positions := make(map[*execution.Plan]*Ticket, len(b.items))
	for i, t := range b.items {
		plan := *t.plan
		plan.Context = t.ctx
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
			r.mu.Lock()
			r.retainResultLocked(ticket, proto.Size(event))
			r.mu.Unlock()
			ticket.event = event
			return nil
		}
		if plan.Command.GetNative() != nil {
			r.mu.Lock()
			r.retainResultLocked(ticket, proto.Size(event)+execution.ResultOverheadBytes)
			r.mu.Unlock()
			return ticket.emit(event)
		}
		return fmt.Errorf("adapter emitted unknown command result")
	}
	r.metrics.executions.WithLabelValues("execution").Inc()
	r.metrics.batch.Observe(float64(len(plans)))
	started := time.Now()
	r.adapter.Execute(b.ctx, plans, emit)
	r.metrics.duration.WithLabelValues("execution").Observe(time.Since(started).Seconds())
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
		r.completeLocked(ticket, event)
		if ticket.session != nil && ticket.session.Events != nil && !ticket.acked {
			r.startPublisherLocked(ticket)
		}
	}
	r.notifyLocked()
}
func (r *Runtime) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot := Snapshot{Pending: len(r.queue), PendingBytes: r.pendingBytes, Active: len(r.batches), Retained: len(r.live), ResultBytes: r.resultBytes, WorkingBytes: r.workingBytes, Publishers: r.publishers, Draining: r.draining, Closed: r.closed, Overloaded: r.overloaded}
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
// resource ordering. Native uses a session-owned event publisher.
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
	case <-time.After(r.limits.CleanupTimeout):
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
