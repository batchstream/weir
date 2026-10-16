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
	"github.com/batchstream/weir/internal/overload"
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
	memory                                  *ExecutionMemory
	limits                                  Limits
	queue                                   []*Ticket
	queueHead, queueTail                    *Ticket
	nextOrder                               uint64
	candidates                              *batchCandidate
	selectionCutoff                         *Ticket
	live                                    map[*Ticket]struct{}
	batches                                 map[*batch]struct{}
	keys                                    map[resourceKey]*Ticket
	pendingBytes, resultBytes, workingBytes int
	publishers                              int
	paused, pausedBytes                     int
	resumeHead, resumeTail                  *batch
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
	executionLease   *executionLease
	batch            *batch
	blocked          bool
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
	queueIndex       int
	order            uint64
	queuePrev        *Ticket
	queueNext        *Ticket
	selectionNext    *Ticket
}
type resourceKey struct {
	session  *Session
	resource string
}

type batch struct {
	runtime      *Runtime
	native       *Ticket
	owner        *executionLease
	io           bool
	readWindow   int
	ctx          context.Context
	items        []*Ticket
	cancel       context.CancelFunc
	workingBytes int
	pausedBytes  int
	resume       chan struct{}
	resumeErr    error
	resumeOrder  uint64
	resumePrev   *batch
	resumeNext   *batch
}
type Snapshot struct {
	Ready, ReadyBytes                     int
	Pending, PendingBytes                 int
	Active, Retained                      int
	ResultBytes, WorkingBytes, Publishers int
	Paused, PausedBytes                   int
	Draining, Closed, Overloaded          bool
}

func New(adapter execution.Adapter, limits Limits, memory *ExecutionMemory) (*Runtime, error) {
	if adapter == nil {
		return nil, fmt.Errorf("missing adapter")
	}
	if err := limits.Validate(); err != nil {
		_ = adapter.Close()
		return nil, err
	}
	runtime := newRuntime(adapter, limits, memory)
	go runtime.loop()
	return runtime, nil
}
func newRuntime(adapter execution.Adapter, limits Limits, memory *ExecutionMemory) *Runtime {
	if limits.CleanupTimeout == 0 {
		limits.CleanupTimeout = DefaultLimits().CleanupTimeout
	}
	runtime := &Runtime{adapter: adapter, limits: limits, live: make(map[*Ticket]struct{}), batches: make(map[*batch]struct{}), keys: make(map[resourceKey]*Ticket), wake: make(chan struct{}, 1), changed: make(chan struct{}), done: make(chan struct{})}
	if memory == nil {
		memory = NewExecutionMemory()
		guard := overload.New(nil, overload.DefaultLimits())
		memory.ObserveMemory(guard.Snapshot())
	}
	runtime.memory = memory
	memory.register(runtime.wake)
	runtime.metrics = newRuntimeMetrics()
	return runtime
}
func (r *Runtime) NewSession() *Session {
	session := &Session{runtime: r}
	return session
}

func (r *Runtime) ExecutionMemory() *ExecutionMemory { return r.memory }

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
		// An already-accounted native receive source may wait for an actual
		// queue vacancy. Direct callers without a live same-Node owner must not
		// create an uncharged queue outside this bounded scheduler.
		if r.memory.ownsIngress(plan) {
			return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "admission capacity exhausted"), changed
		}
		r.metrics.rejections.WithLabelValues("capacity").Inc()
		return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "admission capacity exhausted"), nil
	}
	lease, failure, wait := r.memory.admit(plan)
	if failure != nil {
		if wait {
			return nil, failure, changed
		}
		return nil, failure, nil
	}
	ticketContext, cancel := context.WithCancel(ctx)
	ownedPlan := *plan
	ownedPlan.Context = ticketContext
	ticket := &Ticket{executionLease: lease, runtime: r, session: session, ctx: ticketContext, cancel: cancel, plan: &ownedPlan, ready: make(chan struct{}), queuedAt: time.Now()}
	ownedPlan.Memory = ticket
	if session != nil && ownedPlan.Command.GetNative() != nil && session.Events == nil {
		session.Events = make(chan *Emission, 1)
	}
	r.enqueueLocked(ticket)
	r.live[ticket] = struct{}{}
	r.pendingBytes += ownedPlan.Bytes
	r.resultBytes += ownedPlan.ResultBytes
	ticket.stopWatch = context.AfterFunc(ticketContext, r.signal)
	r.signal()
	return ticket, nil, changed
}

// A single large operation proceeds alone instead of being rejected by the
// batching window. Further waiting work resumes after dispatch releases credit.
func (r *Runtime) queueCapacityLocked(plan *execution.Plan) bool {
	return len(r.queue) < r.limits.QueueOperations && (len(r.queue) == 0 || plan.Bytes <= r.limits.QueueBytes-r.pendingBytes)
}

// The slice bounds membership and supports constant-time removal. The linked
// order keeps dispatch FIFO when removing a ticket swaps another slice entry.
func (r *Runtime) enqueueLocked(ticket *Ticket) {
	ticket.order = r.nextOrder
	r.nextOrder++
	ticket.queueIndex = len(r.queue)
	ticket.queuePrev = r.queueTail
	if r.queueTail == nil {
		r.queueHead = ticket
	} else {
		r.queueTail.queueNext = ticket
	}
	r.queueTail = ticket
	r.queue = append(r.queue, ticket)
	r.candidates = nil
}

func (r *Runtime) dequeueLocked(ticket *Ticket) {
	if ticket.queuePrev == nil {
		r.queueHead = ticket.queueNext
	} else {
		ticket.queuePrev.queueNext = ticket.queueNext
	}
	if ticket.queueNext == nil {
		r.queueTail = ticket.queuePrev
	} else {
		ticket.queueNext.queuePrev = ticket.queuePrev
	}
	last := len(r.queue) - 1
	moved := r.queue[last]
	r.queue[ticket.queueIndex] = moved
	moved.queueIndex = ticket.queueIndex
	r.queue[last] = nil
	r.queue = r.queue[:last]
	ticket.queuePrev, ticket.queueNext = nil, nil
	ticket.selectionNext = nil
	r.pendingBytes -= ticket.plan.Bytes
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

func (t *Ticket) SetWorking(bytes int) error {
	r := t.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.batch == nil {
		return execution.ErrExecutionMemory
	}
	b := t.batch
	if err := b.owner.setWorking(bytes); err != nil {
		return err
	}
	r.workingBytes += bytes - b.workingBytes
	b.workingBytes = bytes
	return nil
}

func (t *Ticket) ReserveResult(bytes int) error {
	r := t.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.plan == nil {
		return execution.ErrExecutionMemory
	}
	if err := t.executionLease.reserveResult(bytes); err != nil {
		return err
	}
	bytes = max(bytes, execution.ResultOverheadBytes)
	r.resultBytes += bytes - t.plan.ResultBytes
	t.plan.ResultBytes = bytes
	return nil
}

func (t *Ticket) ReserveWire(bytes int) (*WireReservation, error) {
	r := t.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.executionLease == nil {
		return nil, execution.ErrExecutionMemory
	}
	return t.executionLease.reserveWire(bytes)
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
	if t.executionLease != nil {
		t.executionLease.release()
		t.executionLease = nil
	}
	r.resultBytes -= t.plan.ResultBytes
	t.plan = nil
	t.event = nil
}
func (r *Runtime) completeLocked(t *Ticket, event *pb.Event) {
	if t.plan.Command.GetNative() != nil {
		if event == nil {
			failure := protocol.ContextFailure(t.ctx)
			end := protocol.NativeFailure(false, failure)
			value := &pb.Event_NativeEnd{NativeEnd: end}
			event = &pb.Event{Value: value}
		}
		if err := t.executionLease.terminal(proto.Size(event)); err != nil {
			started := event.GetNativeEnd().GetCompletion() != pb.NativeCompletion_NATIVE_NOT_STARTED
			failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, errExecutionMemory.Error())
			end := protocol.NativeFailure(started, failure)
			value := &pb.Event_NativeEnd{NativeEnd: end}
			event = &pb.Event{Value: value}
			// Every admitted lease already owns a fixed terminal window;
			// this fixed failure fits without borrowing further capacity.
			_ = t.executionLease.terminal(proto.Size(event))
		}
		result := proto.Size(event) + execution.ResultOverheadBytes
		r.resultBytes += result - t.plan.ResultBytes
		t.plan.ResultBytes = result
	} else {
		if err := t.executionLease.finishResult(proto.Size(event)+execution.ResultOverheadBytes, event.GetReadResult().GetDocument() != nil); err != nil {
			failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, execution.ErrExecutionMemory.Error())
			event = execution.FailedEvent(t.plan.Command, pb.MutationOutcome_NOT_STARTED, failure)
			_ = t.executionLease.finishResult(execution.ResultOverheadBytes, false)
		}
		t.executionLease.markTerminal()
	}
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
		r.candidates = nil
	}
	if t.abandoned || t.session != nil && t.session.closed {
		r.releaseLocked(t)
	}
}
func (r *Runtime) cancelQueuedLocked() {
	changed := false
	for t := r.queueHead; t != nil; {
		next := t.queueNext
		if t.ctx.Err() != nil || t.abandoned || r.closed {
			r.dequeueLocked(t)
			r.candidates = nil
			changed = true
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
		}
		t = next
	}
	if changed {
		r.notifyLocked()
	}
}
func (r *Runtime) loop() {
	defer close(r.done)
	defer r.memory.unregister(r.wake)
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
		// Memory releases and trusted-capacity updates signal the same loop as
		// dispatch. Broadcast them even if no Store queue item can be selected;
		// Submit's already-owned input may be waiting only for that credit.
		close(r.changed)
		r.changed = make(chan struct{})
		r.cancelQueuedLocked()
		for t := r.queueHead; t != nil; t = t.queueNext {
			t.blocked = false
		}
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
	items                    []*Ticket
	pendingHead, pendingTail *Ticket
	singleton                bool
	full                     bool
	next                     *batchCandidate
}

type batchGroup struct {
	backend string
	read    bool
}

// collectLocked builds one selection round. Every eligible group is examined
// once, then each dispatch removes its tickets directly instead of rescanning
// and compacting the entire queue. Admission, cancellation and a released
// session dependency start a fresh round.
func (r *Runtime) collectLocked(now time.Time) {
	var seen map[resourceKey]struct{}
	groups := make(map[batchGroup]*batchCandidate)
	var tail *batchCandidate
	r.selectionCutoff = nil
	ready, slots := 0, len(r.queue)
	if r.limits.MaxInFlight > 0 {
		slots = r.limits.MaxInFlight - (len(r.batches) - r.paused)
	}
	for t := r.queueHead; t != nil; t = t.queueNext {
		if t.blocked {
			continue
		}
		if t.session != nil && t.plan.Command.GetMutate() != nil && t.plan.Key != "" {
			key := resourceKey{session: t.session, resource: t.plan.Key}
			if _, duplicate := seen[key]; duplicate || r.keys[key] != nil {
				continue
			}
			if seen == nil {
				seen = make(map[resourceKey]struct{})
			}
			seen[key] = struct{}{}
		}
		singleton := t.plan.Command.GetNative() != nil
		group := batchGroup{backend: t.plan.BatchKey, read: t.plan.Command.GetRead() != nil}
		candidate := groups[group]
		if candidate == nil || singleton {
			// A busy runtime needs only enough ready groups to fill its free
			// slots. New admissions can invalidate this round immediately;
			// avoid allocating candidates they cannot dispatch yet.
			if ready >= slots {
				if r.selectionCutoff == nil {
					r.selectionCutoff = t
				}
				continue
			}
			candidate = &batchCandidate{singleton: singleton}
			if tail == nil {
				r.candidates = candidate
			} else {
				tail.next = candidate
			}
			tail = candidate
			if !singleton {
				groups[group] = candidate
			}
			at := t.queuedAt.Add(r.limits.BatchWait)
			deadline, bounded := t.ctx.Deadline()
			if singleton || group.read || r.draining || r.limits.BatchOperations == 1 || t.plan.Bytes >= r.limits.BatchBytes || !now.Before(at) || bounded && deadline.Before(at) {
				ready++
			}
		}
		t.selectionNext = nil
		if candidate.pendingTail == nil {
			candidate.pendingHead = t
		} else {
			candidate.pendingTail.selectionNext = t
		}
		candidate.pendingTail = t
	}
	for candidate := r.candidates; candidate != nil; candidate = candidate.next {
		r.fillCandidateLocked(candidate)
	}
}

func (r *Runtime) fillCandidateLocked(candidate *batchCandidate) {
	candidate.items = nil
	candidate.full = false
	var records map[string]struct{}
	if candidate.pendingHead.plan.Command.GetMutate() != nil {
		records = make(map[string]struct{})
	}
	var previous *Ticket
	bytes := 0
	for ticket := candidate.pendingHead; ticket != nil; {
		if len(candidate.items) >= r.limits.BatchOperations || len(candidate.items) > 0 && bytes >= r.limits.BatchBytes {
			break
		}
		next := ticket.selectionNext
		if _, duplicate := records[ticket.plan.Key]; duplicate {
			previous = ticket
			ticket = next
			continue
		}
		if len(candidate.items) > 0 && ticket.plan.Bytes > r.limits.BatchBytes-bytes {
			candidate.full = true
			previous = ticket
			ticket = next
			continue
		}
		candidate.items = append(candidate.items, ticket)
		if records != nil {
			records[ticket.plan.Key] = struct{}{}
		}
		bytes += ticket.plan.Bytes
		if previous == nil {
			candidate.pendingHead = next
		} else {
			previous.selectionNext = next
		}
		if next == nil {
			candidate.pendingTail = previous
		}
		ticket.selectionNext = nil
		ticket = next
	}
	candidate.full = candidate.full || len(candidate.items) >= r.limits.BatchOperations || bytes >= r.limits.BatchBytes
}

func (r *Runtime) reinsertCandidateLocked(candidate *batchCandidate) {
	r.fillCandidateLocked(candidate)
	order := candidate.items[0].order
	var previous *batchCandidate
	next := r.candidates
	for next != nil && next.items[0].order < order {
		previous = next
		next = next.next
	}
	candidate.next = next
	if previous == nil {
		r.candidates = candidate
	} else {
		previous.next = candidate
	}
}

// selectLocked returns the earliest collection deadline when every eligible
// batch is still collecting. Reads, full batches and streaming commands bypass it.
func (r *Runtime) selectLocked(now time.Time) (*batch, time.Time) {
	var next time.Time
	if r.limits.MaxInFlight > 0 && len(r.batches)-r.paused >= r.limits.MaxInFlight {
		return nil, next
	}
	var chosen, previous *batchCandidate
	if r.limits.BatchOperations == 1 {
		// A one-record batch is full immediately and cannot combine work.
		// Select its oldest eligible ticket without building group backlogs.
		r.candidates = nil
		r.selectionCutoff = nil
		for ticket := r.queueHead; ticket != nil; ticket = ticket.queueNext {
			if ticket.blocked {
				continue
			}
			if ticket.session != nil && ticket.plan.Command.GetMutate() != nil && ticket.plan.Key != "" {
				key := resourceKey{session: ticket.session, resource: ticket.plan.Key}
				if r.keys[key] != nil {
					continue
				}
			}
			items := []*Ticket{ticket}
			chosen = &batchCandidate{items: items}
			break
		}
	} else {
		if r.candidates == nil {
			r.collectLocked(now)
		}
		for candidate := r.candidates; candidate != nil; candidate = candidate.next {
			if r.selectionCutoff != nil && candidate.items[0].order > r.selectionCutoff.order {
				// Refilled groups must not overtake an older group omitted by the
				// free-slot bound. Rebuild with that oldest pending admission.
				r.candidates = nil
				return r.selectLocked(now)
			}
			at := candidate.items[0].queuedAt.Add(r.limits.BatchWait)
			if r.limits.BatchWait == 0 || candidate.full || candidate.singleton || r.draining || candidate.items[0].plan.Command.GetRead() != nil {
				at = now
			}
			for _, ticket := range candidate.items {
				if deadline, bounded := ticket.ctx.Deadline(); bounded && deadline.Before(at) {
					at = now
				}
			}
			if now.Before(at) {
				if next.IsZero() || at.Before(next) {
					next = at
				}
				previous = candidate
				continue
			}
			chosen = candidate
			break
		}
	}
	if chosen == nil && r.selectionCutoff != nil {
		r.candidates = nil
		return r.selectLocked(now)
	}
	if r.resumeHead != nil && (chosen == nil || r.resumeHead.resumeOrder < chosen.items[0].order) {
		b := r.resumeHead
		started, waiting := b.native.executionLease.beginRead(b.readWindow)
		if !started && waiting {
			if chosen == nil {
				return nil, next
			}
		} else {
			r.removeResumeLocked(b)
			if started {
				r.releasePauseLocked(b)
				b.io = true
			} else {
				b.resumeErr = errExecutionMemory
			}
			close(b.resume)
			b.resume = nil
			r.notifyLocked()
			return r.selectLocked(now)
		}
	}
	if chosen == nil {
		return nil, next
	}
	items := chosen.items
	seed := items[0]
	workingBytes, reserve := 0, 0
	var latest time.Time
	allBounded := true
	for _, ticket := range items {
		if ticket.ctx.Err() != nil || ticket.abandoned {
			r.cancelQueuedLocked()
			return nil, next
		}
		workingBytes += ticket.plan.WorkingBytes
		reserve = max(reserve, ticket.plan.IOReserve)
		deadline, bounded := ticket.ctx.Deadline()
		allBounded = allBounded && bounded
		if deadline.After(latest) {
			latest = deadline
		}
	}
	if seed.plan.Command.GetNative() == nil {
		workingBytes += reserve
	}
	started, waiting := seed.executionLease.start(workingBytes, reserve)
	if !started {
		r.candidates = nil
		for _, ticket := range items {
			if waiting {
				ticket.blocked = true
				continue
			}
			r.dequeueLocked(ticket)
			failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, errExecutionMemory.Error())
			if ticket.plan.Command.GetNative() != nil {
				end := protocol.NativeFailure(false, failure)
				value := &pb.Event_NativeEnd{NativeEnd: end}
				event := &pb.Event{Value: value}
				r.completeLocked(ticket, event)
				r.startPublisherLocked(ticket)
			} else {
				event := execution.FailedEvent(ticket.plan.Command, pb.MutationOutcome_NOT_STARTED, failure)
				r.completeLocked(ticket, event)
			}
		}
		return r.selectLocked(now)
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
	b := &batch{runtime: r, owner: seed.executionLease, io: true, ctx: ctx, items: items, cancel: cancel, workingBytes: workingBytes}
	if seed.plan.Command.GetNative() != nil {
		b.native = seed
		seed.plan.NativeIO = b
	}
	if previous == nil {
		r.candidates = chosen.next
	} else {
		previous.next = chosen.next
	}
	for _, ticket := range items {
		r.dequeueLocked(ticket)
		r.metrics.queue.WithLabelValues("execution").Observe(now.Sub(ticket.queuedAt).Seconds())
		ticket.state = 1
		ticket.batch = b
		if ticket.session != nil && ticket.plan.Command.GetMutate() != nil && ticket.plan.Key != "" {
			key := resourceKey{session: ticket.session, resource: ticket.plan.Key}
			r.keys[key] = ticket
		}
	}
	if chosen.pendingHead != nil {
		r.reinsertCandidateLocked(chosen)
	}
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

// Native output already owns its retained working reservation. Publishing never
// owns backend IO, and never attempts to borrow the input queue's byte limit.
func (r *Runtime) emitNative(b *batch, ticket *Ticket, event *pb.Event) error {
	if event.GetNativeEnd() != nil {
		// The adapter has no more backend work. After Execute returns, the
		// existing completion path releases working memory and reserves only
		// this terminal, without borrowing another output handoff first.
		ticket.event = event
		return nil
	}
	// Synthetic/test adapters may use Emit without the explicit IO boundary.
	if err := b.EndIO(ticket.plan.WorkingBytes, proto.Size(event)); err != nil {
		return err
	}
	return ticket.emit(event)
}

func (b *batch) GrowIO(working int) error {
	m := b.native.executionLease.memory
	m.mu.Lock()
	m.reserve = max(m.reserve, uint64(b.native.plan.IOReserve)+executionProgressReserve)
	m.resizeInputLocked()
	m.mu.Unlock()
	return b.native.executionLease.grow(working)
}

func (b *batch) EndIO(working, eventBytes int) error {
	r := b.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := b.native.executionLease.settle(working, eventBytes); err != nil {
		return err
	}
	r.workingBytes += working - b.workingBytes
	b.workingBytes = working
	b.native.plan.WorkingBytes = working
	result := eventBytes + execution.ResultOverheadBytes
	r.resultBytes += result - b.native.plan.ResultBytes
	b.native.plan.ResultBytes = result
	if !b.io {
		r.pausedBytes -= b.pausedBytes
	}
	if b.io {
		b.io = false
		r.paused++
	}
	b.pausedBytes = b.native.plan.Bytes + working + b.native.plan.ResultBytes
	r.pausedBytes += b.pausedBytes
	r.notifyLocked()
	return nil
}

func (b *batch) BeginIO(ctx context.Context, extra int) error {
	r := b.runtime
	r.mu.Lock()
	if b.io {
		r.mu.Unlock()
		return nil
	}
	if ctx.Err() != nil {
		r.mu.Unlock()
		return ctx.Err()
	}
	b.readWindow = extra
	b.resumeErr = nil
	resume := make(chan struct{})
	b.resume = resume
	b.resumeOrder = r.nextOrder
	r.nextOrder++
	b.resumePrev = r.resumeTail
	if r.resumeTail == nil {
		r.resumeHead = b
	} else {
		r.resumeTail.resumeNext = b
	}
	r.resumeTail = b
	r.candidates = nil
	r.notifyLocked()
	r.mu.Unlock()
	select {
	case <-resume:
		if err := ctx.Err(); err != nil {
			return err
		}
		return b.resumeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runtime) removeResumeLocked(b *batch) {
	if b.resumePrev == nil {
		r.resumeHead = b.resumeNext
	} else {
		b.resumePrev.resumeNext = b.resumeNext
	}
	if b.resumeNext == nil {
		r.resumeTail = b.resumePrev
	} else {
		b.resumeNext.resumePrev = b.resumePrev
	}
	b.resumePrev, b.resumeNext = nil, nil
}

func (r *Runtime) releasePauseLocked(b *batch) {
	r.paused--
	r.pausedBytes -= b.pausedBytes
	b.pausedBytes = 0
}
func (r *Runtime) startPublisherLocked(t *Ticket) {
	r.publishers++
	t.publishing = true
	go func() {
		if t.plan.Command.GetNative() != nil && t.event != nil {
			_ = t.emit(t.event)
		}
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
		plans[i] = t.plan
		positions[t.plan] = t
	}
	nativeEnded := false
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
			if nativeEnded {
				return fmt.Errorf("Native emitted after terminal result")
			}
			nativeEnded = event.GetNativeEnd() != nil
			return r.emitNative(b, ticket, event)
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
	if b.native != nil {
		if b.native.event == nil {
			failure := protocol.ContextFailure(b.ctx)
			end := protocol.NativeFailure(true, failure)
			value := &pb.Event_NativeEnd{NativeEnd: end}
			b.native.event = &pb.Event{Value: value}
		}
	}
	r.workingBytes -= b.workingBytes
	if b.native == nil {
		b.owner.finishWorking()
	}
	for _, ticket := range b.items {
		ticket.batch = nil
	}
	if b.pausedBytes != 0 {
		if b.resume != nil {
			r.removeResumeLocked(b)
		}
		r.releasePauseLocked(b)
	}
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
	snapshot := Snapshot{Pending: len(r.queue), PendingBytes: r.pendingBytes, Active: len(r.batches) - r.paused, Paused: r.paused, PausedBytes: r.pausedBytes, Retained: len(r.live), ResultBytes: r.resultBytes, WorkingBytes: r.workingBytes, Publishers: r.publishers, Draining: r.draining, Closed: r.closed, Overloaded: r.overloaded}
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
