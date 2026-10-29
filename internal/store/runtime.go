// Package store queues prepared operations for one Store, groups compatible
// records into batches and runs them on the Store's adapter.
package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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
	MaxInFlight                 int // Record batches; zero allows unlimited.
	// MaxNative bounds running Native commands. They bypass the queue and the
	// record slots because each one lasts until its caller has read the
	// response; further Natives wait for a free slot.
	MaxNative int
}

func DefaultLimits() Limits {
	limits := Limits{QueueOperations: 1024, QueueBytes: 32 << 20, BatchOperations: 128, BatchBytes: 4 << 20, BatchWait: 20 * time.Millisecond, MaxInFlight: 16, MaxNative: 64, CleanupTimeout: 2 * time.Second}
	return limits
}

func (limits Limits) Validate() error {
	if limits.QueueOperations < 1 || limits.QueueBytes < 1 ||
		limits.BatchOperations < 1 || limits.BatchBytes < 1 || limits.MaxInFlight < 0 || limits.MaxNative < 1 || limits.CleanupTimeout <= 0 || limits.BatchWait < 0 {
		return fmt.Errorf("invalid batch, execution or queue bounds")
	}
	return nil
}

type Runtime struct {
	adapter execution.Adapter
	limits  Limits
	metrics runtimeMetrics

	mu                           sync.Mutex
	queue                        []*Ticket
	queueBytes                   int
	batches                      map[*batch]struct{}
	records, natives             int
	keys                         map[resourceKey]*Ticket
	draining, closed, overloaded bool
	// space and nativeSpace are closed and replaced whenever a waiting record
	// or Native submitter may proceed.
	space, nativeSpace chan struct{}
	wake               chan struct{}
	done               chan struct{}
	// selection is selectLocked's scratch state, reused under mu.
	selection selection

	closeOnce sync.Once
	closeErr  error
}

// Session identifies one record stream. Its mutations of one resource execute
// one at a time, in submission order.
type Session struct {
	runtime *Runtime
}

// Ticket is one queued operation. Wait returns its final event.
type Ticket struct {
	ctx      context.Context
	cancel   context.CancelFunc
	stop     func() bool
	plan     *execution.Plan
	session  *Session
	emit     execution.Emit
	queuedAt time.Time
	started  bool
	event    *pb.Event
	done     chan struct{}
}

type resourceKey struct {
	session  *Session
	resource string
}

type batch struct {
	ctx    context.Context
	cancel context.CancelFunc
	items  []*Ticket
	stops  []func() bool
}

func New(adapter execution.Adapter, limits Limits) (*Runtime, error) {
	if adapter == nil {
		return nil, fmt.Errorf("missing adapter")
	}
	if err := limits.Validate(); err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), DefaultLimits().CleanupTimeout)
		defer cancel()
		_ = adapter.Close(cleanup)
		return nil, err
	}
	runtime := &Runtime{
		adapter:     adapter,
		limits:      limits,
		metrics:     newRuntimeMetrics(),
		batches:     make(map[*batch]struct{}),
		keys:        make(map[resourceKey]*Ticket),
		space:       make(chan struct{}),
		nativeSpace: make(chan struct{}),
		wake:        make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
	go runtime.loop()
	return runtime, nil
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
	if plan == nil || plan.Command != record.Command() {
		return nil, protocol.Fail(pb.FailureCode_INTERNAL, "invalid prepared record")
	}
	plan.Bytes = max(plan.Bytes, record.Bytes())
	return plan, nil
}

func (r *Runtime) PrepareNative(ctx context.Context, command *pb.Command) (*execution.Plan, *pb.Failure) {
	plan, failure := r.adapter.PrepareNative(ctx, command)
	if failure != nil {
		r.metrics.rejections.WithLabelValues("prepare").Inc()
		return nil, failure
	}
	if plan == nil || plan.Command != command {
		return nil, protocol.Fail(pb.FailureCode_INTERNAL, "invalid prepared Native command")
	}
	plan.Bytes = max(plan.Bytes, proto.Size(command))
	return plan, nil
}

// Submit queues a prepared record. It waits for queue space and fails when the
// Store drains, the process is overloaded or the caller's context ends.
func (r *Runtime) Submit(ctx context.Context, plan *execution.Plan, session *Session) (*Ticket, *pb.Failure) {
	if plan == nil || plan.Command.GetRead() == nil && plan.Command.GetMutate() == nil || session != nil && session.runtime != r {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid record plan")
	}
	ticket := &Ticket{plan: plan, session: session}
	return r.enqueue(ctx, ticket)
}

// SubmitNative waits for a free Native slot and starts the command. Its
// response head and chunks are sent through emit.
func (r *Runtime) SubmitNative(ctx context.Context, plan *execution.Plan, emit execution.Emit) (*Ticket, *pb.Failure) {
	if plan == nil || plan.Command.GetNative() == nil || emit == nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid Native plan")
	}
	ticket := &Ticket{plan: plan, emit: emit}
	return r.enqueue(ctx, ticket)
}

func (r *Runtime) enqueue(ctx context.Context, ticket *Ticket) (*Ticket, *pb.Failure) {
	native := ticket.plan.Command.GetNative() != nil
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		if r.draining || r.closed {
			r.metrics.rejections.WithLabelValues("draining").Inc()
			return nil, protocol.Fail(pb.FailureCode_UNAVAILABLE, "store draining")
		}
		if r.overloaded {
			r.metrics.rejections.WithLabelValues("overload").Inc()
			return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "process overloaded")
		}
		if ctx.Err() != nil {
			return nil, protocol.ContextFailure(ctx)
		}
		// A single record larger than QueueBytes waits for an empty queue
		// instead of being rejected.
		fits := len(r.queue) < r.limits.QueueOperations && (len(r.queue) == 0 || ticket.plan.Bytes <= r.limits.QueueBytes-r.queueBytes)
		if native && r.natives < r.limits.MaxNative || !native && fits {
			break
		}
		space := r.space
		if native {
			space = r.nativeSpace
		}
		r.mu.Unlock()
		select {
		case <-space:
		case <-ctx.Done():
		}
		r.mu.Lock()
	}
	plan := *ticket.plan
	ticket.ctx, ticket.cancel = context.WithCancel(ctx)
	plan.Context = ticket.ctx
	ticket.plan = &plan
	ticket.queuedAt = time.Now()
	ticket.done = make(chan struct{})
	ticket.stop = context.AfterFunc(ticket.ctx, r.signal)
	if native {
		items := []*Ticket{ticket}
		r.launchLocked(items)
		return ticket, nil
	}
	r.queue = append(r.queue, ticket)
	r.queueBytes += plan.Bytes
	r.signal()
	return ticket, nil
}

// Wait returns a record result or the Native NativeEnd. A caller that stops
// waiting abandons the operation.
func (t *Ticket) Wait(ctx context.Context) (*pb.Event, error) {
	select {
	case <-t.done:
		return t.event, nil
	case <-ctx.Done():
		t.cancel()
		return nil, ctx.Err()
	}
}

func (r *Runtime) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *Runtime) notifySpaceLocked() {
	close(r.space)
	r.space = make(chan struct{})
}

func (r *Runtime) notifyNativeLocked() {
	close(r.nativeSpace)
	r.nativeSpace = make(chan struct{})
}

func (r *Runtime) loop() {
	defer close(r.done)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	for {
		r.mu.Lock()
		r.dropCanceledLocked()
		wakeAt := r.dispatchLocked(time.Now())
		finished := r.draining && len(r.queue) == 0 && len(r.batches) == 0
		r.mu.Unlock()
		if finished {
			return
		}
		var tick <-chan time.Time
		if !wakeAt.IsZero() {
			timer.Reset(time.Until(wakeAt))
			tick = timer.C
		}
		select {
		case <-r.wake:
		case <-tick:
		}
		timer.Stop()
	}
}

// dropCanceledLocked completes queued operations whose caller has gone, and
// every queued operation once the runtime is closed. None of them started.
func (r *Runtime) dropCanceledLocked() {
	kept := r.queue[:0]
	for _, t := range r.queue {
		if t.ctx.Err() == nil && !r.closed {
			kept = append(kept, t)
			continue
		}
		r.queueBytes -= t.plan.Bytes
		failure := protocol.ContextFailure(t.ctx)
		if r.closed {
			failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "shutdown deadline")
		}
		event := execution.FailedEvent(t.plan.Command, pb.MutationOutcome_NOT_STARTED, failure)
		r.completeLocked(t, event)
	}
	if len(kept) != len(r.queue) {
		clear(r.queue[len(kept):])
		r.queue = kept
		r.notifySpaceLocked()
	}
}

// dispatchLocked starts ready batches while record slots are free and
// returns when the earliest still-collecting batch becomes ready.
func (r *Runtime) dispatchLocked(now time.Time) time.Time {
	for !r.closed {
		items, wakeAt := r.selectLocked(now)
		if items == nil {
			return wakeAt
		}
		r.startLocked(items, now)
	}
	var never time.Time
	return never
}

type groupKey struct {
	batch string
	read  bool
}

type group struct {
	items []*Ticket
	bytes int
	full  bool
	// Mutations of one resource never share a batch.
	resources map[string]struct{}
}

type selection struct {
	groups  []*group
	used    int
	byKey   map[groupKey]*group
	ordered map[resourceKey]struct{}
}

// reset empties the scratch state after a scan so it retains no tickets; it
// keeps the maps and the slices of groups that were not handed to a batch.
func (s *selection) reset() {
	for _, g := range s.groups[:s.used] {
		clear(g.items)
		g.items = g.items[:0]
		g.bytes, g.full = 0, false
		clear(g.resources)
	}
	s.used = 0
	clear(s.byKey)
	clear(s.ordered)
}

func (s *selection) group(key groupKey) *group {
	if g := s.byKey[key]; g != nil {
		return g
	}
	if s.used == len(s.groups) {
		g := &group{}
		s.groups = append(s.groups, g)
	}
	g := s.groups[s.used]
	s.used++
	s.byKey[key] = g
	return g
}

// selectLocked scans the queue in FIFO order, groups compatible records and
// returns the oldest ready group while a record slot is free.
func (r *Runtime) selectLocked(now time.Time) ([]*Ticket, time.Time) {
	var wakeAt time.Time
	if r.limits.MaxInFlight > 0 && r.records >= r.limits.MaxInFlight {
		return nil, wakeAt
	}
	s := &r.selection
	if s.byKey == nil {
		s.byKey = make(map[groupKey]*group)
		s.ordered = make(map[resourceKey]struct{})
	}
	defer s.reset()
	for _, t := range r.queue {
		plan := t.plan
		mutation := plan.Command.GetMutate() != nil
		if t.session != nil && mutation {
			// The oldest queued mutation of a session's resource runs first,
			// and only after the previous one finished.
			key := resourceKey{session: t.session, resource: plan.Key}
			if _, waiting := s.ordered[key]; waiting || r.keys[key] != nil {
				continue
			}
			s.ordered[key] = struct{}{}
		}
		key := groupKey{batch: plan.BatchKey, read: plan.Command.GetRead() != nil}
		g := s.group(key)
		if _, duplicate := g.resources[plan.Key]; mutation && duplicate {
			continue
		}
		if len(g.items) >= r.limits.BatchOperations || len(g.items) > 0 && plan.Bytes > r.limits.BatchBytes-g.bytes {
			g.full = true
			continue
		}
		g.items = append(g.items, t)
		g.bytes += plan.Bytes
		if mutation {
			if g.resources == nil {
				g.resources = make(map[string]struct{})
			}
			g.resources[plan.Key] = struct{}{}
		}
	}
	for _, g := range s.groups[:s.used] {
		at := r.readyAt(g, now)
		if !now.Before(at) {
			// The batch owns the returned slice.
			items := g.items
			g.items = nil
			var ready time.Time
			return items, ready
		}
		if wakeAt.IsZero() || at.Before(wakeAt) {
			wakeAt = at
		}
	}
	return nil, wakeAt
}

// readyAt is when a group stops collecting. Mutations wait up to BatchWait for
// companions; reads, full batches and a draining Store do not
// wait, and neither does a batch holding a caller deadline inside the window.
func (r *Runtime) readyAt(g *group, now time.Time) time.Time {
	first := g.items[0]
	full := g.full || len(g.items) >= r.limits.BatchOperations || g.bytes >= r.limits.BatchBytes
	if r.draining || r.limits.BatchWait == 0 || first.plan.Command.GetMutate() == nil || full {
		return now
	}
	at := first.queuedAt.Add(r.limits.BatchWait)
	for _, t := range g.items {
		if deadline, bounded := t.ctx.Deadline(); bounded && deadline.Before(at) {
			return now
		}
	}
	return at
}

func (r *Runtime) startLocked(items []*Ticket, now time.Time) {
	for _, t := range items {
		t.started = true
		r.queueBytes -= t.plan.Bytes
		if t.session != nil && t.plan.Command.GetMutate() != nil {
			key := resourceKey{session: t.session, resource: t.plan.Key}
			r.keys[key] = t
		}
		r.metrics.queueWait.Observe(now.Sub(t.queuedAt).Seconds())
	}
	kept := r.queue[:0]
	for _, t := range r.queue {
		if !t.started {
			kept = append(kept, t)
		}
	}
	clear(r.queue[len(kept):])
	r.queue = kept
	r.notifySpaceLocked()
	r.launchLocked(items)
}

// launchLocked runs items as one adapter invocation.
func (r *Runtime) launchLocked(items []*Ticket) {
	for _, t := range items {
		t.stop()
	}
	b := newBatch(items)
	r.batches[b] = struct{}{}
	if b.native() {
		r.natives++
	} else {
		r.records++
	}
	go r.execute(b)
}

// newBatch gives a shared batch the lifetime of its callers: it ends at the
// latest caller deadline, stays unbounded for an unbounded caller, and is
// canceled once no caller remains interested. Each plan's Context still carries
// its own caller's cancellation into the adapter.
func newBatch(items []*Ticket) *batch {
	b := &batch{items: items}
	if len(items) == 1 {
		b.ctx, b.cancel = context.WithCancel(items[0].ctx)
		return b
	}
	var latest time.Time
	bounded := true
	for _, t := range items {
		deadline, ok := t.ctx.Deadline()
		bounded = bounded && ok
		if deadline.After(latest) {
			latest = deadline
		}
	}
	if bounded {
		b.ctx, b.cancel = context.WithDeadline(context.Background(), latest)
	} else {
		b.ctx, b.cancel = context.WithCancel(context.Background())
	}
	var live atomic.Int64
	live.Store(int64(len(items)))
	for _, t := range items {
		stop := context.AfterFunc(t.ctx, func() {
			if live.Add(-1) != 0 {
				return
			}
			// Keep the batch's own deadline as the cause when it already passed.
			if deadline, ok := b.ctx.Deadline(); ok && !time.Now().Before(deadline) {
				return
			}
			b.cancel()
		})
		b.stops = append(b.stops, stop)
	}
	return b
}

func (b *batch) native() bool {
	return b.items[0].plan.Command.GetNative() != nil
}

var errInvalidNativeEvent = errors.New("adapter emitted an invalid Native event")

func (r *Runtime) execute(b *batch) {
	started := time.Now()
	events := r.run(b)
	r.metrics.duration.Observe(time.Since(started).Seconds())
	for _, stop := range b.stops {
		stop()
	}
	b.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.batches, b)
	if b.native() {
		r.natives--
		r.notifyNativeLocked()
	} else {
		r.records--
	}
	for i, t := range b.items {
		if t.session != nil && t.plan.Command.GetMutate() != nil {
			key := resourceKey{session: t.session, resource: t.plan.Key}
			if r.keys[key] == t {
				delete(r.keys, key)
			}
		}
		r.completeLocked(t, events[i])
	}
	r.signal()
}

// run invokes the adapter and returns one valid final event per item.
func (r *Runtime) run(b *batch) []*pb.Event {
	r.metrics.executions.Inc()
	r.metrics.batch.Observe(float64(len(b.items)))
	first := b.items[0]
	if b.native() {
		emit := func(event *pb.Event) error {
			switch event.GetValue().(type) {
			case *pb.Event_Head, *pb.Event_Chunk:
				return first.emit(event)
			default:
				return errInvalidNativeEvent
			}
		}
		end := r.adapter.ExecuteNative(b.ctx, first.plan, emit)
		if end == nil {
			failure := protocol.Fail(pb.FailureCode_INTERNAL, "adapter returned no NativeEnd")
			end = protocol.NativeFailure(true, failure)
		}
		events := []*pb.Event{nativeEndEvent(end)}
		return events
	}
	plans := make([]*execution.Plan, len(b.items))
	for i, t := range b.items {
		plans[i] = t.plan
	}
	results := r.adapter.ExecuteRecords(b.ctx, plans)
	events := make([]*pb.Event, len(plans))
	for i, plan := range plans {
		var event *pb.Event
		if len(results) == len(plans) {
			event = results[i]
		}
		read := plan.Command.GetRead() != nil
		if read && event.GetReadResult() == nil || !read && event.GetMutationResult() == nil {
			failure := protocol.Fail(pb.FailureCode_INTERNAL, "adapter returned no valid result")
			event = execution.FailedEvent(plan.Command, pb.MutationOutcome_UNKNOWN, failure)
		}
		events[i] = event
	}
	return events
}

func nativeEndEvent(end *pb.NativeEnd) *pb.Event {
	value := &pb.Event_NativeEnd{NativeEnd: end}
	event := &pb.Event{Value: value}
	return event
}

func (r *Runtime) completeLocked(t *Ticket, event *pb.Event) {
	t.stop()
	t.event = event
	r.metrics.observe(t.plan.Command, event)
	t.cancel()
	close(t.done)
}

func (r *Runtime) SetOverloaded(value bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.overloaded != value {
		r.overloaded = value
		r.notifySpaceLocked()
		r.notifyNativeLocked()
	}
}

// BeginDrain rejects new operations and dispatches queued ones without waiting
// for batch companions.
func (r *Runtime) BeginDrain() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.draining {
		r.draining = true
		r.notifySpaceLocked()
		r.notifyNativeLocked()
	}
	r.signal()
}

// Close drains queued and executing work until ctx ends, then cancels what
// remains and gives the adapter the rest of CleanupTimeout to retire its I/O.
// Concurrent callers share one result.
func (r *Runtime) Close(ctx context.Context) error {
	r.closeOnce.Do(func() { r.closeErr = r.close(ctx) })
	return r.closeErr
}

func (r *Runtime) close(ctx context.Context) error {
	r.BeginDrain()
	select {
	case <-r.done:
	case <-ctx.Done():
	}
	started := time.Now()
	end := started.Add(r.limits.CleanupTimeout)
	grace := started.Add(3 * r.limits.CleanupTimeout / 4)
	closeEnd := started.Add(7 * r.limits.CleanupTimeout / 8)
	r.mu.Lock()
	r.closed = true
	for b := range r.batches {
		b.cancel()
		for _, t := range b.items {
			t.cancel()
		}
	}
	r.mu.Unlock()
	r.signal()

	timer := time.NewTimer(time.Until(grace))
	select {
	case <-r.done:
	case <-timer.C:
	}
	timer.Stop()
	cleanup, cancel := context.WithDeadline(context.Background(), closeEnd)
	defer cancel()
	// A stuck adapter must not turn Close into an unbounded wait.
	closed := make(chan error, 1)
	go func() { closed <- r.adapter.Close(cleanup) }()
	retirement := time.NewTimer(time.Until(end))
	defer retirement.Stop()
	executions := r.done
	var closeErr error
	for executions != nil || closed != nil {
		select {
		case <-executions:
			executions = nil
		case err := <-closed:
			closeErr = err
			closed = nil
		case <-retirement.C:
			joinErr := fmt.Errorf("backend retirement deadline exceeded (executions_joined=%t adapter_joined=%t)", executions == nil, closed == nil)
			return errors.Join(closeErr, joinErr)
		}
	}
	return closeErr
}
