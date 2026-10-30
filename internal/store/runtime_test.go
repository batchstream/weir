package store

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"google.golang.org/protobuf/proto"
)

type fakeAdapter struct {
	mu       sync.Mutex
	batches  [][]*execution.Plan
	started  chan []*execution.Plan
	records  func(context.Context, []*execution.Plan) []*pb.Event
	native   func(context.Context, *execution.Plan, execution.Emit) *pb.NativeEnd
	closes   atomic.Int32
	closeErr error
	closing  func(context.Context)
}

func (a *fakeAdapter) PrepareRecord(_ context.Context, record *execution.Record) (*execution.Plan, *pb.Failure) {
	plan := &execution.Plan{Command: record.Command(), Key: record.Key(), BatchKey: "records", Bytes: record.Bytes()}
	return plan, nil
}

func (a *fakeAdapter) PrepareNative(_ context.Context, command *pb.Command) (*execution.Plan, *pb.Failure) {
	plan := &execution.Plan{Command: command, Key: command.GetNative().GetResource()}
	return plan, nil
}

func (a *fakeAdapter) ExecuteRecords(ctx context.Context, plans []*execution.Plan) []*pb.Event {
	a.mu.Lock()
	a.batches = append(a.batches, plans)
	a.mu.Unlock()
	if a.started != nil {
		a.started <- plans
	}
	if a.records != nil {
		return a.records(ctx, plans)
	}
	return applied(plans)
}

func (a *fakeAdapter) ExecuteNative(ctx context.Context, plan *execution.Plan, emit execution.Emit) *pb.NativeEnd {
	if a.started != nil {
		plans := []*execution.Plan{plan}
		a.started <- plans
	}
	if a.native != nil {
		return a.native(ctx, plan, emit)
	}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	return end
}

func (a *fakeAdapter) Close(ctx context.Context) error {
	a.closes.Add(1)
	if a.closing != nil {
		a.closing(ctx)
	}
	return a.closeErr
}

func (a *fakeAdapter) executed() [][]string {
	a.mu.Lock()
	defer a.mu.Unlock()
	batches := make([][]string, len(a.batches))
	for i, plans := range a.batches {
		for _, plan := range plans {
			batches[i] = append(batches[i], plan.Key)
		}
	}
	return batches
}

func applied(plans []*execution.Plan) []*pb.Event {
	events := make([]*pb.Event, len(plans))
	for i, plan := range plans {
		if plan.Command.GetRead() != nil {
			value := &pb.Event_ReadResult{ReadResult: protocol.Missing()}
			events[i] = &pb.Event{Value: value}
			continue
		}
		events[i] = execution.FailedEvent(plan.Command, pb.MutationOutcome_APPLIED, nil)
	}
	return events
}

// blockUntil makes record executions wait for release or their batch context.
func blockUntil(release <-chan struct{}) func(context.Context, []*execution.Plan) []*pb.Event {
	return func(ctx context.Context, plans []*execution.Plan) []*pb.Event {
		select {
		case <-release:
			return applied(plans)
		case <-ctx.Done():
			events := make([]*pb.Event, len(plans))
			for i, plan := range plans {
				events[i] = execution.FailedEvent(plan.Command, pb.MutationOutcome_UNKNOWN, protocol.ContextFailure(ctx))
			}
			return events
		}
	}
}

func readPlan(resource string) *execution.Plan {
	read := &pb.ReadRequest{Resource: resource}
	value := &pb.Command_Read{Read: read}
	command := &pb.Command{Operation: value}
	plan := &execution.Plan{Command: command, Key: resource, BatchKey: "records", Bytes: proto.Size(command)}
	return plan
}

func mutationPlan(resource string) *execution.Plan {
	empty := &pb.Empty{}
	action := &pb.MutateRequest_Delete{Delete: empty}
	mutate := &pb.MutateRequest{Resource: resource, Action: action}
	value := &pb.Command_Mutate{Mutate: mutate}
	command := &pb.Command{Operation: value}
	plan := &execution.Plan{Command: command, Key: resource, BatchKey: "records", Bytes: proto.Size(command)}
	return plan
}

func nativePlan() *execution.Plan {
	request := &pb.NativeRequest{Resource: "db"}
	value := &pb.Command_Native{Native: request}
	command := &pb.Command{Operation: value}
	plan := &execution.Plan{Command: command, Key: "db", Bytes: proto.Size(command)}
	return plan
}

func open(t *testing.T, adapter *fakeAdapter, limits Limits) *Runtime {
	t.Helper()
	runtime, err := New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = runtime.Close(ctx)
	})
	return runtime
}

func limits(change func(*Limits)) Limits {
	result := DefaultLimits()
	result.BatchWait = 0
	if change != nil {
		change(&result)
	}
	return result
}

func submit(t *testing.T, runtime *Runtime, plan *execution.Plan, session *Session) *Ticket {
	t.Helper()
	ticket, failure := runtime.Submit(t.Context(), plan, session)
	if failure != nil {
		t.Fatal("submit failed", failure)
	}
	return ticket
}

// submitWithin submits a sessionless record that lives only as long as ctx.
func submitWithin(t *testing.T, ctx context.Context, runtime *Runtime, plan *execution.Plan) *Ticket {
	t.Helper()
	ticket, failure := runtime.Submit(ctx, plan, nil)
	if failure != nil {
		t.Fatal("submit failed", failure)
	}
	return ticket
}

func wait(t *testing.T, ticket *Ticket) *pb.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	event, err := ticket.Wait(ctx)
	if err != nil {
		t.Fatal("result not delivered", err)
	}
	return event
}

func started(t *testing.T, adapter *fakeAdapter) []string {
	t.Helper()
	select {
	case plans := <-adapter.started:
		keys := make([]string, len(plans))
		for i, plan := range plans {
			keys[i] = plan.Key
		}
		return keys
	case <-time.After(5 * time.Second):
		t.Fatal("execution did not start")
		return nil
	}
}

func notStarted(t *testing.T, adapter *fakeAdapter) {
	t.Helper()
	select {
	case plans := <-adapter.started:
		t.Fatal("unexpected execution", len(plans))
	case <-time.After(50 * time.Millisecond):
	}
}

func TestReadsAndMutationsBatchSeparately(t *testing.T) {
	adapter := &fakeAdapter{}
	runtime := open(t, adapter, limits(func(l *Limits) { l.BatchWait = 50 * time.Millisecond; l.MaxInFlight = 1 }))
	release := make(chan struct{})
	adapter.records = blockUntil(release)
	blocker := submit(t, runtime, mutationPlan("blocker"), nil)
	time.Sleep(80 * time.Millisecond)
	var tickets []*Ticket
	for _, key := range []string{"r1", "r2"} {
		tickets = append(tickets, submit(t, runtime, readPlan(key), nil))
	}
	for _, key := range []string{"m1", "m2"} {
		tickets = append(tickets, submit(t, runtime, mutationPlan(key), nil))
	}
	close(release)
	wait(t, blocker)
	for _, ticket := range tickets {
		wait(t, ticket)
	}
	want := [][]string{{"blocker"}, {"r1", "r2"}, {"m1", "m2"}}
	if got := adapter.executed(); !slices.EqualFunc(got, want, slices.Equal) {
		t.Fatal("reads and mutations were not grouped separately", got)
	}
}

func TestMutationsCollectUntilBatchWaitOrFull(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	runtime := open(t, adapter, limits(func(l *Limits) { l.BatchWait = 100 * time.Millisecond; l.BatchOperations = 3 }))
	begin := time.Now()
	first := submit(t, runtime, mutationPlan("a"), nil)
	second := submit(t, runtime, mutationPlan("b"), nil)
	if keys := started(t, adapter); !slices.Equal(keys, []string{"a", "b"}) || time.Since(begin) < 90*time.Millisecond {
		t.Fatal("collection window was not honored", keys, time.Since(begin))
	}
	wait(t, first)
	wait(t, second)

	begin = time.Now()
	for _, key := range []string{"c", "d", "e"} {
		submit(t, runtime, mutationPlan(key), nil)
	}
	if keys := started(t, adapter); len(keys) != 3 || time.Since(begin) > 80*time.Millisecond {
		t.Fatal("full batch waited for the collection window", keys, time.Since(begin))
	}
}

func TestReadsAndNativeDoNotWaitForCompanions(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	runtime := open(t, adapter, limits(func(l *Limits) { l.BatchWait = time.Hour }))
	read := submit(t, runtime, readPlan("r"), nil)
	if keys := started(t, adapter); !slices.Equal(keys, []string{"r"}) {
		t.Fatal("read waited for companions", keys)
	}
	wait(t, read)
	emit := func(*pb.Event) error { return nil }
	native, failure := runtime.SubmitNative(t.Context(), nativePlan(), emit)
	if failure != nil {
		t.Fatal(failure)
	}
	started(t, adapter)
	if event := wait(t, native); event.GetNativeEnd().GetCompletion() != pb.NativeCompletion_RESPONSE_COMPLETE {
		t.Fatal("Native result lost", event)
	}
}

func TestCallerDeadlineInsideCollectionWindowDispatchesImmediately(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	runtime := open(t, adapter, limits(func(l *Limits) { l.BatchWait = time.Hour }))
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	submitWithin(t, ctx, runtime, mutationPlan("a"))
	if keys := started(t, adapter); !slices.Equal(keys, []string{"a"}) {
		t.Fatal("short caller deadline waited for the collection window", keys)
	}
}

func TestDrainDispatchesCollectingMutations(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	runtime := open(t, adapter, limits(func(l *Limits) { l.BatchWait = time.Hour }))
	ticket := submit(t, runtime, mutationPlan("a"), nil)
	notStarted(t, adapter)
	runtime.BeginDrain()
	started(t, adapter)
	if event := wait(t, ticket); event.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("drained mutation lost its result", event)
	}
}

func TestBatchesRespectCountAndByteBounds(t *testing.T) {
	adapter := &fakeAdapter{}
	release := make(chan struct{})
	runtime := open(t, adapter, limits(func(l *Limits) { l.MaxInFlight = 1; l.BatchOperations = 2 }))
	adapter.records = blockUntil(release)
	blocker := submit(t, runtime, readPlan("blocker"), nil)
	time.Sleep(20 * time.Millisecond)
	var tickets []*Ticket
	for _, key := range []string{"a", "b", "c"} {
		tickets = append(tickets, submit(t, runtime, readPlan(key), nil))
	}
	close(release)
	wait(t, blocker)
	for _, ticket := range tickets {
		wait(t, ticket)
	}
	want := [][]string{{"blocker"}, {"a", "b"}, {"c"}}
	if got := adapter.executed(); !slices.EqualFunc(got, want, slices.Equal) {
		t.Fatal("count bound not applied", got)
	}

	adapter = &fakeAdapter{}
	release = make(chan struct{})
	size := readPlan("x").Bytes
	runtime = open(t, adapter, limits(func(l *Limits) { l.MaxInFlight = 1; l.BatchBytes = 2*size + 1 }))
	adapter.records = blockUntil(release)
	blocker = submit(t, runtime, readPlan("blocker"), nil)
	time.Sleep(20 * time.Millisecond)
	tickets = nil
	for _, key := range []string{"x", "y", "z"} {
		tickets = append(tickets, submit(t, runtime, readPlan(key), nil))
	}
	close(release)
	wait(t, blocker)
	for _, ticket := range tickets {
		wait(t, ticket)
	}
	want = [][]string{{"blocker"}, {"x", "y"}, {"z"}}
	if got := adapter.executed(); !slices.EqualFunc(got, want, slices.Equal) {
		t.Fatal("byte bound not applied", got)
	}
}

func TestBatchKeysSeparateGroups(t *testing.T) {
	adapter := &fakeAdapter{}
	release := make(chan struct{})
	runtime := open(t, adapter, limits(func(l *Limits) { l.MaxInFlight = 1 }))
	adapter.records = blockUntil(release)
	blocker := submit(t, runtime, readPlan("blocker"), nil)
	time.Sleep(20 * time.Millisecond)
	first, second := readPlan("a"), readPlan("b")
	second.BatchKey = "other"
	third := readPlan("c")
	tickets := []*Ticket{
		submit(t, runtime, first, nil),
		submit(t, runtime, second, nil),
		submit(t, runtime, third, nil),
	}
	close(release)
	wait(t, blocker)
	for _, ticket := range tickets {
		wait(t, ticket)
	}
	want := [][]string{{"blocker"}, {"a", "c"}, {"b"}}
	if got := adapter.executed(); !slices.EqualFunc(got, want, slices.Equal) {
		t.Fatal("batch keys were mixed or reordered", got)
	}
}

func TestSessionMutationsOfOneResourceRunInOrder(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release := make(chan struct{})
	runtime := open(t, adapter, limits(nil))
	adapter.records = blockUntil(release)
	session := runtime.NewSession()
	first := submit(t, runtime, mutationPlan("x"), session)
	if keys := started(t, adapter); !slices.Equal(keys, []string{"x"}) {
		t.Fatal(keys)
	}
	second := submit(t, runtime, mutationPlan("x"), session)
	other := submit(t, runtime, mutationPlan("y"), session)
	if keys := started(t, adapter); !slices.Equal(keys, []string{"y"}) {
		t.Fatal("same-resource mutation overtook its predecessor", keys)
	}
	notStarted(t, adapter)
	close(release)
	if keys := started(t, adapter); !slices.Equal(keys, []string{"x"}) {
		t.Fatal("ordered mutation did not follow its predecessor", keys)
	}
	for _, ticket := range []*Ticket{first, second, other} {
		wait(t, ticket)
	}
}

func TestSameResourceMutationsNeverShareABatch(t *testing.T) {
	adapter := &fakeAdapter{}
	release := make(chan struct{})
	runtime := open(t, adapter, limits(func(l *Limits) { l.MaxInFlight = 1 }))
	adapter.records = blockUntil(release)
	blocker := submit(t, runtime, mutationPlan("blocker"), nil)
	time.Sleep(20 * time.Millisecond)
	tickets := []*Ticket{
		submit(t, runtime, mutationPlan("x"), runtime.NewSession()),
		submit(t, runtime, mutationPlan("x"), runtime.NewSession()),
		submit(t, runtime, readPlan("r"), nil),
		submit(t, runtime, readPlan("r"), nil),
	}
	close(release)
	wait(t, blocker)
	for _, ticket := range tickets {
		wait(t, ticket)
	}
	want := [][]string{{"blocker"}, {"x"}, {"x"}, {"r", "r"}}
	if got := adapter.executed(); !slices.EqualFunc(got, want, slices.Equal) {
		t.Fatal("same-resource mutations shared a batch or reads were split", got)
	}
}

func TestCanceledQueuedMutationKeepsSessionOrder(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release := make(chan struct{})
	runtime := open(t, adapter, limits(nil))
	adapter.records = blockUntil(release)
	session := runtime.NewSession()
	first := submit(t, runtime, mutationPlan("x"), session)
	started(t, adapter)
	ctx, cancel := context.WithCancel(t.Context())
	second, failure := runtime.Submit(ctx, mutationPlan("x"), session)
	if failure != nil {
		t.Fatal(failure)
	}
	third := submit(t, runtime, mutationPlan("x"), session)
	cancel()
	if outcome := wait(t, second).GetMutationResult().GetOutcome(); outcome != pb.MutationOutcome_NOT_STARTED {
		t.Fatal("canceled queued mutation lost NOT_STARTED evidence", outcome)
	}
	notStarted(t, adapter)
	close(release)
	if keys := started(t, adapter); !slices.Equal(keys, []string{"x"}) {
		t.Fatal("later mutation did not follow the running one", keys)
	}
	wait(t, first)
	wait(t, third)
}

func TestFullBatchesKeepSessionOrder(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release := make(chan struct{})
	runtime := open(t, adapter, limits(func(l *Limits) { l.MaxInFlight = 1; l.BatchOperations = 2 }))
	adapter.records = blockUntil(release)
	blocker := submit(t, runtime, mutationPlan("blocker"), nil)
	started(t, adapter)
	session := runtime.NewSession()
	var tickets []*Ticket
	for _, key := range []string{"a", "b", "c", "a"} {
		tickets = append(tickets, submit(t, runtime, mutationPlan(key), session))
	}
	close(release)
	wait(t, blocker)
	for _, ticket := range tickets {
		wait(t, ticket)
	}
	want := [][]string{{"blocker"}, {"a", "b"}, {"c", "a"}}
	if got := adapter.executed(); !slices.EqualFunc(got, want, slices.Equal) {
		t.Fatal("full batches reordered session mutations", got)
	}
}

func TestReadyGroupsPassCollectingMutations(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	runtime := open(t, adapter, limits(func(l *Limits) { l.BatchWait = time.Hour; l.BatchOperations = 2 }))
	collecting := submit(t, runtime, mutationPlan("old"), nil)
	read := submit(t, runtime, readPlan("r"), nil)
	if keys := started(t, adapter); !slices.Equal(keys, []string{"r"}) {
		t.Fatal("ready read waited behind a collecting mutation", keys)
	}
	tickets := []*Ticket{collecting, read}
	for _, key := range []string{"a", "b"} {
		plan := mutationPlan(key)
		plan.BatchKey = "other"
		tickets = append(tickets, submit(t, runtime, plan, nil))
	}
	if keys := started(t, adapter); !slices.Equal(keys, []string{"a", "b"}) {
		t.Fatal("full batch waited behind a collecting mutation", keys)
	}
	notStarted(t, adapter)
	runtime.BeginDrain()
	if keys := started(t, adapter); !slices.Equal(keys, []string{"old"}) {
		t.Fatal("drain did not dispatch the collecting mutation", keys)
	}
	for _, ticket := range tickets {
		wait(t, ticket)
	}
}

func TestMaxInFlightBoundsConcurrentExecutions(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release := make(chan struct{})
	runtime := open(t, adapter, limits(func(l *Limits) { l.MaxInFlight = 1 }))
	adapter.records = blockUntil(release)
	first, second := readPlan("a"), readPlan("b")
	second.BatchKey = "other"
	firstTicket := submit(t, runtime, first, nil)
	started(t, adapter)
	secondTicket := submit(t, runtime, second, nil)
	notStarted(t, adapter)
	close(release)
	started(t, adapter)
	wait(t, firstTicket)
	wait(t, secondTicket)

	adapter = &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release = make(chan struct{})
	runtime = open(t, adapter, limits(func(l *Limits) { l.MaxInFlight = 0 }))
	adapter.records = blockUntil(release)
	for i := range 4 {
		plan := readPlan("a")
		plan.BatchKey = strings.Repeat("k", i+1)
		submit(t, runtime, plan, nil)
	}
	for range 4 {
		started(t, adapter)
	}
	close(release)
}

func TestSubmitWaitsForQueueSpace(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release := make(chan struct{})
	runtime := open(t, adapter, limits(func(l *Limits) { l.MaxInFlight = 1; l.QueueOperations = 1 }))
	adapter.records = blockUntil(release)
	running := submit(t, runtime, readPlan("a"), nil)
	started(t, adapter)
	queued := submit(t, runtime, readPlan("b"), nil)

	expired, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, failure := runtime.Submit(expired, readPlan("c"), nil); failure.GetCode() != pb.FailureCode_DEADLINE_EXCEEDED {
		t.Fatal("full queue did not hold the submitter until its deadline", failure)
	}

	admitted := make(chan *Ticket, 1)
	go func() {
		ticket, _ := runtime.Submit(t.Context(), readPlan("c"), nil)
		admitted <- ticket
	}()
	select {
	case <-admitted:
		t.Fatal("submitter passed a full queue")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case ticket := <-admitted:
		wait(t, ticket)
	case <-time.After(5 * time.Second):
		t.Fatal("submitter was not admitted after dispatch freed space")
	}
	wait(t, running)
	wait(t, queued)
}

func TestOversizedOperationWaitsForAnEmptyQueue(t *testing.T) {
	adapter := &fakeAdapter{}
	runtime := open(t, adapter, limits(func(l *Limits) { l.QueueBytes = 1; l.BatchBytes = 1 }))
	ticket := submit(t, runtime, readPlan("large"), nil)
	if event := wait(t, ticket); event.GetReadResult() == nil {
		t.Fatal("oversized operation lost its result", event)
	}
}

func TestOverloadAndDrainRejectSubmissions(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release := make(chan struct{})
	runtime := open(t, adapter, limits(func(l *Limits) { l.MaxInFlight = 1; l.QueueOperations = 1 }))
	adapter.records = blockUntil(release)
	submit(t, runtime, readPlan("a"), nil)
	started(t, adapter)
	submit(t, runtime, readPlan("b"), nil)
	blocked := make(chan *pb.Failure, 1)
	go func() {
		_, failure := runtime.Submit(t.Context(), readPlan("c"), nil)
		blocked <- failure
	}()
	time.Sleep(20 * time.Millisecond)
	runtime.SetOverloaded(true)
	if failure := <-blocked; failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("waiting submitter ignored overload", failure)
	}
	if _, failure := runtime.Submit(t.Context(), readPlan("d"), nil); failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("overloaded runtime admitted work", failure)
	}
	runtime.SetOverloaded(false)
	runtime.BeginDrain()
	if _, failure := runtime.Submit(t.Context(), readPlan("e"), nil); failure.GetCode() != pb.FailureCode_UNAVAILABLE {
		t.Fatal("draining runtime admitted work", failure)
	}
	close(release)
}

func TestRefusingRuntimeRefusesRecordsBeforeTheAdapterPreparesThem(t *testing.T) {
	record, err := execution.NewRecord("records", 1, readPlan("database/collection/s:a").Command)
	if err != nil {
		t.Fatal(err)
	}
	overloaded := open(t, &fakeAdapter{}, limits(nil))
	overloaded.SetOverloaded(true)
	if _, failure := overloaded.PrepareRecord(t.Context(), record); failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("overloaded runtime prepared a record", failure)
	}
	// A stream can outlive its Store's adapter during shutdown.
	closed := open(t, &fakeAdapter{}, limits(nil))
	if err := closed.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, failure := closed.PrepareRecord(t.Context(), record); failure.GetCode() != pb.FailureCode_UNAVAILABLE {
		t.Fatal("closed runtime prepared a record", failure)
	}
}

func TestQueuedCancellationCompletesNotStarted(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release := make(chan struct{})
	runtime := open(t, adapter, limits(func(l *Limits) { l.MaxInFlight = 1 }))
	adapter.records = blockUntil(release)
	running := submit(t, runtime, mutationPlan("a"), nil)
	started(t, adapter)
	ctx, cancel := context.WithCancel(t.Context())
	queued := submitWithin(t, ctx, runtime, mutationPlan("b"))
	cancel()
	event := wait(t, queued)
	result := event.GetMutationResult()
	if result.GetOutcome() != pb.MutationOutcome_NOT_STARTED || result.GetFailure().GetCode() != pb.FailureCode_CANCELLED {
		t.Fatal("canceled queued mutation lost NOT_STARTED evidence", event)
	}
	close(release)
	wait(t, running)
	notStarted(t, adapter)
}

func TestSharedBatchFollowsInterestedCallers(t *testing.T) {
	adapter := &fakeAdapter{}
	contexts := make(chan context.Context, 1)
	plans := make(chan []*execution.Plan, 1)
	runtime := open(t, adapter, limits(func(l *Limits) { l.BatchWait = 100 * time.Millisecond }))
	adapter.records = func(ctx context.Context, batch []*execution.Plan) []*pb.Event {
		contexts <- ctx
		plans <- batch
		<-ctx.Done()
		return applied(batch)
	}
	first, cancelFirst := context.WithCancel(t.Context())
	second, cancelSecond := context.WithCancel(t.Context())
	defer cancelSecond()
	bounded, cancelBounded := context.WithTimeout(t.Context(), time.Hour)
	defer cancelBounded()
	submitWithin(t, first, runtime, mutationPlan("a"))
	submitWithin(t, second, runtime, mutationPlan("b"))
	submitWithin(t, bounded, runtime, mutationPlan("c"))
	ctx := <-contexts
	batch := <-plans
	if _, ok := ctx.Deadline(); ok {
		t.Fatal("unbounded caller inherited another caller's deadline")
	}
	cancelFirst()
	if batch[0].Context.Err() == nil || ctx.Err() != nil {
		t.Fatal("one caller's cancellation did not stay with its own plan")
	}
	cancelSecond()
	cancelBounded()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("batch outlived every interested caller")
	}

	adapter.records = func(ctx context.Context, batch []*execution.Plan) []*pb.Event {
		contexts <- ctx
		return applied(batch)
	}
	early, cancelEarly := context.WithTimeout(t.Context(), time.Minute)
	defer cancelEarly()
	late, cancelLate := context.WithTimeout(t.Context(), time.Hour)
	defer cancelLate()
	submitWithin(t, early, runtime, mutationPlan("d"))
	submitWithin(t, late, runtime, mutationPlan("e"))
	ctx = <-contexts
	deadline, ok := ctx.Deadline()
	latest, _ := late.Deadline()
	if !ok || !deadline.Equal(latest) {
		t.Fatal("bounded batch did not use the latest caller deadline", deadline, ok)
	}
}

func TestPlanContextsDoNotMutateCallerPlans(t *testing.T) {
	adapter := &fakeAdapter{}
	runtime := open(t, adapter, limits(nil))
	plan := readPlan("a")
	wait(t, submit(t, runtime, plan, nil))
	if plan.Context != nil {
		t.Fatal("submit changed the caller's plan")
	}
}

func TestNativeStreamsThroughEmit(t *testing.T) {
	adapter := &fakeAdapter{}
	runtime := open(t, adapter, limits(nil))
	head := &pb.NativeHead{BodyContentType: "application/json"}
	headValue := &pb.Event_Head{Head: head}
	headEvent := &pb.Event{Value: headValue}
	chunkValue := &pb.Event_Chunk{Chunk: []byte("{}")}
	chunkEvent := &pb.Event{Value: chunkValue}
	var rejected error
	adapter.native = func(_ context.Context, _ *execution.Plan, emit execution.Emit) *pb.NativeEnd {
		_ = emit(headEvent)
		_ = emit(chunkEvent)
		end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
		endValue := &pb.Event_NativeEnd{NativeEnd: end}
		endEvent := &pb.Event{Value: endValue}
		rejected = emit(endEvent)
		return end
	}
	var received []*pb.Event
	emit := func(event *pb.Event) error {
		received = append(received, event)
		return nil
	}
	ticket, failure := runtime.SubmitNative(t.Context(), nativePlan(), emit)
	if failure != nil {
		t.Fatal(failure)
	}
	event := wait(t, ticket)
	if event.GetNativeEnd().GetCompletion() != pb.NativeCompletion_RESPONSE_COMPLETE || len(received) != 2 || received[0] != headEvent || received[1] != chunkEvent {
		t.Fatal("Native stream lost order or terminal evidence", event, received)
	}
	if !errors.Is(rejected, errInvalidNativeEvent) {
		t.Fatal("terminal event was accepted through emit", rejected)
	}
}

func TestNativesUseTheirOwnSlots(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release := make(chan struct{})
	runtime := open(t, adapter, limits(func(l *Limits) { l.MaxInFlight, l.MaxNative, l.QueueOperations = 1, 1, 1 }))
	adapter.native = func(context.Context, *execution.Plan, execution.Emit) *pb.NativeEnd {
		<-release
		end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
		return end
	}
	emit := func(*pb.Event) error { return nil }
	first, failure := runtime.SubmitNative(t.Context(), nativePlan(), emit)
	if failure != nil {
		t.Fatal(failure)
	}
	started(t, adapter)
	waiting := make(chan *Ticket, 2)
	for range 2 {
		go func() {
			ticket, _ := runtime.SubmitNative(t.Context(), nativePlan(), emit)
			waiting <- ticket
		}()
	}
	expired, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, failure := runtime.SubmitNative(expired, nativePlan(), emit); failure.GetCode() != pb.FailureCode_DEADLINE_EXCEEDED {
		t.Fatal("Native passed a full Native pool", failure)
	}
	// Natives waiting for a slot occupy neither the record queue nor a record slot.
	read := submit(t, runtime, readPlan("a"), nil)
	started(t, adapter)
	wait(t, read)
	if stats := runtime.Stats(); stats.Natives != 1 || stats.Active != 0 || stats.Queued != 0 {
		t.Fatal("Native slots were not counted separately", stats)
	}
	close(release)
	wait(t, first)
	for range 2 {
		select {
		case ticket := <-waiting:
			if ticket == nil {
				t.Fatal("waiting Native was refused")
			}
			wait(t, ticket)
		case <-time.After(5 * time.Second):
			t.Fatal("waiting Native did not get a free slot")
		}
	}
}

func TestInvalidAdapterResultsBecomeUnknown(t *testing.T) {
	adapter := &fakeAdapter{}
	runtime := open(t, adapter, limits(nil))
	adapter.records = func(context.Context, []*execution.Plan) []*pb.Event { return nil }
	event := wait(t, submit(t, runtime, mutationPlan("a"), nil))
	result := event.GetMutationResult()
	if result.GetOutcome() != pb.MutationOutcome_UNKNOWN || result.GetFailure().GetCode() != pb.FailureCode_INTERNAL {
		t.Fatal("missing adapter result was not reported as uncertain", event)
	}
	adapter.native = func(context.Context, *execution.Plan, execution.Emit) *pb.NativeEnd { return nil }
	emit := func(*pb.Event) error { return nil }
	ticket, failure := runtime.SubmitNative(t.Context(), nativePlan(), emit)
	if failure != nil {
		t.Fatal(failure)
	}
	if end := wait(t, ticket).GetNativeEnd(); end.GetCompletion() != pb.NativeCompletion_RESPONSE_INCOMPLETE || end.GetFailure().GetCode() != pb.FailureCode_INTERNAL {
		t.Fatal("missing NativeEnd was not reported as an incomplete internal failure", end)
	}
}

func TestCloseDrainsAcceptedWork(t *testing.T) {
	adapter := &fakeAdapter{}
	runtime, err := New(adapter, limits(func(l *Limits) { l.BatchWait = time.Hour }))
	if err != nil {
		t.Fatal(err)
	}
	ticket := submit(t, runtime, mutationPlan("a"), nil)
	if err := runtime.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if event := wait(t, ticket); event.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("drain lost accepted work", event)
	}
	if adapter.closes.Load() != 1 {
		t.Fatal("adapter was not closed exactly once", adapter.closes.Load())
	}
}

func TestCloseCancelsRemainingWorkAfterItsDeadline(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	runtime, err := New(adapter, limits(func(l *Limits) { l.MaxInFlight = 1; l.CleanupTimeout = time.Second }))
	if err != nil {
		t.Fatal(err)
	}
	adapter.records = blockUntil(make(chan struct{}))
	running := submit(t, runtime, mutationPlan("a"), nil)
	started(t, adapter)
	queued := submit(t, runtime, mutationPlan("b"), nil)
	expired, cancel := context.WithCancel(t.Context())
	cancel()
	if err := runtime.Close(expired); err != nil {
		t.Fatal(err)
	}
	if result := wait(t, running).GetMutationResult(); result.GetOutcome() != pb.MutationOutcome_UNKNOWN {
		t.Fatal("canceled execution lost its adapter evidence", result)
	}
	result := wait(t, queued).GetMutationResult()
	if result.GetOutcome() != pb.MutationOutcome_NOT_STARTED || result.GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE {
		t.Fatal("queued work was not reported as not started at shutdown", result)
	}
}

func TestCloseReportsWorkThatDoesNotRetire(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	release := make(chan struct{})
	defer close(release)
	runtime, err := New(adapter, limits(func(l *Limits) { l.CleanupTimeout = 200 * time.Millisecond }))
	if err != nil {
		t.Fatal(err)
	}
	adapter.records = func(_ context.Context, plans []*execution.Plan) []*pb.Event {
		<-release
		return applied(plans)
	}
	submit(t, runtime, readPlan("a"), nil)
	started(t, adapter)
	expired, cancel := context.WithCancel(t.Context())
	cancel()
	begin := time.Now()
	err = runtime.Close(expired)
	if err == nil || !strings.Contains(err.Error(), "executions_joined=false") || time.Since(begin) > 2*time.Second {
		t.Fatal("stuck execution was not reported within the cleanup budget", err, time.Since(begin))
	}
}

func TestCloseReportsNativeBlockedInEmit(t *testing.T) {
	adapter := &fakeAdapter{started: make(chan []*execution.Plan, 8)}
	runtime, err := New(adapter, limits(func(l *Limits) { l.CleanupTimeout = 200 * time.Millisecond }))
	if err != nil {
		t.Fatal(err)
	}
	adapter.native = func(ctx context.Context, _ *execution.Plan, emit execution.Emit) *pb.NativeEnd {
		value := &pb.Event_Chunk{Chunk: []byte("x")}
		event := &pb.Event{Value: value}
		if err := emit(event); err != nil {
			return protocol.NativeFailure(true, protocol.ContextFailure(ctx))
		}
		end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
		return end
	}
	// The caller's emit blocks until its RPC ends, as a stalled stream does.
	caller, cancelCaller := context.WithCancel(t.Context())
	defer cancelCaller()
	emit := func(*pb.Event) error {
		<-caller.Done()
		return caller.Err()
	}
	ticket, failure := runtime.SubmitNative(caller, nativePlan(), emit)
	if failure != nil {
		t.Fatal(failure)
	}
	started(t, adapter)
	expired, cancel := context.WithCancel(t.Context())
	cancel()
	begin := time.Now()
	err = runtime.Close(expired)
	if err == nil || !strings.Contains(err.Error(), "executions_joined=false") || time.Since(begin) > 2*time.Second {
		t.Fatal("Native blocked in emit was not reported within the cleanup budget", err, time.Since(begin))
	}
	cancelCaller()
	if end := wait(t, ticket).GetNativeEnd(); end.GetCompletion() != pb.NativeCompletion_RESPONSE_INCOMPLETE {
		t.Fatal("interrupted Native lost incomplete evidence", end)
	}
}

func TestConcurrentCloseSharesTheAdapterResult(t *testing.T) {
	failure := errors.New("adapter close failed")
	adapter := &fakeAdapter{closeErr: failure}
	runtime, err := New(adapter, limits(nil))
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		group.Go(func() { errs[i] = runtime.Close(t.Context()) })
	}
	group.Wait()
	for _, err := range errs {
		if !errors.Is(err, failure) {
			t.Fatal("close result was not shared", err)
		}
	}
	if adapter.closes.Load() != 1 {
		t.Fatal("adapter was closed more than once", adapter.closes.Load())
	}
}

func TestInvalidLimitsCloseTheAdapter(t *testing.T) {
	adapter := &fakeAdapter{}
	invalid := limits(func(l *Limits) { l.QueueOperations = 0 })
	if _, err := New(adapter, invalid); err == nil || adapter.closes.Load() != 1 {
		t.Fatal("invalid limits accepted or adapter leaked", err)
	}
}
