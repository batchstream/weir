package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type crossRequestCall struct {
	ctx   context.Context
	plans []*execution.Plan
}

type crossRequestAdapter struct {
	lifecycleAdapter
	calls chan crossRequestCall
	gate  <-chan struct{}
}

func (a *crossRequestAdapter) PrepareRecord(record *execution.Record) (*execution.Plan, *pb.Failure) {
	if record.Segments()[len(record.Segments())-1] == "invalid" {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "backend rejected last record")
	}
	operation := record.Operation()
	work := &execution.Plan{ID: operation.Index, Operation: operation, Key: record.Key(), BatchKey: record.Segments()[0], Bytes: record.Bytes(), WorkingBytes: 1024}
	return work, nil
}

func (a *crossRequestAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) execution.Feedback {
	call := crossRequestCall{ctx: ctx, plans: plans}
	a.calls <- call
	if a.gate != nil {
		select {
		case <-a.gate:
		case <-ctx.Done():
			// A missing acknowledgement for already dispatched mutations must
			// remain UNKNOWN, even when all callers have stopped waiting.
			return execution.Neutral
		}
	}
	for _, work := range plans {
		var result *execution.Result
		if work.Operation.Read != nil {
			var read *pb.ReadResult
			if work.Results.Reserve(len(work.Key)) {
				document := &pb.Document{MediaType: "application/json", Data: []byte(fmt.Sprintf("%q", work.Key))}
				read = protocol.ReadDocument(document)
			} else {
				read = protocol.ReadFailure(protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "result budget exhausted"))
			}
			result = &execution.Result{Index: work.ID, Read: read}
		} else {
			result = execution.FailedResult(work.Operation, pb.MutationOutcome_APPLIED, nil)
		}
		output := &execution.Output{Result: result}
		if err := emit(work, output); err != nil {
			return execution.Neutral
		}
	}
	return execution.Healthy
}

func prepareCrossReads(t testing.TB, runtime *Runtime, resources []string) *PreparedBatch {
	t.Helper()
	request := &pb.ReadBatchRequest{StoreName: "test"}
	for _, resource := range resources {
		read := &pb.ReadRequest{Resource: resource}
		request.Requests = append(request.Requests, read)
	}
	records, failure := execution.NewReadRecords(request, runtime.PendingByteLimit())
	if failure != nil {
		t.Fatal(failure)
	}
	prepared, failure := runtime.PrepareBatch(records)
	if failure != nil {
		t.Fatal(failure)
	}
	return prepared
}

func prepareCrossWrites(t testing.TB, runtime *Runtime, resources []string) *PreparedBatch {
	t.Helper()
	request := &pb.MutateBatchRequest{StoreName: "test"}
	for _, resource := range resources {
		empty := &pb.Empty{}
		action := &pb.MutateRequest_Delete{Delete: empty}
		mutation := &pb.MutateRequest{Resource: resource, Action: action}
		request.Requests = append(request.Requests, mutation)
	}
	records, failure := execution.NewMutationRecords(request, runtime.PendingByteLimit())
	if failure != nil {
		t.Fatal(failure)
	}
	prepared, failure := runtime.PrepareBatch(records)
	if failure != nil {
		t.Fatal(failure)
	}
	return prepared
}

func submitCrossBatch(t testing.TB, runtime *Runtime, ctx context.Context, prepared *PreparedBatch) *Ticket {
	t.Helper()
	ticket, failure, _ := runtime.SubmitBatch(ctx, prepared)
	if failure != nil {
		t.Fatal(failure)
	}
	return ticket
}

func selectCrossBatch(runtime *Runtime) *batch {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.selectLocked(time.Now())
}

func crossResults(t testing.TB, ticket *Ticket) []*execution.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	results, err := ticket.WaitBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return results
}

func TestIndependentRPCsShareExecutionAndKeepResultsAndQuota(t *testing.T) {
	adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 8)}
	runtime := newRuntime(adapter, DefaultLimits())
	var tickets []*Ticket
	var resources [][]string
	for i := range 35 {
		resources = append(resources, []string{fmt.Sprintf("records/s:%d", i)})
	}
	resources = append(resources, []string{"records/s:x", "records/s:y", "records/s:z"}, []string{"records/s:z", "records/s:x"})
	for _, input := range resources {
		prepared := prepareCrossReads(t, runtime, input)
		tickets = append(tickets, submitCrossBatch(t, runtime, t.Context(), prepared))
	}
	first := selectCrossBatch(runtime)
	if first == nil || len(first.items) != 32 {
		t.Fatal("independent single-record RPCs were not combined into one physical group", first)
	}
	if snapshot := runtime.Snapshot(); snapshot.Active != 1 || snapshot.WorkingBytes != 1024 || snapshot.Pending != 5 {
		t.Fatal("shared execution did not use one working envelope", snapshot)
	}
	runtime.execute(first)
	second := selectCrossBatch(runtime)
	if second == nil || len(second.items) != 5 {
		t.Fatal("compatible small batches were not combined with single records", second)
	}
	runtime.execute(second)
	for _, size := range []int{32, 8} {
		call := <-adapter.calls
		if len(call.plans) != size {
			t.Fatal("unexpected adapter batch size", len(call.plans), size)
		}
		for _, work := range call.plans {
			if work.Context == nil || work.Results == nil {
				t.Fatal("dispatch lost per-RPC context or result budget")
			}
		}
	}
	for i, ticket := range tickets {
		results := crossResults(t, ticket)
		if len(results) != len(resources[i]) {
			t.Fatal("cross-RPC result count changed", i, results)
		}
		for position, result := range results {
			if result.Index != uint64(position+1) || string(result.Read.GetDocument().GetData()) != fmt.Sprintf("%q", resources[i][position]) {
				t.Fatal("repeated RPC ordinals crossed result ownership", i, position, result)
			}
		}
		if i != 0 {
			ticket.Ack()
		}
	}
	if snapshot := runtime.Snapshot(); snapshot.Retained != 1 || snapshot.ResultBytes != execution.ResultOverheadBytes+len(resources[0][0]) || snapshot.PendingBytes != tickets[0].bulk.bytes || snapshot.Active != 0 || snapshot.WorkingBytes != 0 {
		t.Fatal("slow result owner retained another RPC's quota", snapshot)
	}
	tickets[0].Ack()
	waitReleased(t, runtime)
}

func TestCrossRPCSelectionPreservesNamespaceAndPhysicalBounds(t *testing.T) {
	cases := []struct {
		name        string
		count       int
		singleton   bool
		otherTarget bool
		byteBound   bool
		working     bool
	}{
		{name: "different target", otherTarget: true},
		{name: "singleton", singleton: true},
		{name: "native bytes", byteBound: true},
		{name: "working envelope", working: true},
		{name: "large RPC", count: 33},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			limits := DefaultLimits()
			adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 64)}
			runtime := newRuntime(adapter, limits)
			resources := []string{"records/s:first"}
			if test.count != 0 {
				resources = nil
				for i := range test.count {
					resources = append(resources, fmt.Sprintf("records/s:%d", i))
				}
			}
			prepared := prepareCrossReads(t, runtime, resources)
			prepared.plans[0].Singleton = test.singleton
			if test.byteBound {
				prepared.plans[0].Bytes = limits.BatchBytes
				prepared.bytes = limits.BatchBytes
			}
			first := submitCrossBatch(t, runtime, t.Context(), prepared)
			resource := "records/s:second"
			if test.otherTarget {
				resource = "other/s:second"
			}
			other := prepareCrossReads(t, runtime, []string{resource})
			if test.working {
				other.plans[0].WorkingBytes = limits.WorkingBytes
				other.workingBytes = limits.WorkingBytes
				runtime.workingBytes = 1024
			}
			second := submitCrossBatch(t, runtime, t.Context(), other)
			selected := selectCrossBatch(runtime)
			if selected == nil || len(selected.items) != 1 || selected.items[0] != first {
				t.Fatal("incompatible RPCs shared execution", selected)
			}
			runtime.execute(selected)
			if test.working {
				runtime.workingBytes = 0
			}
			runtime.execute(selectCrossBatch(runtime))
			crossResults(t, first)
			crossResults(t, second)
			first.Ack()
			second.Ack()
			waitReleased(t, runtime)
		})
	}
}

func TestCrossRPCDuplicateMutationsKeepInputOrder(t *testing.T) {
	adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 4)}
	runtime := newRuntime(adapter, DefaultLimits())
	prepared := prepareCrossWrites(t, runtime, []string{"records/s:same", "records/s:other", "records/s:same"})
	first := submitCrossBatch(t, runtime, t.Context(), prepared)
	peer := prepareCrossWrites(t, runtime, []string{"records/s:peer"})
	second := submitCrossBatch(t, runtime, t.Context(), peer)
	selected := selectCrossBatch(runtime)
	if len(selected.items) != 2 {
		t.Fatal("small duplicate-key RPC could not coalesce with its peer")
	}
	runtime.execute(selected)
	one, two := <-adapter.calls, <-adapter.calls
	if len(one.plans) != 2 || len(two.plans) != 2 || one.plans[0].ID != 1 || one.plans[1].ID != 2 || two.plans[0].ID != 3 || two.plans[1].ID != 1 {
		t.Fatal("duplicate mutation sequence or peer ordinal changed", one, two)
	}
	for _, ticket := range []*Ticket{first, second} {
		for _, result := range crossResults(t, ticket) {
			if result.Mutation.Outcome != pb.MutationOutcome_APPLIED {
				t.Fatal(result)
			}
		}
		ticket.Ack()
	}
	waitReleased(t, runtime)
}

func TestCrossRPCSharedContextUsesLatestCallerAndBackendCap(t *testing.T) {
	for _, unbounded := range []bool{false, true} {
		t.Run(fmt.Sprint(unbounded), func(t *testing.T) {
			adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 2)}
			limits := DefaultLimits()
			limits.BackendTimeout = time.Second
			runtime := newRuntime(adapter, limits)
			short, cancelShort := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancelShort()
			long, cancelLong := context.WithTimeout(t.Context(), 500*time.Millisecond)
			defer cancelLong()
			if unbounded {
				long = t.Context()
			}
			prepared := prepareCrossReads(t, runtime, []string{"records/s:same"})
			first := submitCrossBatch(t, runtime, short, prepared)
			second := submitCrossBatch(t, runtime, long, prepared)
			before := time.Now()
			selected := selectCrossBatch(runtime)
			deadline, _ := selected.ctx.Deadline()
			if unbounded {
				if !selected.timeoutOwned || deadline.Before(before.Add(900*time.Millisecond)) || deadline.After(time.Now().Add(time.Second)) {
					t.Fatal("unbounded peer lost bounded backend timeout", deadline)
				}
			} else if callerDeadline, _ := long.Deadline(); selected.timeoutOwned || !deadline.Equal(callerDeadline) {
				t.Fatal("shared deadline did not use latest caller", deadline, callerDeadline)
			}
			cancelShort()
			if selected.ctx.Err() != nil || second.ctx.Err() != nil {
				t.Fatal("short caller poisoned the peer")
			}
			runtime.execute(selected)
			call := <-adapter.calls
			if len(call.plans) != 1 || call.plans[0].Context != second.ctx {
				t.Fatal("canceled caller was sent to backend or surviving caller lost ownership")
			}
			if crossResults(t, first)[0].Read.GetFailure().GetCode() != pb.FailureCode_CANCELLED || crossResults(t, second)[0].Read.GetDocument() == nil {
				t.Fatal("cancellation changed surviving results")
			}
			first.Ack()
			second.Ack()
			waitReleased(t, runtime)
		})
	}
}

func TestCrossRPCBackendCapPrecedesLongCallerDeadline(t *testing.T) {
	adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 2)}
	limits := DefaultLimits()
	limits.BackendTimeout = 100 * time.Millisecond
	runtime := newRuntime(adapter, limits)
	caller, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	first := submitCrossBatch(t, runtime, caller, prepareCrossReads(t, runtime, []string{"records/s:first"}))
	second := submitCrossBatch(t, runtime, caller, prepareCrossReads(t, runtime, []string{"records/s:second"}))
	before := time.Now()
	selected := selectCrossBatch(runtime)
	deadline, ok := selected.ctx.Deadline()
	if !ok || !selected.timeoutOwned || deadline.Before(before.Add(limits.BackendTimeout)) || deadline.After(time.Now().Add(limits.BackendTimeout)) {
		t.Fatal("long callers replaced the configured execution cap", deadline, selected.timeoutOwned)
	}
	runtime.execute(selected)
	call := <-adapter.calls
	if len(call.plans) != 2 {
		t.Fatal("independent requests stopped sharing execution", len(call.plans))
	}
	for _, ticket := range []*Ticket{first, second} {
		results := crossResults(t, ticket)
		if len(results) != 1 || results[0].Read.GetFailure() != nil {
			t.Fatal("configured cap changed confirmed results", results)
		}
		ticket.Ack()
	}
	waitReleased(t, runtime)
}

func TestCrossRPCPreflightRejectsLateBackendInvalidRecordBeforeAdmission(t *testing.T) {
	adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 1)}
	runtime := newRuntime(adapter, DefaultLimits())
	one := &pb.ReadRequest{Resource: "records/valid"}
	last := &pb.ReadRequest{Resource: "records/invalid"}
	request := &pb.ReadBatchRequest{StoreName: "test", Requests: []*pb.ReadRequest{one, last}}
	records, failure := execution.NewReadRecords(request, runtime.PendingByteLimit())
	if failure != nil {
		t.Fatal(failure)
	}
	if prepared, failure := runtime.PrepareBatch(records); failure == nil || prepared != nil {
		t.Fatal("late backend-invalid record entered scheduler")
	}
	if snapshot := runtime.Snapshot(); snapshot.Pending != 0 || snapshot.Retained != 0 || snapshot.Active != 0 || snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 {
		t.Fatal("preflight rejection retained admission or backend work", snapshot)
	}
	select {
	case <-adapter.calls:
		t.Fatal("preflight rejection caused backend work")
	default:
	}
}

func TestCrossRPCActiveCancellationDoesNotCancelPeerOrEraseAcknowledgement(t *testing.T) {
	gate := make(chan struct{}, 1)
	adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 4), gate: gate}
	limits := DefaultLimits()
	limits.Concurrency = 1
	runtime, err := New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	defer runtime.Close(ctx)
	blocker := submitCrossBatch(t, runtime, ctx, prepareCrossReads(t, runtime, []string{"records/s:blocker"}))
	<-adapter.calls
	short, cancelShort := context.WithCancel(ctx)
	defer cancelShort()
	first := submitCrossBatch(t, runtime, short, prepareCrossWrites(t, runtime, []string{"records/s:first"}))
	second := submitCrossBatch(t, runtime, ctx, prepareCrossWrites(t, runtime, []string{"records/s:second"}))
	gate <- struct{}{}
	call := <-adapter.calls
	if len(call.plans) != 2 {
		t.Fatal("independent queued RPCs did not coalesce", len(call.plans))
	}
	cancelShort()
	if call.ctx.Err() != nil || second.ctx.Err() != nil {
		t.Fatal("active cancellation interrupted surviving caller")
	}
	if snapshot := runtime.Snapshot(); snapshot.Active != 1 || snapshot.WorkingBytes != 1024 || snapshot.Retained != 3 {
		t.Fatal("active cancellation returned working or retained ownership early", snapshot)
	}
	gate <- struct{}{}
	for _, ticket := range []*Ticket{first, second} {
		result := crossResults(t, ticket)[0]
		if result.Mutation.Outcome != pb.MutationOutcome_APPLIED || result.Mutation.Failure != nil {
			t.Fatal("proved backend acknowledgement was overwritten after caller cancellation", result)
		}
		ticket.Ack()
	}
	crossResults(t, blocker)
	blocker.Ack()
	waitReleased(t, runtime)
}

func TestCrossRPCAllCallersCancelSharedBackendAndKeepUnknownWrites(t *testing.T) {
	gate := make(chan struct{}, 1)
	adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 4), gate: gate}
	limits := DefaultLimits()
	limits.Concurrency = 1
	runtime, err := New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	defer runtime.Close(ctx)
	blocker := submitCrossBatch(t, runtime, ctx, prepareCrossReads(t, runtime, []string{"records/s:blocker"}))
	<-adapter.calls
	one, cancelOne := context.WithCancel(ctx)
	defer cancelOne()
	two, cancelTwo := context.WithCancel(ctx)
	defer cancelTwo()
	first := submitCrossBatch(t, runtime, one, prepareCrossWrites(t, runtime, []string{"records/s:first"}))
	second := submitCrossBatch(t, runtime, two, prepareCrossWrites(t, runtime, []string{"records/s:second"}))
	gate <- struct{}{}
	call := <-adapter.calls
	if len(call.plans) != 2 {
		t.Fatal("queued mutation RPCs did not coalesce")
	}
	cancelOne()
	if call.ctx.Err() != nil {
		t.Fatal("one canceled caller stopped shared execution")
	}
	cancelTwo()
	select {
	case <-call.ctx.Done():
	case <-ctx.Done():
		t.Fatal("all canceled callers left backend working")
	}
	for _, ticket := range []*Ticket{first, second} {
		result := crossResults(t, ticket)[0]
		if result.Mutation.Outcome != pb.MutationOutcome_UNKNOWN || result.Mutation.Failure.GetCode() != pb.FailureCode_CANCELLED {
			t.Fatal("missing dispatched acknowledgement became NOT_STARTED or APPLIED", result)
		}
		ticket.Ack()
	}
	crossResults(t, blocker)
	blocker.Ack()
	waitReleased(t, runtime)
}

func TestCrossRPCOwnBackendDeadlineAndAbandonedOwnersReleaseAfterExecution(t *testing.T) {
	gate := make(chan struct{})
	adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 2), gate: gate}
	limits := DefaultLimits()
	limits.BackendTimeout = 30 * time.Millisecond
	runtime := newRuntime(adapter, limits)
	first := submitCrossBatch(t, runtime, t.Context(), prepareCrossWrites(t, runtime, []string{"records/s:first"}))
	second := submitCrossBatch(t, runtime, t.Context(), prepareCrossWrites(t, runtime, []string{"records/s:second"}))
	selected := selectCrossBatch(runtime)
	done := make(chan struct{})
	go func() {
		runtime.execute(selected)
		close(done)
	}()
	<-adapter.calls
	first.Abandon()
	if snapshot := runtime.Snapshot(); snapshot.Retained != 2 || snapshot.Active != 1 || snapshot.WorkingBytes != 1024 {
		t.Fatal("abandon freed references still owned by adapter", snapshot)
	}
	<-done
	result := crossResults(t, second)[0]
	if result.Mutation.Outcome != pb.MutationOutcome_UNKNOWN || result.Mutation.Failure.GetCode() != pb.FailureCode_DEADLINE_EXCEEDED {
		t.Fatal("backend deadline lost unknown mutation evidence", result)
	}
	if snapshot := runtime.Snapshot(); snapshot.Retained != 1 || snapshot.WorkingBytes != 0 || snapshot.ResultBytes != execution.ResultOverheadBytes {
		t.Fatal("abandoned owner or shared working gate did not release", snapshot)
	}
	second.Ack()
	waitReleased(t, runtime)
}

func TestCrossRPCLargeDuplicateMutationBatchKeepsSequentialGroups(t *testing.T) {
	adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 40)}
	runtime := newRuntime(adapter, DefaultLimits())
	var resources []string
	for i := range 35 {
		key := fmt.Sprintf("records/s:%d", i)
		if i%8 == 0 {
			key = "records/s:duplicate"
		}
		resources = append(resources, key)
	}
	first := submitCrossBatch(t, runtime, t.Context(), prepareCrossWrites(t, runtime, resources))
	second := submitCrossBatch(t, runtime, t.Context(), prepareCrossWrites(t, runtime, []string{"records/s:peer"}))
	selected := selectCrossBatch(runtime)
	if len(selected.items) != 1 || selected.items[0] != first {
		t.Fatal("large client batch lost its sequential task")
	}
	runtime.execute(selected)
	ordinal := uint64(1)
	for ordinal <= uint64(len(resources)) {
		call := <-adapter.calls
		seen := make(map[string]bool)
		if len(call.plans) > runtime.limits.BatchOperations {
			t.Fatal("large client batch exceeded physical operation bound")
		}
		for _, work := range call.plans {
			if work.ID != ordinal || seen[work.Key] {
				t.Fatal("duplicate mutation key or input ordinal escaped sequential waves", work.ID, ordinal)
			}
			seen[work.Key] = true
			ordinal++
		}
	}
	runtime.execute(selectCrossBatch(runtime))
	for _, ticket := range []*Ticket{first, second} {
		for _, result := range crossResults(t, ticket) {
			if result.Mutation.Outcome != pb.MutationOutcome_APPLIED {
				t.Fatal(result)
			}
		}
		ticket.Ack()
	}
	waitReleased(t, runtime)
}

func TestCrossRPC512QueuedAndRetainedRequestsAreBoundedByBytes(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprint(canceled), func(t *testing.T) {
			adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 32)}
			runtime := newRuntime(adapter, DefaultLimits())
			prepared := prepareCrossReads(t, runtime, []string{"records/s:same"})
			if prepared.bytes < 1024 || prepared.resultBytes < execution.ResultOverheadBytes {
				t.Fatal("prepared RPC omitted retained metadata charges")
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var tickets []*Ticket
			for range 512 {
				tickets = append(tickets, submitCrossBatch(t, runtime, ctx, prepared))
			}
			if snapshot := runtime.Snapshot(); snapshot.Pending != 512 || snapshot.Retained != 512 || snapshot.PendingBytes != 512*prepared.bytes || snapshot.ResultBytes != 512*execution.ResultOverheadBytes {
				t.Fatal("queued requests or per-RPC metadata charges missing", snapshot)
			}
			if canceled {
				cancel()
				runtime.mu.Lock()
				runtime.cancelQueuedLocked()
				runtime.mu.Unlock()
			} else {
				for selected := selectCrossBatch(runtime); selected != nil; selected = selectCrossBatch(runtime) {
					runtime.execute(selected)
				}
			}
			for _, ticket := range tickets {
				result := crossResults(t, ticket)[0]
				if canceled && result.Read.GetFailure().GetCode() != pb.FailureCode_CANCELLED || !canceled && result.Read.GetDocument() == nil {
					t.Fatal("independent caller lost terminal result", result)
				}
			}
			if snapshot := runtime.Snapshot(); snapshot.Retained != 512 || snapshot.Ready != 512 || snapshot.PendingBytes != 512*prepared.bytes || snapshot.Active != 0 || snapshot.WorkingBytes != 0 {
				t.Fatal("completed RPCs released retained ownership before ACK", snapshot)
			}
			for _, ticket := range tickets {
				ticket.Ack()
			}
			waitReleased(t, runtime)
			if snapshot := runtime.Snapshot(); snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 {
				t.Fatal("512 ACK/cancel owners leaked byte reservations", snapshot)
			}
		})
	}
}

func TestCrossRPCByteAdmissionRemainsIndependentPerStore(t *testing.T) {
	for _, resultBound := range []bool{false, true} {
		t.Run(fmt.Sprint(resultBound), func(t *testing.T) {
			limits := DefaultLimits()
			adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 2)}
			construction := newRuntime(adapter, limits)
			var prepared *PreparedBatch
			if resultBound {
				count := (protocol.MaxDocument + execution.ResultOverheadBytes) / execution.ResultOverheadBytes
				resources := make([]string, count)
				for i := range resources {
					resources[i] = "records/s:one"
				}
				prepared = prepareCrossReads(t, construction, resources)
				limits.ResultBytes = prepared.resultBytes
			} else {
				data := []byte(`{"n":0}` + strings.Repeat(" ", 1<<20))
				document := &pb.Document{MediaType: "application/json", Data: data}
				request := &pb.MutateBatchRequest{StoreName: "test"}
				for i := range 10 {
					action := &pb.MutateRequest_Put{Put: document}
					mutation := &pb.MutateRequest{Resource: fmt.Sprintf("records/s:%d", i), Action: action}
					request.Requests = append(request.Requests, mutation)
				}
				records, failure := execution.NewMutationRecords(request, construction.PendingByteLimit())
				if failure != nil {
					t.Fatal(failure)
				}
				prepared, failure = construction.PrepareBatch(records)
				if failure != nil {
					t.Fatal(failure)
				}
				limits.PendingBytes = prepared.bytes
			}
			if err := limits.Validate(); err != nil {
				t.Fatal("test uses an illegal production memory limit", err)
			}
			runtime := newRuntime(adapter, limits)
			other := newRuntime(adapter, limits)
			first := submitCrossBatch(t, runtime, t.Context(), prepared)
			fresh := prepareCrossReads(t, runtime, []string{"records/s:two"})
			if _, failure, _ := runtime.SubmitBatch(t.Context(), fresh); failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
				t.Fatal("full byte budget admitted another RPC", failure)
			}
			peer := submitCrossBatch(t, other, t.Context(), fresh)
			first.Abandon()
			runtime.mu.Lock()
			runtime.cancelQueuedLocked()
			runtime.mu.Unlock()
			if snapshot := runtime.Snapshot(); snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 || snapshot.Retained != 0 {
				t.Fatal("queued cancellation did not return byte reservation", snapshot)
			}
			if snapshot := other.Snapshot(); snapshot.Pending != 1 || snapshot.Retained != 1 || snapshot.PendingBytes != fresh.bytes {
				t.Fatal("another Store's cancellation changed peer admission", snapshot)
			}
			replacement := submitCrossBatch(t, runtime, t.Context(), fresh)
			runtime.execute(selectCrossBatch(runtime))
			other.execute(selectCrossBatch(other))
			crossResults(t, replacement)
			crossResults(t, peer)
			replacement.Ack()
			peer.Ack()
			waitReleased(t, runtime)
			waitReleased(t, other)
		})
	}
}
