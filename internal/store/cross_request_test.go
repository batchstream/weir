package store

import (
	"context"
	"fmt"
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
	calls        chan crossRequestCall
	gate         <-chan struct{}
	maxReadBytes int
}

func (a *crossRequestAdapter) PrepareRecord(record *execution.Record) (*execution.Plan, *pb.Failure) {
	if record.Segments()[len(record.Segments())-1] == "invalid" {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "backend rejected resource")
	}
	bytes := execution.ResultOverheadBytes
	if record.Command().GetRead() != nil {
		bytes += max(len(fmt.Sprintf("%q", record.Key())), a.maxReadBytes)
	}
	work := &execution.Plan{ID: record.Index(), Command: record.Command(), Key: record.Key(), BatchKey: record.Segments()[0], Bytes: record.Bytes(), ResultBytes: bytes, WorkingBytes: 1024}
	return work, nil
}
func (a *crossRequestAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) bool {
	call := crossRequestCall{ctx: ctx, plans: plans}
	a.calls <- call
	if a.gate != nil {
		select {
		case <-a.gate:
		case <-ctx.Done():
			return false
		}
	}
	for _, work := range plans {
		var event *pb.Event
		if work.Command.GetRead() != nil {
			data := []byte(fmt.Sprintf("%q", work.Key))
			var read *pb.ReadResult
			if len(data) <= work.ResultBytes-execution.ResultOverheadBytes {
				document := &pb.Document{ContentType: "application/json", Data: data}
				read = protocol.ReadDocument(document)
			} else {
				read = protocol.ReadFailure(protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "result budget exhausted"))
			}
			event = &pb.Event{Value: &pb.Event_ReadResult{ReadResult: read}}
		} else {
			event = execution.FailedEvent(work.Command, pb.MutationOutcome_APPLIED, nil)
		}
		if err := emit(work, event); err != nil {
			return false
		}
	}
	return false
}
func prepareCrossRecord(t testing.TB, runtime *Runtime, index uint64, resource string) *execution.Plan {
	t.Helper()
	read := &pb.ReadRequest{Resource: resource}
	command := &pb.Command{Operation: &pb.Command_Read{Read: read}}
	record, err := execution.NewRecord("test", index, command)
	if err != nil {
		t.Fatal(err)
	}
	work, failure := runtime.PrepareRecord(record)
	if failure != nil {
		t.Fatal(failure)
	}
	return work
}
func selectCrossBatch(runtime *Runtime) *batch {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.selectLocked(time.Now())
}
func TestIndependentRPCsShareExecutionAndKeepResultsAndQuota(t *testing.T) {
	adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 8)}
	runtime := newRuntime(adapter, DefaultLimits())
	var tickets []*Ticket
	for i := range 35 {
		work := prepareCrossRecord(t, runtime, 1, fmt.Sprintf("records/s:%d", i))
		ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	first := selectCrossBatch(runtime)
	if first == nil || len(first.items) != 32 {
		t.Fatal("single tickets did not aggregate", first)
	}
	runtime.execute(first)
	second := selectCrossBatch(runtime)
	if second == nil || len(second.items) != 3 {
		t.Fatal("remaining tickets did not aggregate", second)
	}
	runtime.execute(second)
	for i, ticket := range tickets {
		document := recordEvent(t, ticket).GetReadResult().GetDocument()
		if string(document.GetData()) != fmt.Sprintf("%q", fmt.Sprintf("records/s:%d", i)) {
			t.Fatal("cross-client result association lost", i, document)
		}
	}
	before := runtime.Snapshot()
	if before.Retained != 35 || before.Ready != 35 || before.Publishers != 0 || before.WorkingBytes != 0 {
		t.Fatal("records retained publisher or lost result credits", before)
	}
	tickets[0].Ack()
	if after := runtime.Snapshot(); after.Retained != 34 || after.ResultBytes >= before.ResultBytes {
		t.Fatal("Ack released another client's ownership", after)
	}
	for _, ticket := range tickets[1:] {
		ticket.Ack()
	}
	if snapshot := runtime.Snapshot(); snapshot.Retained != 0 || snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 {
		t.Fatal("record credits leaked", snapshot)
	}
}
func TestReadAggregationAllowsSharedResourceWithinAndAcrossSessions(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(fmt.Sprint(shared), func(t *testing.T) {
			adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 2)}
			runtime := newRuntime(adapter, DefaultLimits())
			session := runtime.NewSession()
			var sessions []*Session
			for i := range 32 {
				current := session
				if !shared {
					current = runtime.NewSession()
				}
				sessions = append(sessions, current)
				work := prepareCrossRecord(t, runtime, uint64(i+1), "records/s:same")
				_, failure, _ := runtime.Submit(t.Context(), work, current)
				if failure != nil {
					t.Fatal(failure)
				}
			}
			selected := selectCrossBatch(runtime)
			if selected == nil || len(selected.items) != 32 {
				t.Fatal("identical reads were fragmented", selected)
			}
			runtime.execute(selected)
			for _, session := range sessions {
				session.Close()
			}
			if snapshot := runtime.Snapshot(); snapshot.Retained != 0 || snapshot.Publishers != 0 {
				t.Fatal("session retained record publishers", snapshot)
			}
		})
	}
}
func TestPhysicalBatchSeparatesResourceIfEitherMemberIsMutation(t *testing.T) {
	for _, firstRead := range []bool{false, true} {
		t.Run(fmt.Sprint(firstRead), func(t *testing.T) {
			runtime := newRuntime(nil, DefaultLimits())
			first := plan(1, "same", firstRead)
			second := plan(2, "same", !firstRead)
			firstTicket, failure, _ := runtime.Submit(t.Context(), first, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			secondTicket, failure, _ := runtime.Submit(t.Context(), second, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			selected := selectCrossBatch(runtime)
			if selected == nil || len(selected.items) != 1 || selected.items[0] != firstTicket {
				t.Fatal("mixed duplicate resource entered one exchange", selected)
			}
			runtime.mu.Lock()
			finish(runtime, selected)
			runtime.mu.Unlock()
			firstTicket.Ack()
			selected = selectCrossBatch(runtime)
			if selected == nil || len(selected.items) != 1 || selected.items[0] != secondTicket {
				t.Fatal("separated successor disappeared", selected)
			}
			runtime.mu.Lock()
			finish(runtime, selected)
			runtime.mu.Unlock()
			secondTicket.Ack()
		})
	}
}

func TestSharedRecordBudgetBlocksUntilPublicationAcknowledgement(t *testing.T) {
	adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 2), maxReadBytes: protocol.MaxDocument}
	limits := DefaultLimits()
	limits.ResultBytes = protocol.MaxDocument + execution.ResultOverheadBytes
	runtime := newRuntime(adapter, limits)
	first := prepareCrossRecord(t, runtime, 1, "records/s:first")
	second := prepareCrossRecord(t, runtime, 2, "records/s:second")
	ticket, failure, _ := runtime.Submit(t.Context(), first, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	runtime.execute(selectCrossBatch(runtime))
	if recordEvent(t, ticket).GetReadResult().GetDocument() == nil {
		t.Fatal("lost first document")
	}
	_, failure, changed := runtime.Submit(t.Context(), second, nil)
	if failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("completed owner released credits before Ack", failure)
	}
	ticket.Ack()
	select {
	case <-changed:
	default:
		t.Fatal("returned credit did not wake admission")
	}
	next, failure, _ := runtime.Submit(t.Context(), second, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	runtime.execute(selectCrossBatch(runtime))
	next.Ack()
	if snapshot := runtime.Snapshot(); snapshot.ResultBytes != 0 || snapshot.Retained != 0 {
		t.Fatal("result reservation leaked", snapshot)
	}
}
func TestPreparedRecordMetadataFloorAndOversizedBudget(t *testing.T) {
	adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 1)}
	runtime := newRuntime(adapter, DefaultLimits())
	work := prepareCrossRecord(t, runtime, 1, "records/s:a")
	if work.Bytes <= execution.EntryOverheadBytes {
		t.Fatal("record metadata floor was discounted", work.Bytes)
	}
	work.ResultBytes = runtime.limits.ResultBytes + 1
	_, failure, _ := runtime.Submit(t.Context(), work, nil)
	if failure.GetCode() != pb.FailureCode_INVALID_ARGUMENT || runtime.Snapshot().Retained != 0 {
		t.Fatal("oversized single record entered admission", failure)
	}
}
