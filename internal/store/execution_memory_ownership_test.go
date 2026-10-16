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
	"github.com/batchstream/weir/internal/overload"
	"google.golang.org/protobuf/proto"
)

func TestExecutionMemoryLegalDocumentAndWireFitSmallDefaultCapacity(t *testing.T) {
	for _, capacity := range []uint64{256, 512, 1024} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			memory := testExecutionMemory((capacity << 20) * 70 / 100)
			work := plan(1, "db/records/s:item", true)
			work.WorkingBytes, work.IOReserve = 4096, 48_000_000
			lease, failure := memory.admit(work)
			if failure != nil {
				t.Fatal(failure)
			}
			if started, waiting := lease.start(work.WorkingBytes+work.IOReserve, work.IOReserve); !started || waiting {
				t.Fatal("one legal read could not reserve negotiated IO", memory.Snapshot())
			}
			const document = 16 << 20
			if err := lease.reserveResult(document + execution.ResultOverheadBytes); err != nil {
				t.Fatal("a legal maximum document was excluded by a speculative per-record peak", err, memory.Snapshot())
			}
			if snapshot := memory.Snapshot(); snapshot.UsedBytes > snapshot.BudgetBytes || snapshot.ResidentBytes > snapshot.BudgetBytes-snapshot.IOReserveBytes {
				t.Fatal("document copy exceeded the shared bound", snapshot)
			}
			lease.finishWorking()
			wire, err := lease.reserveWire(document + 64)
			if err != nil {
				t.Fatal("adapter cleanup did not leave room for the actual encoded document", err)
			}
			before := memory.Snapshot()
			lease.release()
			after := memory.Snapshot()
			if after.Count != 1 || after.WireCount != 1 || after.WireBytes != document+64 || after.UsedBytes < after.WireBytes || after.UsedBytes >= before.UsedBytes {
				t.Fatal("Ack erased live wire or retained the original document", before, after)
			}
			wire.Release()
			wire.Release()
			if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.WireCount != 0 || snapshot.WireBytes != 0 || snapshot.UsedBytes != 0 || snapshot.InputUsedBytes != 0 || snapshot.InputReservedBytes != 0 {
				t.Fatal("last native buffer reference did not release exactly once", snapshot)
			}
		})
	}
}

func TestExecutionMemoryNativePublicationTransfersToWireWithoutDoubleCharge(t *testing.T) {
	memory := testExecutionMemory(768 << 20)
	work := plan(1, "native", false)
	work.Command = nativeCall()
	lease, failure := memory.admit(work)
	if failure != nil {
		t.Fatal(failure)
	}
	if started, _ := lease.start(48_000_000, defaultNativeIOReserve); !started {
		t.Fatal("IO did not begin")
	}
	if err := lease.settle(8<<20, protocol.NativeChunk+20); err != nil {
		t.Fatal(err)
	}
	before := memory.Snapshot()
	wire, err := lease.reserveWire(protocol.NativeChunk + 30)
	if err != nil {
		t.Fatal(err)
	}
	if after := memory.Snapshot(); after.UsedBytes != before.UsedBytes || after.WireCount != 1 {
		t.Fatal("Native prepaid publication was charged twice", before, after)
	}
	lease.release()
	sample := overload.Snapshot{RecoveryAllowanceKnown: true}
	memory.ObserveMemory(sample)
	if snapshot := memory.Snapshot(); snapshot.UsedBytes < snapshot.WireBytes || snapshot.WireBytes != protocol.NativeChunk+30 || snapshot.Count != 1 || snapshot.InputUsedBytes == 0 {
		t.Fatal("cancel or known-zero capacity erased transport ownership", snapshot)
	}
	if _, failure := memory.admit(work); failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("known-zero share admitted new memory", failure)
	}
	wire.Release()
	if snapshot := memory.Snapshot(); snapshot.UsedBytes != 0 || snapshot.WireBytes != 0 || snapshot.Count != 0 || snapshot.InputUsedBytes != 0 {
		t.Fatal(snapshot)
	}
}

func TestExecutionMemoryFixedTerminalFitsActualMinimumWireAtFullBudget(t *testing.T) {
	memory := testExecutionMemory(768 << 20)
	work := plan(1, "queued", true)
	lease, failure := memory.admit(work)
	if failure != nil {
		t.Fatal(failure)
	}
	memory.mu.Lock()
	memory.budget = memory.used
	memory.mu.Unlock()
	if err := lease.reserveResult(execution.ResultOverheadBytes + 200); err != nil {
		t.Fatal("a small confirmed result required extra capacity", err)
	}
	wire, err := lease.reserveWire(execution.MinimumWireBytes)
	if err != nil {
		t.Fatal("1025-byte actual wire did not fit a pre-guaranteed terminal", err)
	}
	lease.release()
	if snapshot := memory.Snapshot(); snapshot.WireBytes != execution.MinimumWireBytes || snapshot.UsedBytes > snapshot.BudgetBytes {
		t.Fatal(snapshot)
	}
	wire.Release()
	if snapshot := memory.Snapshot(); snapshot.UsedBytes != 0 || snapshot.Count != 0 {
		t.Fatal(snapshot)
	}
}

type sharedTypedMemoryAdapter struct {
	lifecycleAdapter
	batchSize int
	document  int
	capacity  int
}

func TestExecutionMemoryFullResidentAndSlowBorrowedWireLeaveShortIOProgress(t *testing.T) {
	for _, padding := range []int{1024, 8192} {
		t.Run(fmt.Sprint(padding), func(t *testing.T) {
			memory := testExecutionMemory((256 << 20) * 70 / 100)
			held := plan(1, "held", false)
			held.Command = nativeCall()
			lease, failure := memory.admit(held)
			if failure != nil {
				t.Fatal(failure)
			}
			publication := lease.publication - nativeQueuedWindow
			working := int(memory.residentLimitLocked()-memory.inputReserved) - publication
			if started, _ := lease.start(working, defaultNativeIOReserve); !started {
				t.Fatal("held response did not start")
			}
			if err := lease.settle(working, 0); err != nil {
				t.Fatal(err)
			}
			if snapshot := memory.Snapshot(); snapshot.ResidentBytes != snapshot.BudgetBytes-snapshot.IOReserveBytes {
				t.Fatal("resident output did not exactly fill its share", snapshot)
			}
			document := `{"id":"item","n":7,"pad":"` + strings.Repeat("p", padding) + `"}`
			typedAdapter := &sharedTypedMemoryAdapter{document: len(document)}
			limits := DefaultLimits()
			typed, err := New(typedAdapter, limits, memory)
			if err != nil {
				t.Fatal(err)
			}
			gate := make(chan struct{})
			close(gate)
			nativeAdapter := &nativeAdmissionBurstAdapter{entered: make(chan struct{}, 1), release: gate}
			native, err := New(nativeAdapter, limits, memory)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			t.Cleanup(func() {
				cancel()
				lease.release()
				_ = typed.Close(context.Background())
				_ = native.Close(context.Background())
			})
			work := plan(2, "healthy", true)
			work.WorkingBytes, work.IOReserve = 2048, 48_000_000
			ticket, failure, _ := typed.Submit(ctx, work, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			event, err := ticket.Wait(ctx)
			if err != nil || len(event.GetReadResult().GetDocument().GetData()) != len(document) {
				t.Fatal("full resident share rejected an ordinary small document with metadata", event, err, memory.Snapshot())
			}
			response := &pb.ExecuteResponse{Index: 1, Event: event}
			wire, err := ticket.ReserveWire(max(proto.Size(response), execution.MinimumWireBytes))
			if err != nil {
				t.Fatal(err)
			}
			defer wire.Release()
			ticket.Ack()
			// Keep the encoded output genuinely owned while a different Store
			// attempts the maximum short-input boundary plus one negotiated M.
			session := native.NewSession()
			defer session.Close()
			short := plan(3, "short", false)
			short.Command = nativeCall()
			short.Command.GetNative().Request.Data = make([]byte, protocol.NativeChunk)
			short.Bytes = protocol.NativeChunk + execution.EntryOverheadBytes
			short.WorkingBytes = 48_000_000 + 2*protocol.NativeChunk + 8192
			short.IOReserve = defaultNativeIOReserve
			nativeTicket, failure, _ := native.Submit(ctx, short, session)
			if failure != nil {
				t.Fatal("borrowed wire prevented a fresh bounded Native input", failure, memory.Snapshot())
			}
			complete := false
			for {
				select {
				case emission := <-session.Events:
					event := emission.Event
					emission.Release()
					if event.GetNativeEnd() != nil {
						complete = event.GetNativeEnd().Completion == pb.NativeCompletion_RESPONSE_COMPLETE
					}
					if event == nil {
						if !complete {
							t.Fatal("short Native lost successful completion under slow wire pressure")
						}
						nativeTicket.Ack()
						goto completed
					}
				case <-ctx.Done():
					t.Fatal("full resident/borrowed wire occupied the protected short-query workspace", memory.Snapshot())
				}
			}
		completed:
			if snapshot := memory.Snapshot(); snapshot.WireCount != 1 || snapshot.UsedBytes > snapshot.BudgetBytes || snapshot.BudgetRejections != 0 {
				t.Fatal("healthy probes required fake wire release or unbounded memory", snapshot)
			}
			wire.Release()
			lease.release()
			waitReleased(t, native)
			if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 || snapshot.WireBytes != 0 {
				t.Fatal(snapshot)
			}
		})
	}
}

func (a *sharedTypedMemoryAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) {
	a.batchSize = len(plans)
	for _, work := range plans {
		capacity := max(a.document, a.capacity)
		if err := work.Memory.ReserveResult(capacity + execution.ResultOverheadBytes); err != nil {
			failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, execution.ErrExecutionMemory.Error())
			event := execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_STARTED, failure)
			_ = emit(work, event)
			continue
		}
		data := make([]byte, capacity)[:a.document]
		document := &pb.Document{ContentType: "application/json", Data: data}
		read := &pb.ReadResult{Result: &pb.ReadResult_Document{Document: document}}
		event := &pb.Event{Value: &pb.Event_ReadResult{ReadResult: read}}
		_ = emit(work, event)
	}
	_ = plans[0].Memory.SetWorking(0)
}

func TestExecutionMemoryReadyResultRetainsActualCapacityBeyondEncodedLength(t *testing.T) {
	memory := testExecutionMemory(768 << 20)
	adapter := &sharedTypedMemoryAdapter{document: 1024, capacity: 8192}
	limits := DefaultLimits()
	runtime, err := New(adapter, limits, memory)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	work := plan(1, "capacity", true)
	ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	event, err := ticket.Wait(t.Context())
	if err != nil || cap(event.GetReadResult().GetDocument().GetData()) != adapter.capacity {
		t.Fatal(event, err)
	}
	snapshot := memory.Snapshot()
	if snapshot.UsedBytes < snapshot.InputReservedBytes+uint64(adapter.capacity-execution.ResultOverheadBytes) {
		t.Fatal("ready result shrank owned backing capacity to its encoded length", snapshot)
	}
	ticket.Ack()
	if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 {
		t.Fatal(snapshot)
	}
}

func TestExecutionMemoryOneDriverMessagePreserves128SmallReadBatch(t *testing.T) {
	memory := testExecutionMemory((256 << 20) * 70 / 100)
	limits := DefaultLimits()
	adapter := &sharedTypedMemoryAdapter{document: 1024}
	runtime := newRuntime(adapter, limits, memory)
	defer memory.unregister(runtime.wake)
	var tickets []*Ticket
	for index := range limits.BatchOperations {
		work := plan(uint64(index), fmt.Sprint(index), true)
		work.WorkingBytes, work.IOReserve = 2048, 48_000_000
		ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	runtime.mu.Lock()
	batch, _ := runtime.selectLocked(time.Now())
	runtime.mu.Unlock()
	if batch == nil || len(batch.items) != 128 || batch.workingBytes != 48_000_000+128*2048 {
		t.Fatal("driver message credit was multiplied per record", batch)
	}
	runtime.execute(batch)
	if adapter.batchSize != 128 {
		t.Fatal("small reads lost the existing batch efficiency", adapter.batchSize)
	}
	for _, ticket := range tickets {
		event, err := ticket.Wait(t.Context())
		if err != nil || len(event.GetReadResult().GetDocument().GetData()) != 1024 {
			t.Fatal("small read lost its successful result", event, err)
		}
		ticket.Ack()
	}
	if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 {
		t.Fatal(snapshot)
	}
}
