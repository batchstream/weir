package store

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
	"google.golang.org/protobuf/proto"
)

// The original independent 500 MB proof remains byte-identical in the evidence
// directory. This fixture uses its same legal document sizes and ledger calls;
// the corrected contract rejects the large reply before it can consume R.
func borrowBoundFixture(t testing.TB) (*ExecutionMemory, *executionLease) {
	t.Helper()
	memory := testExecutionMemory(750_000_000)
	t.Cleanup(func() {
		if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 || snapshot.ResidentBytes != 0 || snapshot.WireBytes != 0 || snapshot.Borrowed != 0 || snapshot.InputUsedBytes != 0 || snapshot.InputReservedBytes != 0 || memory.io != 0 {
			t.Error("final cleanup retained an actual owner", snapshot)
		}
	})
	work := plan(1, "large-native", false)
	work.Command = nativeCall()
	large, failure, _ := memory.admit(work)
	if failure != nil {
		t.Fatal(failure)
	}
	t.Cleanup(large.release)
	if started, waiting := large.start(48_008_256, defaultNativeIOReserve); !started || waiting {
		t.Fatal("initial Native peak did not fit", memory.Snapshot())
	}
	for index := range 13 {
		work := plan(uint64(index+2), "retained-typed", true)
		lease, failure, _ := memory.admit(work)
		if failure != nil {
			t.Fatal(failure)
		}
		t.Cleanup(lease.release)
		if started, waiting := lease.start(0, 0); !started || waiting {
			t.Fatal("retained result could not start")
		}
		capacity := 16 << 20
		if index == 12 {
			capacity = 7 << 20
		}
		if err := lease.reserveResult(capacity + execution.ResultOverheadBytes); err != nil {
			t.Fatal(err, memory.Snapshot())
		}
		lease.finishWorking()
	}
	return memory, large
}

func TestNativeLargeOutputCannotBorrowProtectedProgressMemory(t *testing.T) {
	memory, large := borrowBoundFixture(t)
	before := memory.Snapshot()
	if err := large.settle((12<<20)+execution.EntryOverheadBytes, protocol.NativeChunk+20); err != execution.ErrExecutionMemory {
		t.Fatal("large paused Native reply consumed the protected IO reserve", err, before, memory.Snapshot())
	}
	after := memory.Snapshot()
	before.BudgetRejections++
	if after != before || !large.preparing || large.working != 48_008_256 {
		t.Fatal("failed settlement changed actual ownership before adapter cleanup", before, after)
	}
	if err := large.terminal(128); err != nil {
		t.Fatal("cleaned exchange could not use its prepaid incomplete terminal", err)
	}
	shortPlan := plan(100, "healthy-short-native", false)
	shortPlan.Command = nativeCall()
	short, failure, _ := memory.admit(shortPlan)
	if failure != nil {
		t.Fatal(failure)
	}
	defer short.release()
	if started, waiting := short.start(48_008_448, defaultNativeIOReserve); !started || waiting {
		t.Fatal("healthy Native could not use R after the rejected reply was cleaned", memory.Snapshot())
	}
	if err := short.settle(1024, 20); err != nil {
		t.Fatal("short reply could not settle", err)
	}
	if err := short.terminal(128); err != nil {
		t.Fatal(err)
	}
	t.Logf("same500MB budget: rejected large reply unchanged=%+v cleaned=%+v", after, memory.Snapshot())
}

func fullResidentBorrowMemory(t testing.TB) *ExecutionMemory {
	t.Helper()
	memory, large := borrowBoundFixture(t)
	remaining := memory.residentLimitLocked() - memory.Snapshot().ResidentBytes
	if remaining%2 != 1 {
		t.Fatal("fixture precondition: no exact legal result boundary", remaining)
	}
	work := plan(50, "resident-tail", true)
	lease, failure, _ := memory.admit(work)
	if failure != nil {
		t.Fatal(failure)
	}
	t.Cleanup(lease.release)
	if err := lease.reserveResult(int(remaining+1537) / 2); err != nil {
		t.Fatal(err)
	}
	if err := large.terminal(128); err != nil {
		t.Fatal(err)
	}
	large.release()
	if snapshot := memory.Snapshot(); snapshot.ResidentBytes != snapshot.BudgetBytes-snapshot.IOReserveBytes || snapshot.UsedBytes != snapshot.ResidentBytes || snapshot.Borrowed != 0 {
		t.Fatal("fixture did not retain exactly the resident share", snapshot)
	}
	return memory
}

func TestSharedShortHandoffsPreserveBothOrdersAndRejectThird(t *testing.T) {
	for _, nativeFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(nativeFirst), func(t *testing.T) {
			memory := fullResidentBorrowMemory(t)
			var typed, native *executionLease
			var typedWire *WireReservation
			for index := range 2 {
				isNative := index == 1
				if nativeFirst {
					isNative = !isNative
				}
				work := plan(uint64(100+index), "short", true)
				if isNative {
					work.Command = nativeCall()
					work.ResultBytes = execution.ResultOverheadBytes
				}
				lease, failure, _ := memory.admit(work)
				if failure != nil {
					t.Fatal(failure)
				}
				t.Cleanup(lease.release)
				// A short input can reach one complete NativeChunk. The protected
				// initial peak includes two real request copies, M and publication.
				working := 48_000_000 + 2*protocol.NativeChunk + 8192
				reserve := 48_000_000
				if isNative {
					reserve = defaultNativeIOReserve
				}
				if started, waiting := lease.start(working, reserve); !started || waiting {
					t.Fatal("held short output blocked the next protected IO", memory.Snapshot())
				}
				if isNative {
					native = lease
					if err := lease.settle(protocol.NativeChunk+execution.EntryOverheadBytes, protocol.NativeChunk+20); err != nil {
						t.Fatal("one full short Native chunk exceeded its handoff", err)
					}
				} else {
					typed = lease
					if err := lease.reserveResult(protocol.NativeChunk + execution.ResultOverheadBytes); err != nil {
						t.Fatal("short typed result could not coexist with short Native", err)
					}
					lease.finishWorking()
					var err error
					typedWire, err = lease.reserveWire(protocol.NativeChunk + 64)
					if err != nil {
						t.Fatal(err)
					}
					defer typedWire.Release()
					lease.release()
				}
			}
			if snapshot := memory.Snapshot(); snapshot.Borrowed != 2 || snapshot.UsedBytes > snapshot.BudgetBytes || snapshot.WireCount != 1 || typed.wire == 0 {
				t.Fatal("both handoffs were not actually held", snapshot)
			}
			work := plan(200, "third", true)
			third, failure, _ := memory.admit(work)
			if failure != nil {
				t.Fatal(failure)
			}
			defer third.release()
			before := memory.Snapshot()
			if err := third.reserveResult(4096); err != execution.ErrExecutionMemory {
				t.Fatal("third slow short result bypassed the finite handoff count", err)
			}
			before.BudgetRejections++
			if snapshot := memory.Snapshot(); snapshot != before {
				t.Fatal("third handoff failure changed another owner's bytes", before, snapshot)
			}
			if err := native.terminal(128); err != nil {
				t.Fatal("Native completion needed another handoff slot", err)
			}
			native.release()
			if snapshot := memory.Snapshot(); snapshot.Borrowed != 1 || snapshot.WireCount != 1 {
				t.Fatal("Native completion erased the slow typed wire", snapshot)
			}
			typedWire.Release()
			if snapshot := memory.Snapshot(); snapshot.Borrowed != 0 || snapshot.WireCount != 0 {
				t.Fatal("real wire retirement retained its handoff", snapshot)
			}
		})
	}
}

func TestNativeTerminalRetainsCanceledWireHandoffUntilRealFree(t *testing.T) {
	memory := fullResidentBorrowMemory(t)
	work := plan(100, "short-native", false)
	work.Command = nativeCall()
	lease, failure, _ := memory.admit(work)
	if failure != nil {
		t.Fatal(failure)
	}
	defer lease.release()
	if started, _ := lease.start(48_008_256, defaultNativeIOReserve); !started {
		t.Fatal("Native did not start")
	}
	if err := lease.settle(protocol.NativeChunk+execution.EntryOverheadBytes, protocol.NativeChunk+20); err != nil {
		t.Fatal(err)
	}
	wire, err := lease.reserveWire(protocol.NativeChunk + 20)
	if err != nil {
		t.Fatal(err)
	}
	defer wire.Release()
	if err := lease.terminal(128); err != nil {
		t.Fatal(err)
	}
	before := memory.Snapshot()
	if before.Borrowed != 1 || before.WireCount != 1 || before.WireBytes != protocol.NativeChunk+20 || lease.window != lease.wire+2*execution.ResultOverheadBytes {
		t.Fatal("terminal hid a nonresident wire or retained unused publication chunks", before)
	}
	if _, err := lease.reserveWire(execution.MinimumWireBytes); err != execution.ErrExecutionMemory {
		t.Fatal("same invocation allowed overlapping encoded owners", err)
	}
	if snapshot := memory.Snapshot(); snapshot != before {
		t.Fatal("rejected overlapping wire changed ownership", before, snapshot)
	}
	lease.release()
	if snapshot := memory.Snapshot(); snapshot.Borrowed != 1 || snapshot.WireCount != 1 || snapshot.NativeCount != 1 || snapshot.UsedBytes < snapshot.WireBytes {
		t.Fatal("cancel/Ack pretended to free transport-owned memory", snapshot)
	}
	wire.Release()
	wire.Release()
	if snapshot := memory.Snapshot(); snapshot.Borrowed != 0 || snapshot.WireCount != 0 || snapshot.NativeCount != 0 {
		t.Fatal("Free or ConnEnd retirement did not finish the owner exactly once", snapshot)
	}
}

func TestNativeFixedTerminalPreservesEvidenceWithBothHandoffsFull(t *testing.T) {
	for _, attempted := range []bool{false, true} {
		t.Run(fmt.Sprint(attempted), func(t *testing.T) {
			memory := fullResidentBorrowMemory(t)
			for index := range executionBorrowLimit {
				work := plan(uint64(100+index), "slow", true)
				lease, failure, _ := memory.admit(work)
				if failure != nil {
					t.Fatal(failure)
				}
				t.Cleanup(lease.release)
				if err := lease.reserveResult(4096); err != nil {
					t.Fatal(err)
				}
			}
			failure := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid Native input")
			if attempted {
				failure = protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, execution.ErrExecutionMemory.Error())
			}
			end := protocol.NativeFailure(attempted, failure)
			value := &pb.Event_NativeEnd{NativeEnd: end}
			event := &pb.Event{Value: value}
			adapter := &nativeAccountingAdapter{events: []*pb.Event{event}}
			limits := DefaultLimits()
			runtime, err := New(adapter, limits, memory)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close(context.Background())
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			session := runtime.NewSession()
			defer session.Close()
			work := plan(200, "terminal", false)
			work.Command = nativeCall()
			ticket, failure, _ := runtime.Submit(ctx, work, session)
			if failure != nil {
				t.Fatal(failure)
			}
			select {
			case emission := <-session.Events:
				if !proto.Equal(emission.Event.GetNativeEnd(), end) {
					t.Fatal("prepaid fixed terminal changed attempt/invalid-input evidence", emission.Event)
				}
				wire, err := ticket.ReserveWire(execution.MinimumWireBytes)
				if err != nil {
					t.Fatal("actual minimum1025-byte codec buffer required another handoff", err)
				}
				emission.Release()
				ticket.Ack()
				wire.Release()
			case <-ctx.Done():
				t.Fatal("two slow outputs blocked an already prepaid terminal", memory.Snapshot())
			}
			waitReleased(t, runtime)
			if snapshot := memory.Snapshot(); snapshot.Borrowed != 2 || snapshot.WireCount != 0 || snapshot.BudgetRejections != 0 {
				t.Fatal("terminal used another handoff or lost existing owners", snapshot)
			}
		})
	}
}

func TestNativeResidentCreditSurvivesShrinkWithoutBorrowingLargeReply(t *testing.T) {
	memory := testExecutionMemory(750_000_000)
	work := plan(1, "resident-large", false)
	work.Command = nativeCall()
	lease, failure, _ := memory.admit(work)
	if failure != nil {
		t.Fatal(failure)
	}
	defer lease.release()
	if started, waiting := lease.start(defaultNativeIOReserve, defaultNativeIOReserve); !started || waiting {
		t.Fatal("initial IO could not start")
	}
	const held = 12 << 20
	if err := lease.settle(held, protocol.NativeChunk); err != nil {
		t.Fatal("large Native output with real resident credit was rejected", err)
	}
	before := memory.Snapshot()
	sample := overload.Snapshot{RecoveryAllowance: 80 << 20, RecoveryAllowanceKnown: true}
	memory.ObserveMemory(sample)
	shrunk := memory.Snapshot()
	if err := lease.settle(held, protocol.NativeChunk); err != nil {
		t.Fatal("capacity shrink discarded an already-owned resident reply", err)
	}
	if after := memory.Snapshot(); after != shrunk || after.Borrowed != 0 || after.ResidentBytes >= before.ResidentBytes {
		t.Fatal("repeated settlement fabricated or dropped resident credit", before, shrunk, after)
	}
	lease.release()
	if snapshot := memory.Snapshot(); snapshot.UsedBytes != 0 || snapshot.Count != 0 || snapshot.ResidentBytes != 0 {
		t.Fatal("resident reply cleanup did not release ownership", snapshot)
	}
}

func TestRuntimeSharedHandoffsProgressWithEitherOutputHeldFirst(t *testing.T) {
	for _, nativeFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(nativeFirst), func(t *testing.T) {
			memory := fullResidentBorrowMemory(t)
			metadata := &pb.Document{ContentType: "application/octet-stream", Data: make([]byte, protocol.NativeChunk)}
			head := &pb.NativeHead{Metadata: metadata, BodyContentType: "application/octet-stream"}
			headValue := &pb.Event_Head{Head: head}
			headEvent := &pb.Event{Value: headValue}
			end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
			endValue := &pb.Event_NativeEnd{NativeEnd: end}
			endEvent := &pb.Event{Value: endValue}
			nativeAdapter := &borrowBoundNativeAdapter{held: protocol.NativeChunk + execution.EntryOverheadBytes, head: headEvent, end: endEvent}
			typedAdapter := &sharedTypedMemoryAdapter{document: protocol.NativeChunk}
			limits := DefaultLimits()
			native, err := New(nativeAdapter, limits, memory)
			if err != nil {
				t.Fatal(err)
			}
			defer native.Close(context.Background())
			typed, err := New(typedAdapter, limits, memory)
			if err != nil {
				t.Fatal(err)
			}
			defer typed.Close(context.Background())
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			session := native.NewSession()
			defer session.Close()
			var nativeTicket *Ticket
			var nativeHead *Emission
			var nativeWire, typedWire *WireReservation
			for index := range 2 {
				isNative := index == 1
				if nativeFirst {
					isNative = !isNative
				}
				work := plan(uint64(index+100), "healthy", !isNative)
				if isNative {
					work.Command = nativeCall()
					work.WorkingBytes, work.IOReserve = defaultNativeIOReserve, defaultNativeIOReserve
					var failure *pb.Failure
					nativeTicket, failure, _ = native.Submit(ctx, work, session)
					if failure != nil {
						t.Fatal(failure)
					}
					select {
					case nativeHead = <-session.Events:
						if !proto.Equal(nativeHead.Event, headEvent) {
							t.Fatal("protected short Native did not deliver its response", nativeHead.Event, memory.Snapshot())
						}
					case <-ctx.Done():
						t.Fatal("slow typed wire blocked the short Native", memory.Snapshot())
					}
					response := &pb.ExecuteResponse{Index: work.ID, Event: nativeHead.Event}
					nativeWire, err = nativeTicket.ReserveWire(max(proto.Size(response), execution.MinimumWireBytes))
					if err != nil {
						t.Fatal(err)
					}
					defer nativeWire.Release()
				} else {
					work.WorkingBytes, work.IOReserve = 2*protocol.NativeChunk+8192, 48_000_000
					ticket, failure, _ := typed.Submit(ctx, work, nil)
					if failure != nil {
						t.Fatal(failure)
					}
					event, err := ticket.Wait(ctx)
					if err != nil || len(event.GetReadResult().GetDocument().GetData()) != protocol.NativeChunk {
						t.Fatal("slow Native output blocked a short typed document", event, err)
					}
					response := &pb.ExecuteResponse{Index: work.ID, Event: event}
					typedWire, err = ticket.ReserveWire(max(proto.Size(response), execution.MinimumWireBytes))
					if err != nil {
						t.Fatal(err)
					}
					defer typedWire.Release()
					ticket.Ack()
				}
			}
			if snapshot := memory.Snapshot(); snapshot.Borrowed != executionBorrowLimit || snapshot.WireCount != 2 || snapshot.BudgetRejections != 0 || snapshot.UsedBytes > snapshot.BudgetBytes {
				t.Fatal("test did not retain both real short outputs within the same budget", snapshot)
			}
			// Match server.sendEvent: the old encoded owner ends before releasing
			// the emission, allowing the next terminal to be published serially.
			nativeWire.Release()
			nativeHead.Release()
			select {
			case emission := <-session.Events:
				if emission.Event.GetNativeEnd().GetCompletion() != pb.NativeCompletion_RESPONSE_COMPLETE {
					t.Fatal("short Native terminal lost its evidence", emission.Event)
				}
				wire, err := nativeTicket.ReserveWire(execution.MinimumWireBytes)
				if err != nil {
					t.Fatal("fixed completion could not use prepaid codec minimum", err)
				}
				wire.Release()
				emission.Release()
				nativeTicket.Ack()
			case <-ctx.Done():
				t.Fatal("short Native completion required another handoff", memory.Snapshot())
			}
			waitReleased(t, native)
			if snapshot := memory.Snapshot(); snapshot.WireCount != 1 || snapshot.Borrowed != 1 || snapshot.BudgetRejections != 0 {
				t.Fatal("Native terminal retired the other Store's live wire", snapshot)
			}
			typedWire.Release()
			waitReleased(t, typed)
		})
	}
}

type borrowBoundNativeAdapter struct {
	lifecycleAdapter
	held     int
	head     *pb.Event
	end      *pb.Event
	attempts atomic.Int32
}

func (a *borrowBoundNativeAdapter) Execute(_ context.Context, works []*execution.Plan, emit execution.Emit) {
	work := works[0]
	a.attempts.Add(1)
	// Initial IO has returned its actual response. Match the real adapters'
	// ownership transition before emitting the first event.
	work.WorkingBytes = a.held
	err := emit(work, a.head)
	end := a.end
	if err != nil {
		failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, err.Error())
		incomplete := protocol.NativeFailure(true, failure)
		value := &pb.Event_NativeEnd{NativeEnd: incomplete}
		end = &pb.Event{Value: value}
	}
	// Reply cleanup happens before terminal publication, as in both adapters.
	work.WorkingBytes = execution.EntryOverheadBytes
	_ = emit(work, end)
}

func TestRuntimeLargeBorrowFailureIsIncompleteAndNeverReplayed(t *testing.T) {
	memory := fullResidentBorrowMemory(t)
	head := &pb.NativeHead{BodyContentType: "application/bson"}
	headValue := &pb.Event_Head{Head: head}
	headEvent := &pb.Event{Value: headValue}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	endValue := &pb.Event_NativeEnd{NativeEnd: end}
	endEvent := &pb.Event{Value: endValue}
	adapter := &borrowBoundNativeAdapter{held: 12 << 20, head: headEvent, end: endEvent}
	limits := DefaultLimits()
	runtime, err := New(adapter, limits, memory)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	session := runtime.NewSession()
	defer session.Close()
	work := plan(100, "large-native", false)
	work.Command, work.WorkingBytes, work.IOReserve = nativeCall(), defaultNativeIOReserve, defaultNativeIOReserve
	ticket, failure, _ := runtime.Submit(ctx, work, session)
	if failure != nil {
		t.Fatal(failure)
	}
	select {
	case emission := <-session.Events:
		end := emission.Event.GetNativeEnd()
		if end.GetCompletion() != pb.NativeCompletion_RESPONSE_INCOMPLETE || end.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
			t.Fatal("attempted output failure lost incomplete evidence", emission.Event)
		}
		wire, err := ticket.ReserveWire(execution.MinimumWireBytes)
		if err != nil {
			t.Fatal(err)
		}
		wire.Release()
		emission.Release()
		ticket.Ack()
	case <-ctx.Done():
		t.Fatal("rejected output waited for slow consumers", memory.Snapshot())
	}
	waitReleased(t, runtime)
	if snapshot := memory.Snapshot(); adapter.attempts.Load() != 1 || snapshot.Borrowed != 0 || snapshot.BudgetRejections != 1 || snapshot.UsedBytes != snapshot.ResidentBytes {
		t.Fatal("attempted Native replayed or retained protected IO memory", adapter.attempts.Load(), snapshot)
	}
}
