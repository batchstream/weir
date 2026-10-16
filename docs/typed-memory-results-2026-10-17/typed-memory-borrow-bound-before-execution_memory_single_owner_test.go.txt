package store

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/protobuf/proto"
)

const singleOwnerBudget = 113_544_396

func singleOwnerPutPlan(t testing.TB) *execution.Plan {
	t.Helper()
	const size = (16 << 20) - 64
	const id = "sdk-write-256-16777152"
	fields := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int64(1)}, {Key: "pad", Value: ""}}
	base, err := bson.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	fields[2].Value = strings.Repeat("x", size-len(base))
	raw, err := bson.Marshal(fields)
	if err != nil || len(raw) != size {
		t.Fatal("SDK-shaped BSON size", len(raw), err)
	}
	document := &pb.Document{ContentType: "application/bson", Data: raw}
	action := &pb.MutateRequest_Put{Put: document}
	mutation := &pb.MutateRequest{Resource: "typed_memory/records/s:" + id, Action: action}
	operation := &pb.Command_Mutate{Mutate: mutation}
	command := &pb.Command{Operation: operation}
	record, err := execution.NewRecord("mongo", 1, command)
	if err != nil {
		t.Fatal(err)
	}
	work := &execution.Plan{ID: 1, Command: command, Key: record.Key(), BatchKey: "records", Bytes: record.Bytes(), ResultBytes: execution.ResultOverheadBytes, WorkingBytes: 2*record.Bytes() + execution.ResultOverheadBytes, IOReserve: 48_000_000}
	return work
}

func TestExecutionMemorySingleOwnerLargePutReclaimsOnlyUnusedInputPromise(t *testing.T) {
	work := singleOwnerPutPlan(t)
	memory := testExecutionMemory(singleOwnerBudget * 3 / 2)
	lease, failure := memory.admit(work)
	if failure != nil {
		t.Fatal(failure)
	}
	defer lease.release()
	before := memory.Snapshot()
	working := work.IOReserve + work.WorkingBytes
	if before.InputUsedBytes+uint64(working) > before.BudgetBytes || before.UsedBytes+uint64(working) <= before.BudgetBytes {
		t.Fatal("test did not separate real SDK-shaped input/IO from unused queue space", before, working)
	}
	started, waiting := lease.start(working, work.IOReserve)
	if !started || waiting {
		t.Fatal("one legal SDK-shaped Put was excluded by unused queue capacity", before, memory.Snapshot(), working)
	}
	after := memory.Snapshot()
	shortage := uint64(working) - (before.BudgetBytes - before.UsedBytes)
	if after.InputReservedBytes != before.InputReservedBytes-shortage || after.InputUsedBytes != before.InputUsedBytes || after.UsedBytes != after.BudgetBytes || after.IOReserveBytes != before.IOReserveBytes {
		t.Fatal("start did not revoke exactly the unused promise needed for the actual peak", before, after)
	}
	// A repeated trusted sample cannot promise queue bytes over actual IO.
	sample := overload.Snapshot{RecoveryAllowance: singleOwnerBudget * 3 / 2, RecoveryAllowanceKnown: true}
	memory.ObserveMemory(sample)
	if sampled := memory.Snapshot(); sampled.InputReservedBytes != after.InputReservedBytes || sampled.UsedBytes != after.UsedBytes {
		t.Fatal("sampling restored a promise on top of live backend memory", after, sampled)
	}
	t.Logf("SDK BSON=%d input=%d IO+working=%d before=%+v after=%+v", len(work.Command.GetMutate().GetPut().Data), before.InputUsedBytes, working, before, after)
	lease.release()
	if snapshot := memory.Snapshot(); snapshot.UsedBytes != 0 || snapshot.Count != 0 || snapshot.InputUsedBytes != 0 {
		t.Fatal("completed large input retained ownership", snapshot)
	}
	adapter := &singleExecutionAdapter{}
	limits := DefaultLimits()
	limits.BatchWait = 0
	runtime, err := New(adapter, limits, memory)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	ticket, failure, _ := runtime.Submit(ctx, work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	event, err := ticket.Wait(ctx)
	if err != nil || event.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("real runtime dispatch changed a fitting mutation to NOT_STARTED", event, err)
	}
	ticket.Ack()
	waitReleased(t, runtime)
	if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 || snapshot.BudgetRejections != 0 {
		t.Fatal("successful sole-owner dispatch retained or rejected memory", snapshot)
	}
}

func TestExecutionMemorySingleOwnerFirstResultReclaimsExactDiagnosticShortage(t *testing.T) {
	for _, working := range []int{48_003_006, 16_780_234} {
		t.Run(fmt.Sprint(working), func(t *testing.T) {
			memory := testExecutionMemory(singleOwnerBudget * 3 / 2)
			work := plan(1, "typed_memory/records/s:item", true)
			work.Bytes = 1247
			lease, failure := memory.admit(work)
			if failure != nil {
				t.Fatal(failure)
			}
			if started, waiting := lease.start(48_003_006, 48_000_000); !started || waiting {
				t.Fatal("read could not reserve the real negotiated wire message")
			}
			if err := lease.setWorking(working); err != nil {
				t.Fatal(err)
			}
			before := memory.Snapshot()
			const next = 33_553_279 + execution.ResultOverheadBytes
			const requested = 16_777_664
			if before.InputUsedBytes != 3296 || before.InputReservedBytes != 32_571_238 || before.ResidentBytes != before.InputReservedBytes {
				t.Fatal("test no longer reproduces the observed locked Read ownership", before)
			}
			if err := lease.reserveResult(requested); err != nil {
				t.Fatal("first result was denied solely by unused input promise", err, before, memory.Snapshot())
			}
			after := memory.Snapshot()
			const revoked = 982_041 + execution.ResultOverheadBytes
			if before.InputReservedBytes-after.InputReservedBytes != revoked || after.InputUsedBytes != before.InputUsedBytes || after.IOReserveBytes != before.IOReserveBytes || after.UsedBytes != before.UsedBytes+next-revoked || after.ResidentBytes != after.BudgetBytes-after.IOReserveBytes || lease.working != working {
				t.Fatal("first result did not revoke the minimum of both real constraints", before, after)
			}
			t.Logf("working=%d next=%d revoked=%d before=%+v after=%+v", working, next, revoked, before, after)
			lease.finishWorking()
			wire, err := lease.reserveWire((16 << 20) - 64)
			if err != nil {
				t.Fatal(err)
			}
			lease.release()
			if snapshot := memory.Snapshot(); snapshot.WireCount != 1 || snapshot.Count != 1 || snapshot.UsedBytes < snapshot.WireBytes {
				t.Fatal("Ack erased the large response's real wire ownership", snapshot)
			}
			adapter := &singleExecutionAdapter{}
			limits := DefaultLimits()
			limits.BatchWait = 0
			runtime, err := New(adapter, limits, memory)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close(context.Background())
			session := runtime.NewSession()
			defer session.Close()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			short := plan(2, "short", false)
			short.Command, short.WorkingBytes, short.IOReserve = nativeCall(), 48_003_006, 48_000_000
			ticket, failure, _ := runtime.Submit(ctx, short, session)
			if failure != nil {
				t.Fatal("retained large wire excluded a healthy short Native", failure, memory.Snapshot())
			}
			complete := false
			for {
				select {
				case emission := <-session.Events:
					event := emission.Event
					if end := event.GetNativeEnd(); end != nil {
						complete = end.Completion == pb.NativeCompletion_RESPONSE_COMPLETE
					}
					emission.Release()
					if event == nil {
						if !complete {
							t.Fatal("short Native ended without complete backend evidence")
						}
						ticket.Ack()
						goto completed
					}
				case <-ctx.Done():
					t.Fatal("retained large wire blocked healthy Native progress", memory.Snapshot())
				}
			}
		completed:
			waitReleased(t, runtime)
			if snapshot := memory.Snapshot(); snapshot.Count != 1 || snapshot.WireCount != 1 || snapshot.IOReserveBytes != before.IOReserveBytes || snapshot.UsedBytes > snapshot.BudgetBytes {
				t.Fatal("healthy short Native consumed or erased the slow large wire", snapshot)
			}
			wire.Release()
			if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 || snapshot.InputReservedBytes != 0 || snapshot.InputUsedBytes != 0 {
				t.Fatal("wire completion retained sole-owner accounting", snapshot)
			}
		})
	}
}

func TestExecutionMemorySingleOwnerLargeReadCompletesThroughRuntime(t *testing.T) {
	for _, extra := range []int{0, 4096} {
		t.Run(fmt.Sprint(extra), func(t *testing.T) {
			memory := testExecutionMemory(singleOwnerBudget * 3 / 2)
			const size = (16 << 20) - 64
			adapter := &sharedTypedMemoryAdapter{document: size, capacity: size + extra}
			limits := DefaultLimits()
			limits.BatchWait = 0
			runtime, err := New(adapter, limits, memory)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close(context.Background())
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			work := plan(math.MaxUint64, "typed_memory/records/s:item", true)
			work.Bytes, work.WorkingBytes, work.IOReserve = 1247, 3006, 48_000_000
			ticket, failure, _ := runtime.Submit(ctx, work, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			event, err := ticket.Wait(ctx)
			if err != nil || len(event.GetReadResult().GetDocument().GetData()) != size {
				t.Fatal("full runtime completion discarded a fitting large Read after the source reservation", event.GetReadResult().GetFailure(), err, memory.Snapshot())
			}
			response := &pb.ExecuteResponse{Index: math.MaxUint64, Event: event}
			encoded := proto.Size(response)
			if encoded-size > execution.ResultOverheadBytes/2 {
				t.Fatal("real backend Read envelope exceeded its prepaid wrapper bound", encoded, size)
			}
			memory.mu.Lock()
			retained := ticket.executionLease.result
			memory.mu.Unlock()
			if retained < size+extra-execution.ResultOverheadBytes {
				t.Fatal("encoded length discarded source backing capacity", retained, size, extra)
			}
			wire, err := ticket.ReserveWire(encoded)
			if err != nil {
				t.Fatal("actual wire did not fit its existing publication promise", err)
			}
			ticket.Ack()
			waitReleased(t, runtime)
			if snapshot := memory.Snapshot(); snapshot.Count != 1 || snapshot.WireCount != 1 || snapshot.UsedBytes < snapshot.WireBytes {
				t.Fatal("Ack erased the full Runtime result's actual wire", snapshot)
			}
			wire.Release()
			if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 || snapshot.BudgetRejections != 0 {
				t.Fatal("full Runtime Read retained or rejected ownership", snapshot)
			}
			t.Logf("document=%d cap=%d max-index encoded=%d wrapper=%d", size, size+extra, encoded, encoded-size)
		})
	}
}

func TestExecutionMemorySingleOwnerDoesNotRevokeSharedOrActualMemory(t *testing.T) {
	for _, mode := range []string{"second_input", "other_live_wire", "same_live_wire", "existing_result", "extra_io", "actual_peak", "known_zero"} {
		t.Run(mode, func(t *testing.T) {
			memory := testExecutionMemory(singleOwnerBudget * 3 / 2)
			work := plan(1, "read", true)
			work.Bytes = 1247
			lease, failure := memory.admit(work)
			if failure != nil {
				t.Fatal(failure)
			}
			defer lease.release()
			if started, waiting := lease.start(48_003_006, 48_000_000); !started || waiting {
				t.Fatal("initial read could not start")
			}
			requested := 16_777_664
			switch mode {
			case "second_input", "other_live_wire":
				other, failure := memory.admit(work)
				if failure != nil {
					t.Fatal(failure)
				}
				defer other.release()
				if mode == "other_live_wire" {
					wire, err := other.reserveWire(execution.MinimumWireBytes)
					if err != nil {
						t.Fatal(err)
					}
					defer wire.Release()
					other.release()
				}
			case "same_live_wire":
				wire, err := lease.reserveWire(execution.MinimumWireBytes)
				if err != nil {
					t.Fatal(err)
				}
				defer wire.Release()
			case "existing_result":
				if err := lease.reserveResult(1024); err != nil {
					t.Fatal(err)
				}
			case "extra_io":
				if started, waiting := lease.beginRead(1); !started || waiting {
					t.Fatal("extra IO could not start")
				}
			case "actual_peak":
				requested = 64 << 20
			case "known_zero":
				sample := overload.Snapshot{RecoveryAllowanceKnown: true}
				memory.ObserveMemory(sample)
			}
			before := memory.Snapshot()
			if err := lease.reserveResult(requested); err != execution.ErrExecutionMemory {
				t.Fatal("non-sole or genuinely oversized allocation did not fail", err, before, memory.Snapshot())
			}
			after := memory.Snapshot()
			if after.InputReservedBytes != before.InputReservedBytes || after.InputUsedBytes != before.InputUsedBytes || after.UsedBytes != before.UsedBytes || after.ResidentBytes != before.ResidentBytes || after.IOReserveBytes != before.IOReserveBytes || after.Count != before.Count || after.WireBytes != before.WireBytes || after.BudgetRejections != before.BudgetRejections+1 {
				t.Fatal("a failed first result revoked another promise or real ownership", before, after)
			}
		})
	}
}

func TestExecutionMemorySingleOwnerStartRequiresSoleWaitingActualPeak(t *testing.T) {
	for _, mode := range []string{"second_input", "other_live_wire", "actual_peak"} {
		t.Run(mode, func(t *testing.T) {
			memory := testExecutionMemory(singleOwnerBudget * 3 / 2)
			work := plan(1, "large", false)
			work.Bytes = 16_778_415
			lease, failure := memory.admit(work)
			if failure != nil {
				t.Fatal(failure)
			}
			defer lease.release()
			working := 81_557_342
			if mode == "actual_peak" {
				working = int(memory.budget-memory.inputUsed) + 1
			} else {
				otherWork := plan(2, "other", false)
				other, failure := memory.admit(otherWork)
				if failure != nil {
					t.Fatal(failure)
				}
				defer other.release()
				if mode == "other_live_wire" {
					wire, err := other.reserveWire(execution.MinimumWireBytes)
					if err != nil {
						t.Fatal(err)
					}
					defer wire.Release()
					other.release()
				}
			}
			before := memory.Snapshot()
			if started, waiting := lease.start(working, 48_000_000); started || waiting {
				t.Fatal("shared or truly oversized start bypassed a real bound", before, memory.Snapshot())
			}
			after := memory.Snapshot()
			if after.InputReservedBytes != before.InputReservedBytes || after.InputUsedBytes != before.InputUsedBytes || after.UsedBytes != before.UsedBytes || after.ResidentBytes != before.ResidentBytes || after.Starting != 0 || after.BudgetRejections != before.BudgetRejections+1 {
				t.Fatal("failed start changed queue promises or began IO", before, after)
			}
		})
	}
}

func TestExecutionMemorySingleOwnerRevokedPromiseReturnsOnlyAfterRealRelease(t *testing.T) {
	memory := testExecutionMemory(singleOwnerBudget * 3 / 2)
	work := plan(1, "large", false)
	work.Bytes = 16_778_415
	lease, failure := memory.admit(work)
	if failure != nil {
		t.Fatal(failure)
	}
	defer lease.release()
	if started, waiting := lease.start(81_557_342, 48_000_000); !started || waiting {
		t.Fatal("sole large input could not start")
	}
	before := memory.Snapshot()
	sample := overload.Snapshot{Unknown: true}
	memory.ObserveMemory(sample)
	if snapshot := memory.Snapshot(); snapshot.UsedBytes != before.UsedBytes || snapshot.InputReservedBytes != before.InputReservedBytes || snapshot.Known {
		t.Fatal("unknown sample changed actual working ownership", snapshot)
	}
	lease.finishWorking()
	sample.Unknown, sample.RecoveryAllowanceKnown, sample.RecoveryAllowance = false, true, singleOwnerBudget*3/2
	memory.ObserveMemory(sample)
	if snapshot := memory.Snapshot(); snapshot.InputReservedBytes != 32_571_238 || snapshot.UsedBytes != snapshot.InputReservedBytes || snapshot.InputUsedBytes != before.InputUsedBytes {
		t.Fatal("freed backend workspace did not restore the ordinary input promise", snapshot)
	}
	sample.RecoveryAllowance = 0
	memory.ObserveMemory(sample)
	if snapshot := memory.Snapshot(); snapshot.InputReservedBytes != snapshot.InputUsedBytes || snapshot.InputUsedBytes != before.InputUsedBytes {
		t.Fatal("known-zero shrink removed real input", snapshot)
	}
	lease.release()
	if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 || snapshot.ResidentBytes != 0 || snapshot.InputReservedBytes != 0 || snapshot.InputUsedBytes != 0 {
		t.Fatal("cancel retained the revoked input promise", snapshot)
	}
}
