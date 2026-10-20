//go:build integration

package mongodb

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// This mirror forwards the actual Runtime-installed Ticket, exactly once per
// call with unchanged arguments/results. It owns no credit and never releases it.
// Runtime.Submit copies the Plan before installing that Ticket. Registration is
// therefore by the immutable owned record ID, never the pre-Submit Plan pointer.
type replicaLuaMemory struct {
	command                   *pb.Command
	native                    *plan
	operation                 testmongo.LuaOperation
	owner                     execution.Memory
	working                   atomic.Int64
	workingCalls, resultCalls atomic.Int32
	overflow                  atomic.Bool
	actualTicket              atomic.Bool
}

func (m *replicaLuaMemory) SetWorking(ctx context.Context, n int) error {
	err := m.owner.SetWorking(ctx, n)
	if err == nil {
		m.working.Store(int64(n))
		if m.workingCalls.Add(1) > 4096 {
			m.overflow.Store(true)
		}
	}
	return err
}
func (m *replicaLuaMemory) ReserveResult(n int) error {
	err := m.owner.ReserveResult(n)
	if err == nil && m.resultCalls.Add(1) > 32 {
		m.overflow.Store(true)
	}
	return err
}
func (a *replicaReadAdapter) executeReplicaPlans(ctx context.Context, plans []*execution.Plan, emit execution.Emit) {
	a.luaMu.Lock()
	originals := make(map[*execution.Plan]execution.Memory)
	for _, work := range plans {
		if mirror := a.luaPlans[work.ID]; mirror != nil {
			native, ok := work.Backend.(*plan)
			ownedID, idOK := "", false
			if ok {
				ownedID, idOK = native.id.(string)
			}
			if work.Command != mirror.command || !ok || native != mirror.native || native.target.String() != mirror.operation.Namespace || !idOK || ownedID != mirror.operation.ID || work.Command.GetMutate().GetAtomicTransform().GetLua() == nil {
				mirror.overflow.Store(true)
				continue
			}
			originals[work] = work.Memory
			mirror.owner = work.Memory
			_, actualTicket := work.Memory.(*store.Ticket)
			mirror.actualTicket.Store(actualTicket)
			work.Memory = mirror
		}
	}
	a.luaMu.Unlock()
	a.Adapter.Execute(ctx, plans, emit)
	for work, original := range originals {
		work.Memory = original
	}
}

type replicaLuaRuntimeOptions struct {
	runtime  *store.Runtime
	delegate *replicaReadAdapter
	fixture  *testmongo.Replica
	proxy    *testmongo.ReplicaProxy
	primary  int
}

func replicaLuaZero(t *testing.T, opts replicaLuaRuntimeOptions) {
	t.Helper()
	s := opts.runtime.Snapshot()
	m := opts.runtime.ExecutionMemory().Snapshot()
	if s.Active != 0 || s.Pending != 0 || s.Retained != 0 || s.ResultBytes != 0 || s.WorkingBytes != 0 || s.Publishers != 0 || m.Count != 0 || m.InputUsedBytes != 0 || m.InputReservedBytes != 0 || m.UsedBytes != 0 || opts.delegate.entered.Load() != opts.delegate.returned.Load() || opts.delegate.closeEntered.Load() != 0 || opts.delegate.client.NumberSessionsInProgress() != 0 {
		t.Fatal("real shared Runtime Lua zero", s, m)
	}
}
func replicaLuaWorking(t *testing.T, opts replicaLuaRuntimeOptions, mirror *replicaLuaMemory) int64 {
	t.Helper()
	s := opts.runtime.Snapshot()
	m := opts.runtime.ExecutionMemory().Snapshot()
	n := mirror.working.Load()
	if !mirror.actualTicket.Load() || n <= 0 || s.WorkingBytes != int(n) || s.Active != 1 || s.Retained != 1 || m.Count != 1 || m.InputUsedBytes == 0 || m.UsedBytes == 0 || mirror.overflow.Load() {
		t.Fatal("actual held snapshot working/input owner", n, s, m)
	}
	return n
}
func replicaLuaRuntime(t *testing.T, opts replicaLuaRuntimeOptions) []testmongo.LuaCase {
	t.Helper()
	replicaLuaZero(t, opts)
	originalExecutions := opts.delegate.entered.Load()
	cases := make([]testmongo.LuaCase, 0, 2)
	for i, confirmed := range []bool{true, false} {
		id := "runtime-lua-confirmed"
		if !confirmed {
			id = "runtime-lua-lost-abort"
		}
		op := testmongo.LuaOperation{Namespace: opts.fixture.DB + ".records", ID: id}
		watch, err := opts.proxy.WatchLua(op)
		if err != nil {
			t.Fatal(err)
		}
		if err := opts.proxy.ArmLuaConflict(opts.primary, watch); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
		defer cancel()
		opts.proxy.SeedLua(t, ctx, watch)
		input := bson.D{{Key: "attempt", Value: int32(0)}}
		// Prepare through the same typed record/Runtime path as ordinary work.
		raw, err := bson.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		luaInput := &pb.Document{ContentType: "application/bson", Data: raw}
		lua := &pb.LuaTransform{Source: []byte(testmongo.ReplicaLuaSource), Input: luaInput}
		form := &pb.Transform_Lua{Lua: lua}
		transform := &pb.Transform{Form: form}
		atomicTransform := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
		mutate := &pb.MutateRequest{Resource: opts.fixture.DB + "/records/s:" + id, Action: atomicTransform}
		operation := &pb.Command_Mutate{Mutate: mutate}
		command := &pb.Command{Operation: operation}
		record, err := execution.NewRecord("mongo", uint64(501+i), command)
		if err != nil {
			t.Fatal(err)
		}
		work, failure := opts.runtime.PrepareRecord(ctx, record)
		if failure != nil {
			t.Fatal(failure)
		}
		mirror := &replicaLuaMemory{command: work.Command, native: work.Backend.(*plan), operation: op}
		opts.delegate.luaMu.Lock()
		if opts.delegate.luaPlans == nil {
			opts.delegate.luaPlans = make(map[uint64]*replicaLuaMemory)
		}
		opts.delegate.luaPlans[work.ID] = mirror
		opts.delegate.luaMu.Unlock()
		session := opts.runtime.NewSession()
		defer session.Close()
		session.Events = make(chan *store.Emission, 1)
		ticket, failure, _ := opts.runtime.Submit(ctx, work, session)
		if failure != nil {
			t.Fatal(failure)
		}
		if err := testmongo.WaitLuaPhase(ctx, watch.SnapshotHeld(0)); err != nil {
			t.Fatal("first real snapshot", err, watch.Snapshot())
		}
		firstWorking := replicaLuaWorking(t, opts, mirror)
		opts.proxy.ExternalLuaUpdate(t, ctx, watch)
		if err := watch.ForwardSnapshot(0); err != nil {
			t.Fatal(err)
		}
		if err := testmongo.WaitLuaPhase(ctx, watch.AbortHeld()); err != nil {
			t.Fatal("real conflict/abort reply", err, watch.Snapshot())
		}
		if err := watch.ValidateAbortHeld(); err != nil {
			t.Fatal("held abort no-rebuild barrier", watch.Snapshot())
		}
		expected := testmongo.LuaLedger{N: 20, External: 1}
		opts.proxy.CheckLuaLedger(t, ctx, watch, expected)
		if opts.delegate.returned.Load() != originalExecutions+int32(2*i) {
			t.Fatal("Execute returned while real abort held")
		}
		if confirmed {
			if err := watch.ForwardAbortReply(); err != nil {
				t.Fatal(err)
			}
			if err := testmongo.WaitLuaPhase(ctx, watch.SnapshotHeld(1)); err != nil {
				t.Fatal("fresh snapshot", err, watch.Snapshot())
			}
			secondWorking := replicaLuaWorking(t, opts, mirror)
			if firstWorking != secondWorking {
				t.Fatal("retry accumulated prior attempt memory", firstWorking, secondWorking)
			}
			if err := watch.ForwardSnapshot(1); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := watch.DropAbortReply(); err != nil {
				t.Fatal(err)
			}
		}
		event, err := ticket.Wait(ctx)
		if err != nil || ctx.Err() != nil || event.GetMutationResult() == nil {
			t.Fatal("original live Lua receipt", err, ctx.Err())
		}
		result := event.GetMutationResult()
		if confirmed {
			if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
				t.Fatal("confirmed abort retry", result)
			}
		} else {
			if result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE || !strings.Contains(result.GetFailure().GetMessage(), "rollback unconfirmed") {
				t.Fatal("lost abort no rebuild receipt", result)
			}
		}
		var emission *store.Emission
		select {
		case emission = <-session.Events:
		case <-ctx.Done():
			t.Fatal("Lua real publisher", ctx.Err())
		}
		emission.Release()
		for opts.runtime.Snapshot().Active != 0 || opts.runtime.Snapshot().Publishers != 0 || opts.delegate.returned.Load() != originalExecutions+int32(2*i)+1 {
			if ctx.Err() != nil {
				t.Fatal("Lua actual Execute/publisher join", ctx.Err())
			}
			time.Sleep(time.Millisecond)
		}
		held := opts.runtime.Snapshot()
		credit := opts.runtime.ExecutionMemory().Snapshot()
		if held.WorkingBytes != 0 || held.Retained != 1 || held.ResultBytes <= 0 || credit.Count != 1 || credit.InputUsedBytes == 0 || mirror.workingCalls.Load() == 0 || mirror.resultCalls.Load() != 0 || mirror.overflow.Load() || opts.delegate.client.NumberSessionsInProgress() != 0 {
			t.Fatal("real joined Lua retained result owner", held, credit)
		}
		if observed := work.Backend.(*plan).program.ObservedAt.UTC().Format(time.RFC3339Nano); observed != watch.Snapshot().Attempt[0].Observed {
			t.Fatal("original ObservedAt witness", observed, watch.Snapshot())
		}
		ticket.Ack()
		// Ack alone must release the genuine retained owner before Session.Close
		// has any opportunity to reclaim missing input/result credit.
		replicaLuaZero(t, opts)
		t.Logf("Lua actual Ack before owning Session.Close snapshot=%+v memory=%+v", opts.runtime.Snapshot(), opts.runtime.ExecutionMemory().Snapshot())
		session.Close()
		opts.delegate.luaMu.Lock()
		delete(opts.delegate.luaPlans, work.ID)
		opts.delegate.luaMu.Unlock()
		replicaLuaZero(t, opts)
		if err := watch.Complete(confirmed); err != nil {
			t.Fatal("Lua terminal receipt/join completion", watch.Snapshot())
		}
		c := testmongo.LuaCase{Watch: watch, Confirmed: confirmed}
		cases = append(cases, c)
		opts.proxy.CheckLuaCases(t, cases, "backend_receipt_and_real_Ack")
		cancel()
		external, stop := context.WithTimeout(t.Context(), 3*time.Second)
		_, usable, err := opts.fixture.WaitUsable(external, -1)
		stop()
		if err != nil {
			t.Fatal(err)
		}
		recovery, stop := context.WithDeadline(t.Context(), usable.Add(5*time.Second))
		newID := id + "-new-work"
		prepared := ownerMutation(t, opts.delegate.Adapter, opts.fixture.DB, newID)
		newRecord, err := execution.NewRecord("mongo", uint64(601+i), prepared.Command)
		if err != nil {
			t.Fatal(err)
		}
		newPlan, failure := opts.runtime.PrepareRecord(recovery, newRecord)
		if failure != nil {
			t.Fatal(failure)
		}
		newTicket, failure, _ := opts.runtime.Submit(recovery, newPlan, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		newEvent, err := newTicket.Wait(recovery)
		if err != nil || newEvent.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || newEvent.GetMutationResult().GetFailure() != nil || time.Since(usable) > 5*time.Second {
			t.Fatal("same Runtime recovery", err)
		}
		newTicket.Ack()
		for opts.delegate.entered.Load() != opts.delegate.returned.Load() || opts.runtime.Snapshot().Active != 0 {
			if recovery.Err() != nil {
				t.Fatal("recovery Execute join")
			}
			time.Sleep(time.Millisecond)
		}
		stop()
		replicaLuaZero(t, opts)
		cases[i].RecoveryID = newID
		opts.proxy.CheckLuaCases(t, cases, "backend_new_work_recovery")
	}
	if opts.delegate.entered.Load() != originalExecutions+4 {
		t.Fatal("Lua and new-work exact Execute count")
	}
	return cases
}
