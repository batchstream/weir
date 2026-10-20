//go:build integration

package mongodb

import (
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func replicaLuaCommitRuntime(t *testing.T, opts replicaLuaRuntimeOptions, abortCases []testmongo.LuaCase) []testmongo.LuaCommitCase {
	t.Helper()
	replicaLuaZero(t, opts)
	originalExecutions := opts.delegate.entered.Load()
	cases := make([]testmongo.LuaCommitCase, 0, 2)
	for i, all := range []bool{false, true} {
		id := "runtime-lua-commit-once"
		if all {
			id = "runtime-lua-commit-all"
		}
		op := testmongo.LuaOperation{Namespace: opts.fixture.DB + ".records", ID: id}
		watch, err := opts.proxy.WatchLuaCommit(op, all, backend.DefaultOptions().RetryAttempts)
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
		record, err := execution.NewRecord("mongo", uint64(701+i), command)
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
		if err := watch.ForwardSnapshot(0); err != nil {
			t.Fatal(err)
		}
		if err := testmongo.WaitLuaPhase(ctx, watch.CommitHeld()); err != nil {
			t.Fatal("first real successful commit", err, watch.Snapshot())
		}
		replicaLuaWorking(t, opts, mirror)
		if err := watch.ValidateCommitHeld(); err != nil {
			t.Fatal("held commit before durable barrier", watch.Snapshot())
		}
		expected := testmongo.LuaLedger{N: 1, Candidate: 1}
		opts.proxy.CheckLuaLedger(t, ctx, watch, expected)
		if opts.delegate.returned.Load() != originalExecutions+int32(2*i) {
			t.Fatal("Execute returned while successful commit held")
		}
		if err := watch.DropFirstCommitReply(); err != nil {
			t.Fatal(err)
		}
		event, err := ticket.Wait(ctx)
		if err != nil || ctx.Err() != nil || event.GetMutationResult() == nil {
			t.Fatal("original live Lua receipt", err, ctx.Err())
		}
		result := event.GetMutationResult()
		if !all {
			if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
				t.Fatal("original commit confirmed", result)
			}
		} else {
			if result.GetOutcome() != pb.MutationOutcome_UNKNOWN || result.GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE {
				t.Fatal("lost original commit acknowledgement", result)
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
		if err := watch.CompleteCommit(); err != nil {
			t.Fatal("commit terminal receipt/join completion", watch.Snapshot())
		}
		c := testmongo.LuaCommitCase{Watch: watch}
		cases = append(cases, c)
		opts.proxy.CheckLuaCommitCases(t, cases, "backend_receipt_and_real_Ack")
		opts.proxy.CheckLuaCases(t, abortCases, "commit_receipt_old_abort_unchanged")
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
		newRecord, err := execution.NewRecord("mongo", uint64(801+i), prepared.Command)
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
		opts.proxy.CheckLuaCommitCases(t, cases, "backend_new_work_recovery")
		opts.proxy.CheckLuaCases(t, abortCases, "commit_recovery_old_abort_unchanged")
	}
	if opts.delegate.entered.Load() != originalExecutions+4 {
		t.Fatal("Lua and new-work exact Execute count")
	}
	return cases
}
