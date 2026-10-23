//go:build integration

package mongodb

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type replicaLuaRuntimeOptions struct {
	runtime  *store.Runtime
	delegate *replicaReadAdapter
	fixture  *testmongo.Replica
	proxy    *testmongo.ReplicaProxy
	primary  int
}

// replicaLuaIdle checks that every adapter execution has returned and no
// driver session remains checked out.
func replicaLuaIdle(t *testing.T, opts replicaLuaRuntimeOptions) {
	t.Helper()
	delegate := opts.delegate
	if delegate.entered.Load() != delegate.returned.Load() || delegate.closeEntered.Load() != 0 || delegate.client.NumberSessionsInProgress() != 0 {
		t.Fatal("shared Runtime Lua not idle", delegate.entered.Load(), delegate.returned.Load(), delegate.client.NumberSessionsInProgress())
	}
}

// replicaLuaRunning checks how many adapter executions are still in progress.
func replicaLuaRunning(t *testing.T, opts replicaLuaRuntimeOptions, want int32) {
	t.Helper()
	if running := opts.delegate.entered.Load() - opts.delegate.returned.Load(); running != want {
		t.Fatal("held Lua executions", running, want)
	}
}

// replicaLuaJoined waits until the adapter has returned from want executions.
func replicaLuaJoined(t *testing.T, ctx context.Context, opts replicaLuaRuntimeOptions, want int32) {
	t.Helper()
	for opts.delegate.returned.Load() != want {
		if ctx.Err() != nil {
			t.Fatal("Lua adapter execution join", ctx.Err(), opts.delegate.returned.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func replicaLuaRuntime(t *testing.T, opts replicaLuaRuntimeOptions) []testmongo.LuaCase {
	t.Helper()
	replicaLuaIdle(t, opts)
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
		ticket, failure := opts.runtime.Submit(ctx, work, opts.runtime.NewSession())
		if failure != nil {
			t.Fatal(failure)
		}
		if err := testmongo.WaitLuaPhase(ctx, watch.SnapshotHeld(0)); err != nil {
			t.Fatal("first real snapshot", err, watch.Snapshot())
		}
		replicaLuaRunning(t, opts, 1)
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
			replicaLuaRunning(t, opts, 1)
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
		replicaLuaJoined(t, ctx, opts, originalExecutions+int32(2*i)+1)
		replicaLuaIdle(t, opts)
		if observed := work.Backend.(*plan).program.ObservedAt.UTC().Format(time.RFC3339Nano); observed != watch.Snapshot().Attempt[0].Observed {
			t.Fatal("original ObservedAt witness", observed, watch.Snapshot())
		}
		if err := watch.Complete(confirmed); err != nil {
			t.Fatal("Lua terminal receipt/join completion", watch.Snapshot())
		}
		c := testmongo.LuaCase{Watch: watch, Confirmed: confirmed}
		cases = append(cases, c)
		opts.proxy.CheckLuaCases(t, cases, "backend_receipt")
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
		newTicket, failure := opts.runtime.Submit(recovery, newPlan, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		newEvent, err := newTicket.Wait(recovery)
		if err != nil || newEvent.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || newEvent.GetMutationResult().GetFailure() != nil || time.Since(usable) > 5*time.Second {
			t.Fatal("same Runtime recovery", err)
		}
		replicaLuaJoined(t, recovery, opts, opts.delegate.entered.Load())
		stop()
		replicaLuaIdle(t, opts)
		cases[i].RecoveryID = newID
		opts.proxy.CheckLuaCases(t, cases, "backend_new_work_recovery")
	}
	if opts.delegate.entered.Load() != originalExecutions+4 {
		t.Fatal("Lua and new-work exact Execute count")
	}
	return cases
}
