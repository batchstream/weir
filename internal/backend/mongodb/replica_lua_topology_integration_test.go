//go:build integration

package mongodb

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Two independent transactions share one topology action, not a transaction or
// a Runtime owner. Both actual terminal replies are held before that action.
type replicaLuaTopology struct {
	opts       replicaLuaRuntimeOptions
	transition string
	entered    int32
	calls      [2]replicaLuaTopologyCall
}
type replicaLuaTopologyCall struct {
	ctx    context.Context
	cancel context.CancelFunc
	watch  *testmongo.LuaWatch
	work   *execution.Plan
	ticket *store.Ticket
}

func replicaLuaTopologyBegin(t *testing.T, opts replicaLuaRuntimeOptions, transition string) *replicaLuaTopology {
	t.Helper()
	replicaLuaIdle(t, opts)
	state := &replicaLuaTopology{opts: opts, transition: transition, entered: opts.delegate.entered.Load()}
	for i := range state.calls {
		c := &state.calls[i]
		id := "runtime-lua-topology-" + transition + "-commit"
		if i == 1 {
			id = "runtime-lua-topology-" + transition + "-abort"
		}
		op := testmongo.LuaOperation{Namespace: opts.fixture.DB + ".records", ID: id}
		var err error
		if i == 0 {
			c.watch, err = opts.proxy.WatchLuaCommit(op, false, backend.DefaultOptions().RetryAttempts)
		} else {
			c.watch, err = opts.proxy.WatchLua(op)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := opts.proxy.ArmLuaConflict(opts.primary, c.watch); err != nil {
			t.Fatal(err)
		}
		c.ctx, c.cancel = context.WithTimeout(t.Context(), 12*time.Second)
		t.Cleanup(c.cancel)
		opts.proxy.SeedLua(t, c.ctx, c.watch)
		input := bson.D{{Key: "attempt", Value: int32(0)}}
		raw, err := bson.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		document := &pb.Document{ContentType: "application/bson", Data: raw}
		lua := &pb.LuaTransform{Source: []byte(testmongo.ReplicaLuaSource), Input: document}
		form := &pb.Transform_Lua{Lua: lua}
		transform := &pb.Transform{Form: form}
		atomicTransform := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
		mutate := &pb.MutateRequest{Resource: opts.fixture.DB + "/records/s:" + id, Action: atomicTransform}
		operation := &pb.Command_Mutate{Mutate: mutate}
		command := &pb.Command{Operation: operation}
		record, err := execution.NewRecord("mongo", uint64(901+i), command)
		if err != nil {
			t.Fatal(err)
		}
		var failure *pb.Failure
		c.work, failure = opts.runtime.PrepareRecord(c.ctx, record)
		if failure != nil {
			t.Fatal(failure)
		}
		c.ticket, failure = opts.runtime.Submit(c.ctx, c.work, opts.runtime.NewSession())
		if failure != nil {
			t.Fatal(failure)
		}
		if err := testmongo.WaitLuaPhase(c.ctx, c.watch.SnapshotHeld(0)); err != nil {
			t.Fatal("topology actual snapshot", err, c.watch.Snapshot())
		}
		if i == 1 {
			opts.proxy.ExternalLuaUpdate(t, c.ctx, c.watch)
		}
		if err := c.watch.ForwardSnapshot(0); err != nil {
			t.Fatal(err)
		}
		held := c.watch.CommitHeld
		if i == 1 {
			held = c.watch.AbortHeld
		}
		if err := testmongo.WaitLuaPhase(c.ctx, held()); err != nil {
			t.Fatal("topology actual terminal reply", err, c.watch.Snapshot())
		}
		if i == 0 {
			if err := c.watch.ValidateCommitHeld(); err != nil {
				t.Fatal(err, c.watch.Snapshot())
			}
			expected := testmongo.LuaLedger{N: 1, Candidate: 1}
			opts.proxy.CheckLuaLedger(t, c.ctx, c.watch, expected)
		} else {
			if err := c.watch.ValidateAbortHeld(); err != nil {
				t.Fatal(err, c.watch.Snapshot())
			}
			expected := testmongo.LuaLedger{N: 20, External: 1}
			opts.proxy.CheckLuaLedger(t, c.ctx, c.watch, expected)
		}
	}
	state.held(t)
	return state
}

func replicaLuaTopologyRecovery(t *testing.T, ctx context.Context, opts replicaLuaRuntimeOptions, transition string) string {
	t.Helper()
	id := "runtime-lua-topology-" + transition + "-new-work"
	prepared := putPlan(t, opts.delegate.Adapter, opts.fixture.DB, id)
	record, err := execution.NewRecord("mongo", 999, prepared.Command)
	if err != nil {
		t.Fatal(err)
	}
	work, failure := opts.runtime.PrepareRecord(ctx, record)
	if failure != nil {
		t.Fatal(failure)
	}
	ticket, failure := opts.runtime.Submit(ctx, work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	event, err := ticket.Wait(ctx)
	if err != nil || ctx.Err() != nil || event.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || event.GetMutationResult().GetFailure() != nil {
		t.Fatal("same Runtime topology recovery", err, event)
	}
	replicaLuaJoined(t, ctx, opts, opts.delegate.entered.Load())
	replicaLuaIdle(t, opts)
	return id
}

func (s *replicaLuaTopology) held(t *testing.T) {
	t.Helper()
	for i := range s.calls {
		if err := s.calls[i].ctx.Err(); err != nil {
			t.Fatal("live original topology caller", err)
		}
	}
	if s.opts.delegate.returned.Load() != s.entered {
		t.Fatal("topology execution returned while its reply was held")
	}
	replicaLuaRunning(t, s.opts, 2)
}

func (s *replicaLuaTopology) finish(t *testing.T) (testmongo.LuaCase, testmongo.LuaCommitCase) {
	t.Helper()
	// Topology action is already complete. Gate release is followed by actual
	// socket-drop accounting, checked again by Complete/CompleteCommit below.
	s.held(t)
	if err := s.calls[0].watch.DropFirstCommitReply(); err != nil {
		t.Fatal(err)
	}
	if err := s.calls[1].watch.DropAbortReply(); err != nil {
		t.Fatal(err)
	}
	for i := range s.calls {
		c := &s.calls[i]
		event, err := c.ticket.Wait(c.ctx)
		if err != nil || c.ctx.Err() != nil || event.GetMutationResult() == nil {
			t.Fatal("original topology caller receipt", err, c.ctx.Err(), c.watch.Snapshot())
		}
		result := event.GetMutationResult()
		if i == 1 {
			if result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE || !strings.Contains(result.GetFailure().GetMessage(), "rollback unconfirmed") {
				t.Fatal("topology abort no rebuild", result)
			}
		} else {
			if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
				t.Fatal("topology original commit confirmation", result, c.watch.Snapshot())
			}
		}
	}
	replicaLuaJoined(t, s.calls[0].ctx, s.opts, s.entered+2)
	replicaLuaIdle(t, s.opts)
	for i := range s.calls {
		c := &s.calls[i]
		if c.work.Backend.(*plan).program.ObservedAt.UTC().Format(time.RFC3339Nano) != c.watch.Snapshot().Attempt[0].Observed {
			t.Fatal("topology original execution witness")
		}
		c.cancel()
	}
	if err := s.calls[0].watch.CompleteCommit(); err != nil {
		t.Fatal("topology commit actual drop/confirmation accounting", err, s.calls[0].watch.Snapshot())
	}
	if err := s.calls[1].watch.Complete(false); err != nil {
		t.Fatal("topology abort actual drop accounting", err, s.calls[1].watch.Snapshot())
	}
	abort := testmongo.LuaCase{Watch: s.calls[1].watch}
	commit := testmongo.LuaCommitCase{Watch: s.calls[0].watch}
	t.Logf("Lua topology=%s", s.transition)
	return abort, commit
}
