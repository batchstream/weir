//go:build integration

package app

import (
	"context"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func replicaCLILuaCommit(t *testing.T, lifetime *replyLossLifetime, abortCases []testmongo.LuaCase) []testmongo.LuaCommitCase {
	t.Helper()
	cases := make([]testmongo.LuaCommitCase, 0, 2)
	for _, all := range []bool{false, true} {
		id := "cli-lua-commit-once"
		if all {
			id = "cli-lua-commit-all"
		}
		op := testmongo.LuaOperation{Namespace: lifetime.fixture.DB + ".records", ID: id}
		watch, err := lifetime.proxy.WatchLuaCommit(op, all, backend.DefaultOptions().RetryAttempts)
		if err != nil {
			t.Fatal(err)
		}
		if err := lifetime.proxy.ArmLuaConflict(lifetime.primary, watch); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
		defer cancel()
		lifetime.proxy.SeedLua(t, ctx, watch)
		input := bson.D{{Key: "attempt", Value: int32(0)}}
		raw, err := bson.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		document := &pb.Document{ContentType: "application/bson", Data: raw}
		lua := &weirclient.LuaTransform{Source: []byte(testmongo.ReplicaLuaSource), Input: document}
		request := &weirclient.AtomicTransformRequest{Resource: lifetime.fixture.DB + "/records/s:" + id, Lua: lua}
		opts := weirclient.AtomicTransformOptions{StoreName: "mongo", Request: request}
		done := make(chan replicaCLILuaReceipt, 1)
		joined := make(chan struct{})
		defer func() {
			cancel()
			select {
			case <-joined:
			case <-time.After(8 * time.Second):
				t.Error("CLI Lua SDK call did not join during cleanup")
			}
		}()
		go func() {
			defer close(joined)
			result, err := lifetime.client.AtomicTransform(ctx, opts)
			receipt := replicaCLILuaReceipt{result: result, err: err}
			done <- receipt
		}()
		if err := testmongo.WaitLuaPhase(ctx, watch.SnapshotHeld(0)); err != nil {
			t.Fatal("CLI first real snapshot", err, watch.Snapshot())
		}
		if err := watch.ForwardSnapshot(0); err != nil {
			t.Fatal(err)
		}
		if err := testmongo.WaitLuaPhase(ctx, watch.CommitHeld()); err != nil {
			t.Fatal("CLI first successful real commit", err, watch.Snapshot())
		}
		if err := watch.ValidateCommitHeld(); err != nil {
			t.Fatal("CLI held commit before durable barrier", watch.Snapshot())
		}
		expected := testmongo.LuaLedger{N: 1, Candidate: 1}
		lifetime.proxy.CheckLuaLedger(t, ctx, watch, expected)
		select {
		case <-done:
			t.Fatal("CLI returned before actual successful commit drop")
		default:
		}
		if err := watch.DropFirstCommitReply(); err != nil {
			t.Fatal(err)
		}
		var receipt replicaCLILuaReceipt
		select {
		case receipt = <-done:
		case <-ctx.Done():
			t.Fatal("original CLI Lua receipt budget", ctx.Err(), watch.Snapshot())
		}
		result := receipt.result
		if receipt.err != nil || result == nil || ctx.Err() != nil {
			t.Fatal("actual live CLI Lua receipt", receipt.err, ctx.Err())
		}
		if !all {
			if result.Outcome != weirclient.MutationApplied || result.Failure != nil {
				t.Fatal("CLI original commit confirmed", result)
			}
		} else {
			if result.Outcome != weirclient.MutationUnknown || result.GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE {
				t.Fatal("CLI original commit unknown", result)
			}
		}
		// The returned receipt, actual SDK call join and product idle-credit
		// observation, not wire success, end this watch's active ownership.
		if err := testmongo.WaitLuaPhase(ctx, joined); err != nil {
			t.Fatal("CLI Lua actual SDK call join", err)
		}
		cancel()
		replicaCLIIdle(t, lifetime.service, time.Now().Add(3*time.Second))
		if err := watch.CompleteCommit(); err != nil {
			t.Fatal("CLI Lua terminal receipt/join completion", watch.Snapshot())
		}
		c := testmongo.LuaCommitCase{Watch: watch}
		cases = append(cases, c)
		lifetime.proxy.CheckLuaCommitCases(t, cases, "CLI_actual_receipt")
		lifetime.proxy.CheckLuaCases(t, abortCases, "commit_receipt_old_abort_unchanged")
		replyLossOracles(t, lifetime, "Lua_receipt_old_mutations_unchanged")
		external, stop := context.WithTimeout(t.Context(), 3*time.Second)
		_, usable, err := lifetime.fixture.WaitUsable(external, -1)
		stop()
		if err != nil {
			t.Fatal(err)
		}
		recovery, stop := context.WithDeadline(t.Context(), usable.Add(5*time.Second))
		newID := id + "-new-work"
		replyLossNewWork(t, recovery, lifetime, newID)
		if time.Since(usable) > 5*time.Second || lifetime.service.command.Process.Pid != lifetime.pid {
			t.Fatal("same PID/SDK new-ID Lua recovery")
		}
		stop()
		cases[len(cases)-1].RecoveryID = newID
		replicaCLIIdle(t, lifetime.service, time.Now().Add(3*time.Second))
		lifetime.proxy.CheckLuaCommitCases(t, cases, "CLI_same_PID_new_work_recovery")
		lifetime.proxy.CheckLuaCases(t, abortCases, "commit_recovery_old_abort_unchanged")
		replyLossOracles(t, lifetime, "Lua_recovery_old_mutations_unchanged")
	}
	return cases
}
