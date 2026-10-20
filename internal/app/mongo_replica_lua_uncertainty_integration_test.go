//go:build integration

package app

import (
	"context"
	"strings"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type replicaCLILuaReceipt struct {
	result *weirclient.MutationResult
	err    error
}

// These are stable uncertainty cases in the already-running typed CLI. Native
// byte/error controls and the later actual live-read SIGTERM remain unchanged.
func replicaCLILua(t *testing.T, lifetime *replyLossLifetime) []testmongo.LuaCase {
	t.Helper()
	cases := make([]testmongo.LuaCase, 0, 2)
	for _, confirmed := range []bool{true, false} {
		id := "cli-lua-confirmed"
		if !confirmed {
			id = "cli-lua-lost-abort"
		}
		op := testmongo.LuaOperation{Namespace: lifetime.fixture.DB + ".records", ID: id}
		watch, err := lifetime.proxy.WatchLua(op)
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
		lifetime.proxy.ExternalLuaUpdate(t, ctx, watch)
		if err := watch.ForwardSnapshot(0); err != nil {
			t.Fatal(err)
		}
		if err := testmongo.WaitLuaPhase(ctx, watch.AbortHeld()); err != nil {
			t.Fatal("CLI real conflict/abort reply", err, watch.Snapshot())
		}
		if err := watch.ValidateAbortHeld(); err != nil {
			t.Fatal("CLI held abort no-rebuild barrier", watch.Snapshot())
		}
		expected := testmongo.LuaLedger{N: 20, External: 1}
		lifetime.proxy.CheckLuaLedger(t, ctx, watch, expected)
		select {
		case <-done:
			t.Fatal("CLI returned before actual abort release")
		default:
		}
		if confirmed {
			if err := watch.ForwardAbortReply(); err != nil {
				t.Fatal(err)
			}
			if err := testmongo.WaitLuaPhase(ctx, watch.SnapshotHeld(1)); err != nil {
				t.Fatal("CLI fresh snapshot", err, watch.Snapshot())
			}
			if err := watch.ForwardSnapshot(1); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := watch.DropAbortReply(); err != nil {
				t.Fatal(err)
			}
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
		if confirmed {
			if result.Outcome != weirclient.MutationApplied || result.Failure != nil {
				t.Fatal("CLI confirmed abort fresh retry", result)
			}
		} else {
			if result.Outcome != weirclient.MutationNotApplied || result.GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE || !strings.Contains(result.GetFailure().GetMessage(), "rollback unconfirmed") {
				t.Fatal("CLI lost abort no-rebuild", result)
			}
		}
		// The returned receipt, actual SDK call join and product idle-credit
		// observation, not wire success, end this watch's active ownership.
		if err := testmongo.WaitLuaPhase(ctx, joined); err != nil {
			t.Fatal("CLI Lua actual SDK call join", err)
		}
		cancel()
		replicaCLIIdle(t, lifetime.service, time.Now().Add(3*time.Second))
		if err := watch.Complete(confirmed); err != nil {
			t.Fatal("CLI Lua terminal receipt/join completion", watch.Snapshot())
		}
		c := testmongo.LuaCase{Watch: watch, Confirmed: confirmed}
		cases = append(cases, c)
		lifetime.proxy.CheckLuaCases(t, cases, "CLI_actual_receipt")
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
		lifetime.proxy.CheckLuaCases(t, cases, "CLI_same_PID_new_work_recovery")
		replyLossOracles(t, lifetime, "Lua_recovery_old_mutations_unchanged")
	}
	return cases
}
