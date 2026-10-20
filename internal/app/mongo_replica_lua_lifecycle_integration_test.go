//go:build integration

package app

import (
	"context"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type replicaCLILuaCall struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan replicaCLILuaReceipt
	joined chan struct{}
	watch  *testmongo.LuaWatch
}

func replicaCLILuaLifecycleStart(t *testing.T, lifetime *replyLossLifetime, phase testmongo.LuaLifecyclePhase) *replicaCLILuaCall {
	t.Helper()
	ids := []string{"cli-lua-cancel", "cli-lua-peer", "cli-lua-retire"}
	op := testmongo.LuaOperation{Namespace: lifetime.fixture.DB + ".records", ID: ids[phase]}
	watch, err := lifetime.proxy.WatchLuaLifecycle(op, phase)
	if err != nil {
		t.Fatal(err)
	}
	if err := lifetime.proxy.ArmLuaConflict(lifetime.primary, watch); err != nil {
		t.Fatal(err)
	}
	seed, stop := context.WithTimeout(t.Context(), 3*time.Second)
	lifetime.proxy.SeedLua(t, seed, watch)
	stop()
	input := bson.D{{Key: "attempt", Value: int32(0)}}
	raw, err := bson.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{ContentType: "application/bson", Data: raw}
	lua := &weirclient.LuaTransform{Source: []byte(testmongo.ReplicaLuaSource), Input: document}
	request := &weirclient.AtomicTransformRequest{Resource: lifetime.fixture.DB + "/records/s:" + op.ID, Lua: lua}
	opts := weirclient.AtomicTransformOptions{StoreName: "mongo", Request: request}
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	call := &replicaCLILuaCall{ctx: ctx, cancel: cancel, done: make(chan replicaCLILuaReceipt, 1), joined: make(chan struct{}), watch: watch}
	t.Cleanup(func() {
		cancel()
		select {
		case <-call.joined:
		case <-time.After(8 * time.Second):
			t.Error("Lua lifecycle SDK cleanup did not join")
		}
	})
	go func() {
		defer close(call.joined)
		result, err := lifetime.client.AtomicTransform(ctx, opts)
		receipt := replicaCLILuaReceipt{result: result, err: err}
		call.done <- receipt
	}()
	if err := testmongo.WaitLuaPhase(ctx, watch.SnapshotHeld(0)); err != nil {
		t.Fatal(err)
	}
	if phase == testmongo.LuaRetireCommit {
		if err := watch.ForwardSnapshot(0); err != nil {
			t.Fatal(err)
		}
		if err := testmongo.WaitLuaPhase(ctx, watch.CommitHeld()); err != nil {
			t.Fatal(err)
		}
	}
	expected := testmongo.LuaLedger{}
	if phase == testmongo.LuaRetireCommit {
		expected.N = 1
		expected.Candidate = 1
	}
	lifetime.proxy.CheckLuaLedger(t, ctx, watch, expected)
	if phase != testmongo.LuaHealthyPeer {
		metrics := testmetrics.Scrape(t, lifetime.service.diagnostic)
		for _, name := range []string{"weir_store_active_executions", "weir_store_working_reserved_bytes", "weir_store_result_reserved_entries", "weir_execution_memory_input_used_bytes", "weir_execution_memory_leases"} {
			if metrics[name] == nil || testmetrics.Sum(metrics, name) <= 0 {
				t.Fatal("held Lua lifecycle actual positive credit", phase, name)
			}
		}
	}
	return call
}
func replicaCLILuaLifecycleReceipt(t *testing.T, ctx context.Context, call *replicaCLILuaCall) replicaCLILuaReceipt {
	t.Helper()
	var receipt replicaCLILuaReceipt
	select {
	case receipt = <-call.done:
	case <-ctx.Done():
		t.Fatal("Lua lifecycle SDK receipt", ctx.Err())
	}
	if err := testmongo.WaitLuaPhase(ctx, call.joined); err != nil {
		t.Fatal("Lua lifecycle actual SDK join", err)
	}
	return receipt
}
func replicaCLILuaLifecycleCancel(t *testing.T, lifetime *replyLossLifetime) []*testmongo.LuaWatch {
	t.Helper()
	replicaCLIIdle(t, lifetime.service, time.Now().Add(3*time.Second))
	canceled := replicaCLILuaLifecycleStart(t, lifetime, testmongo.LuaCancelSnapshot)
	replyLossGauges(t, lifetime, "Lua_cancel_snapshot_real_credit", true)
	peer := replicaCLILuaLifecycleStart(t, lifetime, testmongo.LuaHealthyPeer)
	if s := peer.watch.Snapshot(); s.Attempt[0].FindForwarded != 0 || s.Attempt[0].CommitRequests != 0 {
		t.Fatal("CLI healthy peer must still be held when cancellation fires", s)
	}
	started := time.Now()
	canceled.cancel()
	wait, stop := context.WithDeadline(t.Context(), started.Add(3*time.Second))
	defer stop()
	if err := peer.watch.ForwardSnapshot(0); err != nil {
		t.Fatal(err)
	}
	receipt := replicaCLILuaLifecycleReceipt(t, wait, peer)
	if receipt.err != nil || receipt.result == nil || receipt.result.Outcome != weirclient.MutationApplied || receipt.result.Failure != nil || peer.ctx.Err() != nil {
		t.Fatal("separate concurrent healthy Lua peer survived original cancellation", receipt, peer.ctx.Err())
	}
	peer.cancel()
	receipt = replicaCLILuaLifecycleReceipt(t, wait, canceled)
	if receipt.result != nil && (receipt.result.Outcome != weirclient.MutationNotApplied || receipt.result.GetFailure().GetCode() != pb.FailureCode_CANCELLED) || receipt.result == nil && receipt.err == nil {
		t.Fatal("canceled Lua truthful transport/typed boundary", receipt)
	}
	replicaCLIIdle(t, lifetime.service, started.Add(3*time.Second))
	if err := canceled.watch.WaitLifecycleEOF(wait); err != nil {
		t.Fatal("CLI canceled Lua EOF observation", err)
	}
	if err := canceled.watch.ValidateLifecycle(false); err != nil {
		t.Fatal("CLI canceled Lua natural retirement", err, canceled.watch.Snapshot())
	}
	if err := canceled.watch.CloseLifecycleTail(wait); err != nil {
		t.Fatal(err)
	}
	watches := []*testmongo.LuaWatch{canceled.watch, peer.watch}
	lifetime.proxy.CheckLuaLifecycleCasesContext(t, wait, watches, "CLI_cancel_join")
	if elapsed := time.Since(started); elapsed > 3*time.Second || testmongo.CheckLuaLifecycleDeadline(wait) != nil {
		t.Fatal("CLI Lua cancellation final join and majority ledgers exceeded original 3s", elapsed, wait.Err())
	}
	t.Logf("lua_lifecycle CLI_cancel_elapsed=%s actual_SDK_join=true peer_APPLIED=true separate_concurrent_transactions=true coalesced_batch_coverage=false typed_receipt=%t transport_error=%v", time.Since(started), receipt.result != nil, receipt.err)
	return watches
}
func replicaCLILuaRetired(t *testing.T, call *replicaCLILuaCall, retireDeadline time.Time) {
	t.Helper()
	if call.ctx.Err() != nil {
		t.Fatal("original Lua 12s caller expired during actual Wait", call.ctx.Err())
	}
	wait, stop := context.WithDeadline(t.Context(), retireDeadline)
	defer stop()
	receipt := replicaCLILuaLifecycleReceipt(t, wait, call)
	if receipt.result != nil {
		if receipt.result.Outcome != weirclient.MutationUnknown || receipt.result.Failure == nil {
			t.Fatal("retired Lua must preserve commit uncertainty", receipt.result)
		}
		t.Logf("lua_lifecycle CLI_actual_Wait delivered_typed_UNKNOWN=true failure=%v", receipt.result.Failure)
	} else {
		if receipt.err == nil {
			t.Fatal("Lua shutdown missing typed receipt and transport error")
		}
		t.Logf("lua_lifecycle CLI_actual_Wait delivered_typed_UNKNOWN=false transport_interruption=%v", receipt.err)
	}
	if err := call.watch.WaitLifecycleEOF(wait); err != nil {
		t.Fatal("CLI shutdown Lua EOF observation", err)
	}
	if err := call.watch.ValidateLifecycle(false); err != nil {
		t.Fatal("Lua natural product EOF after actual Wait", err, call.watch.Snapshot())
	}
	if err := call.watch.CloseLifecycleTail(wait); err != nil {
		t.Fatal(err)
	}
	if err := testmongo.CheckLuaLifecycleDeadline(wait); err != nil {
		t.Fatal("CLI Lua SDK/EOF/relay joins exceeded original shutdown 8s", err)
	}
	if call.ctx.Err() != nil {
		t.Fatal("original Lua 12s caller expired before shutdown joins", call.ctx.Err())
	}
	call.cancel()
}
