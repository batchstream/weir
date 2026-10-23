//go:build integration

package mongodb

import (
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type replicaLuaCall struct {
	ctx    context.Context
	cancel context.CancelFunc
	ticket *store.Ticket
	watch  *testmongo.LuaWatch
}

func replicaLuaLifecycleStart(t *testing.T, opts replicaLuaRuntimeOptions, phase testmongo.LuaLifecyclePhase) *replicaLuaCall {
	t.Helper()
	ids := []string{"runtime-lua-cancel", "runtime-lua-peer", "runtime-lua-retire"}
	op := testmongo.LuaOperation{Namespace: opts.fixture.DB + ".records", ID: ids[phase]}
	watch, err := opts.proxy.WatchLuaLifecycle(op, phase)
	if err != nil {
		t.Fatal(err)
	}
	if err := opts.proxy.ArmLuaConflict(opts.primary, watch); err != nil {
		t.Fatal(err)
	}
	seed, stop := context.WithTimeout(t.Context(), 3*time.Second)
	opts.proxy.SeedLua(t, seed, watch)
	stop()
	input := bson.D{{Key: "attempt", Value: int32(0)}}
	raw, err := bson.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{ContentType: "application/bson", Data: raw}
	lua := &pb.LuaTransform{Source: []byte(testmongo.ReplicaLuaSource), Input: document}
	form := &pb.Transform_Lua{Lua: lua}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutate := &pb.MutateRequest{Resource: opts.fixture.DB + "/records/s:" + op.ID, Action: action}
	operation := &pb.Command_Mutate{Mutate: mutate}
	command := &pb.Command{Operation: operation}
	record, err := execution.NewRecord("mongo", 1001+uint64(phase), command)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	t.Cleanup(cancel)
	work, failure := opts.runtime.PrepareRecord(ctx, record)
	if failure != nil {
		t.Fatal(failure)
	}
	ticket, failure := opts.runtime.Submit(ctx, work, opts.runtime.NewSession())
	if failure != nil {
		t.Fatal(failure)
	}
	call := &replicaLuaCall{ctx: ctx, cancel: cancel, ticket: ticket, watch: watch}
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
	opts.proxy.CheckLuaLedger(t, ctx, watch, expected)
	return call
}
func replicaLuaLifecycleReceipt(t *testing.T, ctx context.Context, call *replicaLuaCall) *pb.MutationResult {
	t.Helper()
	event, err := call.ticket.Wait(ctx)
	if err != nil || event.GetMutationResult() == nil {
		t.Fatal("real lifecycle Ticket.Wait", event, err)
	}
	return event.GetMutationResult()
}
func replicaLuaLifecycleCancel(t *testing.T, opts replicaLuaRuntimeOptions) []*testmongo.LuaWatch {
	t.Helper()
	replicaLuaIdle(t, opts)
	base := opts.delegate.returned.Load()
	canceled := replicaLuaLifecycleStart(t, opts, testmongo.LuaCancelSnapshot)
	replicaLuaRunning(t, opts, 1)
	canceledRaw := replicaReadPeer(t, opts.delegate.Adapter, canceled.watch.Snapshot().FindPeer)
	ownedRaw := []*mongoConn{canceledRaw}
	peer := replicaLuaLifecycleStart(t, opts, testmongo.LuaHealthyPeer)
	if s := peer.watch.Snapshot(); s.Attempt[0].FindForwarded != 0 || s.Attempt[0].CommitRequests != 0 {
		t.Fatal("healthy peer must still be held when cancellation fires", s)
	}
	started := time.Now()
	canceled.cancel()
	wait, stop := context.WithDeadline(t.Context(), started.Add(3*time.Second))
	defer stop()
	if err := peer.watch.ForwardSnapshot(0); err != nil {
		t.Fatal(err)
	}
	result := replicaLuaLifecycleReceipt(t, wait, peer)
	if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil || peer.ctx.Err() != nil {
		t.Fatal("independent concurrent Lua peer survived original cancellation", result, peer.ctx.Err())
	}
	peer.cancel()
	result = replicaLuaLifecycleReceipt(t, wait, canceled)
	if result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_CANCELLED {
		t.Fatal("canceled snapshot truthful result", result)
	}
	for opts.delegate.returned.Load() != base+2 || opts.delegate.client.NumberSessionsInProgress() != 0 || !replicaReadRawClosed(opts.delegate.Adapter, ownedRaw) {
		if wait.Err() != nil {
			t.Fatal("Lua cancellation actual execution/session join", wait.Err())
		}
		time.Sleep(time.Millisecond)
	}
	if err := canceled.watch.WaitLifecycleEOF(wait); err != nil {
		t.Fatal("canceled Lua EOF observation", err)
	}
	if err := canceled.watch.ValidateLifecycle(false); err != nil {
		t.Fatal("natural cancellation EOF/no replay", err, canceled.watch.Snapshot())
	}
	replicaLuaIdle(t, opts)
	if err := canceled.watch.CloseLifecycleTail(wait); err != nil {
		t.Fatal(err)
	}
	watches := []*testmongo.LuaWatch{canceled.watch, peer.watch}
	opts.proxy.CheckLuaLifecycleCasesContext(t, wait, watches, "backend_cancel_join")
	if elapsed := time.Since(started); elapsed > 3*time.Second || testmongo.CheckLuaLifecycleDeadline(wait) != nil {
		t.Fatal("Lua cancellation final join and majority ledgers exceeded original 3s", elapsed, wait.Err())
	}
	t.Logf("lua_lifecycle cancellation_elapsed=%s", time.Since(started))
	return watches
}
