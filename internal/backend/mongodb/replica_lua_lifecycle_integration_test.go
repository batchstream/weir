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
	ctx     context.Context
	cancel  context.CancelFunc
	ticket  *store.Ticket
	session *store.Session
	mirror  *replicaLuaMemory
	watch   *testmongo.LuaWatch
	id      uint64
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
	mirror := &replicaLuaMemory{command: work.Command, native: work.Backend.(*plan), operation: op}
	opts.delegate.luaMu.Lock()
	opts.delegate.luaPlans[work.ID] = mirror
	opts.delegate.luaMu.Unlock()
	session := opts.runtime.NewSession()
	t.Cleanup(session.Close)
	session.Events = make(chan *store.Emission, 1)
	ticket, failure, _ := opts.runtime.Submit(ctx, work, session)
	if failure != nil {
		t.Fatal(failure)
	}
	call := &replicaLuaCall{ctx: ctx, cancel: cancel, ticket: ticket, session: session, mirror: mirror, watch: watch, id: work.ID}
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
func replicaLuaLifecycleReceipt(t *testing.T, ctx context.Context, call *replicaLuaCall, terminalOnly bool) *pb.MutationResult {
	t.Helper()
	event, err := call.ticket.Wait(ctx)
	if err != nil || event.GetMutationResult() == nil {
		t.Fatal("real lifecycle Ticket.Wait", event, err)
	}
	// A canceled caller or Runtime.Close may cancel/join the real publisher
	// without a Session emission. Its retained Ticket result remains readable;
	// actual publisher joins are checked separately, never manufactured here.
	if terminalOnly {
		return event.GetMutationResult()
	}
	select {
	case emission := <-call.session.Events:
		emission.Release()
	case <-ctx.Done():
		t.Fatal("real lifecycle publisher handoff", ctx.Err())
	}
	return event.GetMutationResult()
}
func replicaLuaLifecycleForget(opts replicaLuaRuntimeOptions, call *replicaLuaCall) {
	call.session.Close()
	call.cancel()
	opts.delegate.luaMu.Lock()
	delete(opts.delegate.luaPlans, call.id)
	opts.delegate.luaMu.Unlock()
}
func replicaLuaLifecycleCancel(t *testing.T, opts replicaLuaRuntimeOptions) []*testmongo.LuaWatch {
	t.Helper()
	replicaLuaZero(t, opts)
	base := opts.delegate.returned.Load()
	canceled := replicaLuaLifecycleStart(t, opts, testmongo.LuaCancelSnapshot)
	replicaLuaWorking(t, opts, canceled.mirror)
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
	result := replicaLuaLifecycleReceipt(t, wait, peer, false)
	if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil || peer.ctx.Err() != nil {
		t.Fatal("independent concurrent Lua peer survived original cancellation", result, peer.ctx.Err())
	}
	peer.ticket.Ack()
	replicaLuaLifecycleForget(opts, peer)
	result = replicaLuaLifecycleReceipt(t, wait, canceled, true)
	if result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_CANCELLED {
		t.Fatal("canceled snapshot truthful result", result)
	}
	for opts.runtime.Snapshot().Active != 0 || opts.runtime.Snapshot().Publishers != 0 || opts.delegate.returned.Load() != base+2 || opts.delegate.client.NumberSessionsInProgress() != 0 || !replicaReadRawClosed(opts.delegate.Adapter, ownedRaw) {
		if wait.Err() != nil {
			t.Fatal("Lua cancellation actual execution/session join", wait.Err())
		}
		time.Sleep(time.Millisecond)
	}
	held := opts.runtime.Snapshot()
	credit := opts.runtime.ExecutionMemory().Snapshot()
	if held.Retained != 1 || held.Ready != 1 || held.WorkingBytes != 0 || held.ResultBytes <= 0 || credit.Count != 1 || credit.InputUsedBytes == 0 || !canceled.mirror.actualTicket.Load() {
		t.Fatal("canceled Lua actual retained Ticket", held, credit)
	}
	if err := canceled.watch.ValidateLifecycle(false); err != nil {
		t.Fatal("natural cancellation EOF/no replay", err, canceled.watch.Snapshot())
	}
	canceled.ticket.Ack()
	replicaLuaZero(t, opts)
	replicaLuaLifecycleForget(opts, canceled)
	if err := canceled.watch.CloseLifecycleTail(wait); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second || wait.Err() != nil {
		t.Fatal("Lua cancellation final join exceeded original 3s", elapsed, wait.Err())
	}
	t.Logf("lua_lifecycle cancellation_elapsed=%s actual_execution_join=true Ack_before_SessionClose=true peer_APPLIED=true separate_concurrent_transactions=true coalesced_batch_coverage=false", time.Since(started))
	watches := []*testmongo.LuaWatch{canceled.watch, peer.watch}
	opts.proxy.CheckLuaLifecycleCases(t, watches, "backend_cancel_join")
	return watches
}
