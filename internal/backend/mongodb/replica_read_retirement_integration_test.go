//go:build integration

package mongodb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Delegation observes real entry/return only; plans, contexts and Emit are
// forwarded unchanged, including detached cleanup and the real driver.
type replicaReadAdapter struct {
	*Adapter
	entered, returned           atomic.Int32
	closeEntered, closeReturned atomic.Int32
	luaMu                       sync.Mutex
	luaPlans                    map[uint64]*replicaLuaMemory
}

func (a *replicaReadAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) {
	a.entered.Add(1)
	a.executeReplicaPlans(ctx, plans, emit)
	a.returned.Add(1)
}
func (a *replicaReadAdapter) Close(ctx context.Context) error {
	a.closeEntered.Add(1)
	err := a.Adapter.Close(ctx)
	a.closeReturned.Add(1)
	return err
}

type replicaReadRuntimeOptions struct {
	adapter  *Adapter
	fixture  *testmongo.Replica
	proxy    *testmongo.ReplicaProxy
	primary  int
	runtime  *store.Runtime
	delegate *replicaReadAdapter
}

func replicaReadBarrier(t *testing.T, watch *testmongo.ReadWatch, cleanup bool, deadline time.Time) {
	t.Helper()
	barrier := watch.Held()
	if cleanup {
		barrier = watch.CleanupHeld()
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-barrier:
	case <-timer.C:
		t.Fatal("real read reply hold missing", cleanup, watch.Snapshot())
	}
	if s := watch.Snapshot(); s.Error != "" || s.Expired {
		t.Fatal("read observation", s)
	}
}
func replicaReadFacts(t *testing.T, w *testmongo.ReadWatch, phase string) {
	t.Helper()
	s := w.Snapshot()
	if s.FindRequests != 1 || s.FindWrites != 1 || s.FindReplies != 1 || s.FindForwarded != 0 || s.CleanupRequests != 1 || s.CleanupWrites != 1 || s.CleanupReplies != 1 || s.CleanupForwarded != 0 || !s.SessionPresent || !s.Held || !s.CleanupHeld || s.GetMore != 0 || s.KillCursors != 0 || s.Expired || s.Error != "" {
		t.Fatal("persistent sent/unreceived read facts", phase, s)
	}
	t.Logf("sent_read phase=%s unknown_to_Weir_cursor_state=true observation=%+v", phase, s)
}
func replicaReadPeer(t *testing.T, a *Adapter, peer string) *mongoConn {
	t.Helper()
	a.dialer.mu.Lock()
	defer a.dialer.mu.Unlock()
	for conn := range a.dialer.conns {
		if conn.raw.LocalAddr().String() == peer {
			return conn
		}
	}
	t.Fatal("held read raw owner not found", peer)
	return nil
}
func replicaReadRawClosed(a *Adapter, conns []*mongoConn) bool {
	a.dialer.mu.Lock()
	defer a.dialer.mu.Unlock()
	for _, conn := range conns {
		if _, exists := a.dialer.conns[conn]; exists {
			return false
		}
	}
	return a.dialer.closing == 0
}
func replicaReadSubmit(t *testing.T, ctx context.Context, opts replicaReadSubmitOptions) *store.Ticket {
	t.Helper()
	read := &pb.ReadRequest{Resource: opts.database + "/records/s:" + opts.id}
	operation := &pb.Command_Read{Read: read}
	command := &pb.Command{Operation: operation}
	record, err := execution.NewRecord("mongo", opts.index, command)
	if err != nil {
		t.Fatal(err)
	}
	plan, failure := opts.runtime.PrepareRecord(ctx, record)
	if failure != nil {
		t.Fatal(failure)
	}
	ticket, failure, _ := opts.runtime.Submit(ctx, plan, opts.session)
	if failure != nil {
		t.Fatal(failure)
	}
	return ticket
}

type replicaReadSubmitOptions struct {
	runtime      *store.Runtime
	session      *store.Session
	database, id string
	index        uint64
}

func replicaReadRuntime(t *testing.T, opts replicaReadRuntimeOptions) ([]*testmongo.ReadWatch, []*testmongo.LuaWatch) {
	t.Helper()
	luaOptions := replicaLuaRuntimeOptions{runtime: opts.runtime, delegate: opts.delegate, fixture: opts.fixture, proxy: opts.proxy, primary: opts.primary}
	luaWatches := replicaLuaLifecycleCancel(t, luaOptions)
	a := opts.adapter
	baseline := a.client.NumberSessionsInProgress()
	if baseline != 0 {
		t.Fatal("read runtime session baseline", baseline)
	}
	runtime, delegate := opts.runtime, opts.delegate
	executionBaseline := delegate.returned.Load()
	if delegate.entered.Load() != executionBaseline || delegate.closeEntered.Load() != 0 {
		t.Fatal("shared Lua/read execution baseline")
	}
	var err error
	watches := make([]*testmongo.ReadWatch, 0, 2)
	arm := func(id string) *testmongo.ReadWatch {
		watch, err := opts.proxy.WatchRead(id)
		if err != nil {
			t.Fatal(err)
		}
		if err := opts.proxy.ArmReadHold(opts.primary, watch); err != nil {
			t.Fatal(err)
		}
		watches = append(watches, watch)
		return watch
	}
	ordinary := arm("runtime-cancel-read")
	caller, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	submit := replicaReadSubmitOptions{runtime: runtime, database: opts.fixture.DB, id: "runtime-cancel-read", index: 101}
	ticket := replicaReadSubmit(t, caller, submit)
	replicaReadBarrier(t, ordinary, false, time.Now().Add(3*time.Second))
	findConn := replicaReadPeer(t, a, ordinary.Snapshot().FindPeer)
	if a.client.NumberSessionsInProgress() < 1 {
		t.Fatal("held explicit session not checked out")
	}
	cancelled := time.Now()
	cancel()
	replicaReadBarrier(t, ordinary, true, cancelled.Add(3*time.Second))
	cleanupConn := replicaReadPeer(t, a, ordinary.Snapshot().CleanupPeer)
	// Wait is deliberately independent of the canceled submission context: using
	// caller here would Abandon the real retained consumer-owned ticket.
	wait, stop := context.WithDeadline(t.Context(), cancelled.Add(3*time.Second))
	event, err := ticket.Wait(wait)
	stop()
	if err != nil || event.GetReadResult() == nil || event.GetReadResult().GetFailure() == nil || event.GetReadResult().GetDocument() != nil {
		t.Fatal("canceled real read evidence", event, err)
	}
	executeJoinedElapsed := time.Since(cancelled)
	// The driver's raw-socket tail must not mask a shortened execution cleanup.
	if executeJoinedElapsed < 1800*time.Millisecond {
		t.Fatal("ordinary execution cleanup lost its full independent 2s", executeJoinedElapsed)
	}
	ownedReadConns := []*mongoConn{findConn, cleanupConn}
	// The pinned driver can background-read a deadline-timed-out socket for
	// 400ms. Observe its actual raw Close inside the original 3s cancel gate.
	for runtime.Snapshot().Active != 0 || delegate.returned.Load() != executionBaseline+1 || a.client.NumberSessionsInProgress() != baseline || !replicaReadRawClosed(a, ownedReadConns) {
		if time.Now().After(cancelled.Add(3 * time.Second)) {
			t.Fatal("ordinary execution/session/exact raw join", runtime.Snapshot(), a.client.NumberSessionsInProgress(), a.dialer.snapshot(), ordinary.Snapshot())
		}
		time.Sleep(time.Millisecond)
	}
	elapsed := time.Since(cancelled)
	if elapsed < 1800*time.Millisecond || elapsed > 3*time.Second || delegate.closeEntered.Load() != 0 {
		t.Fatal("ordinary cleanup lost its full independent 2s", elapsed)
	}
	replicaReadFacts(t, ordinary, "backend_cancel_join_before_observer_close")
	ticket.Ack()
	if s := runtime.Snapshot(); s.Retained != 0 || s.ResultBytes != 0 || s.WorkingBytes != 0 {
		t.Fatal("ordinary Ack credit", s)
	}
	closeTail, stop := context.WithTimeout(t.Context(), time.Second)
	err = ordinary.CloseHeldRelays(closeTail)
	stop()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("sent_read case=backend_cancel cleanup_and_raw_join_elapsed=%s original_3s_remaining=%s execute_join_observed=%s real_execute_join=true sessions_baseline=%d exact_raw_closed=2 adapter_close_calls=0 snapshot=%+v memory=%+v", elapsed, 3*time.Second-elapsed, executeJoinedElapsed, baseline, runtime.Snapshot(), runtime.ExecutionMemory().Snapshot())

	// A genuine ready read has an actual blocked publisher and remains unacked.
	// Events is the real public Session channel, not an injected product hook.
	readyID := "runtime-ready-read"
	seed := bson.D{{Key: "_id", Value: readyID}, {Key: "n", Value: int32(0)}}
	seedCtx, stop := context.WithTimeout(t.Context(), 3*time.Second)
	_, err = opts.fixture.Admin.Database(opts.fixture.DB).Collection("records").InsertOne(seedCtx, seed)
	stop()
	if err != nil {
		t.Fatal(err)
	}
	session := runtime.NewSession()
	session.Events = make(chan *store.Emission, 1)
	defer session.Close()
	readyCaller, stopReady := context.WithTimeout(t.Context(), 12*time.Second)
	defer stopReady()
	submit.session = session
	submit.id = readyID
	submit.index = 102
	ready := replicaReadSubmit(t, readyCaller, submit)
	wait, stop = context.WithTimeout(t.Context(), 3*time.Second)
	event, err = ready.Wait(wait)
	stop()
	if err != nil || event.GetReadResult().GetDocument() == nil || event.GetReadResult().GetFailure() != nil {
		t.Fatal("genuine ready read", event, err)
	}
	var emission *store.Emission
	select {
	case emission = <-session.Events:
	case <-time.After(time.Second):
		t.Fatal("real ready publisher handoff")
	}
	// Capture the genuine ready owner's input/result usage before its peer is
	// admitted, so Ack can prove exactly whose credit was released.
	readyOwned := runtime.Snapshot()
	readyCredit := runtime.ExecutionMemory().Snapshot()
	if readyOwned.Retained != 1 || readyOwned.Ready != 1 || readyOwned.ResultBytes <= 0 || readyCredit.Count != 1 || readyCredit.InputUsedBytes == 0 {
		t.Fatal("ready owner credit", readyOwned, readyCredit)
	}
	// Never Release this emission until Runtime.Close has joined the publisher.
	lua := replicaLuaLifecycleStart(t, luaOptions, testmongo.LuaRetireCommit)
	if err := lua.watch.ValidateCommitHeld(); err != nil {
		t.Fatal(err)
	}
	luaCharged := runtime.Snapshot()
	luaCredit := runtime.ExecutionMemory().Snapshot()
	if luaCharged.Retained != 2 || luaCharged.Active != 1 || luaCredit.Count != 2 || luaCharged.WorkingBytes <= 0 || !lua.mirror.actualTicket.Load() {
		t.Fatal("retirement Lua real charged Ticket", luaCharged, luaCredit)
	}
	active := arm("runtime-retire-read")
	activeCaller, stopActive := context.WithTimeout(t.Context(), 12*time.Second)
	defer stopActive()
	submit.id = "runtime-retire-read"
	submit.index = 103
	activeTicket := replicaReadSubmit(t, activeCaller, submit)
	replicaReadBarrier(t, active, false, time.Now().Add(3*time.Second))
	before := runtime.Snapshot()
	memory := runtime.ExecutionMemory().Snapshot()
	if before.Active != 2 || before.Retained != 3 || before.Ready != 1 || before.Publishers != 1 || before.ResultBytes <= 0 || memory.Count != 3 || memory.UsedBytes == 0 {
		t.Fatal("real active+ready charged owners", before, memory)
	}
	if activeCaller.Err() != nil || readyCaller.Err() != nil || lua.ctx.Err() != nil {
		t.Fatal("retirement callers expired before close")
	}
	t.Logf("sent_read phase=backend_before_close snapshot=%+v memory=%+v sessions=%d emission_held=true", before, memory, a.client.NumberSessionsInProgress())
	started := time.Now()
	drain, stop := context.WithTimeout(context.Background(), 5*time.Second)
	err = runtime.Close(drain)
	stop()
	elapsed = time.Since(started)
	if err != nil || elapsed < 5*time.Second || elapsed > 8*time.Second {
		t.Fatal("full default drain/retirement actual close", elapsed, err)
	}
	if activeCaller.Err() != nil || readyCaller.Err() != nil || lua.ctx.Err() != nil {
		t.Fatal("original 12s caller expired during Runtime.Close")
	}
	replicaReadBarrier(t, active, true, started.Add(8*time.Second))
	replicaReadFacts(t, active, "backend_actual_runtime_close")
	if s := active.Snapshot(); s.Closed || s.Joined != 0 {
		t.Fatal("observer intervened before actual Runtime.Close", s)
	}
	if delegate.entered.Load() != executionBaseline+4 || delegate.returned.Load() != executionBaseline+4 || delegate.closeEntered.Load() != 1 || delegate.closeReturned.Load() != 1 || a.client.NumberSessionsInProgress() != baseline {
		t.Fatal("actual adapter/execute/session joins", delegate.entered.Load(), delegate.returned.Load(), delegate.closeReturned.Load(), a.client.NumberSessionsInProgress())
	}
	ownerZero(t, a.dialer)
	after := runtime.Snapshot()
	credit := runtime.ExecutionMemory().Snapshot()
	if !after.Closed || after.Active != 0 || after.Pending != 0 || after.WorkingBytes != 0 || after.Publishers != 0 || after.Retained != 3 || after.Ready != 3 || after.ResultBytes <= 0 || credit.Count != 3 || credit.Terminal != 3 || credit.UsedBytes == 0 || credit.InputUsedBytes <= readyCredit.InputUsedBytes || credit.InputReservedBytes == 0 {
		t.Fatal("real postclose retained credit", after, credit)
	}
	wait, stop = context.WithTimeout(t.Context(), time.Second)
	event, err = activeTicket.Wait(wait)
	stop()
	if err != nil || event.GetReadResult() == nil || event.GetReadResult().GetDocument() != nil || event.GetReadResult().GetFailure() == nil {
		t.Fatal("retired read evidence", event, err)
	}
	t.Logf("sent_read phase=backend_after_close elapsed=%s execution_adapter_publisher_join=true sessions_baseline=%d raw_owner=%+v snapshot=%+v memory=%+v", elapsed, baseline, a.dialer.snapshot(), after, credit)
	// Retire the additional Lua owner explicitly before preserving the original
	// ready-read Ack 2->1 and owning Session.Close 1->0 assertions.
	wait, stop = context.WithDeadline(t.Context(), started.Add(8*time.Second))
	luaResult := replicaLuaLifecycleReceipt(t, wait, lua, true)
	if err := lua.watch.WaitLifecycleEOF(wait); err != nil {
		t.Fatal("shutdown Lua EOF observation", err)
	}
	stop()
	if luaResult.GetOutcome() != pb.MutationOutcome_UNKNOWN || luaResult.GetFailure() == nil {
		t.Fatal("retired Lua commit uncertainty", luaResult)
	}
	if err := lua.watch.ValidateLifecycle(false); err != nil {
		t.Fatal("actual shutdown natural Lua EOF", err, lua.watch.Snapshot())
	}
	lua.ticket.Ack()
	withoutLua := runtime.Snapshot()
	withoutLuaCredit := runtime.ExecutionMemory().Snapshot()
	if withoutLua.Retained != 2 || withoutLua.Ready != 2 || withoutLuaCredit.Count != 2 || withoutLuaCredit.Terminal != 2 || withoutLua.ResultBytes <= 0 || withoutLua.ResultBytes >= after.ResultBytes || withoutLuaCredit.InputUsedBytes != credit.InputUsedBytes-(luaCredit.InputUsedBytes-readyCredit.InputUsedBytes) || withoutLuaCredit.InputReservedBytes != credit.InputReservedBytes {
		t.Fatal("Lua actual Ack 3->2 before Session.Close", withoutLua, withoutLuaCredit)
	}
	replicaLuaLifecycleForget(luaOptions, lua)
	joinLua, stop := context.WithDeadline(t.Context(), started.Add(8*time.Second))
	if err := lua.watch.CloseLifecycleTail(joinLua); err != nil {
		t.Fatal(err)
	}
	stop()
	luaWatches = append(luaWatches, lua.watch)
	after, credit = withoutLua, withoutLuaCredit
	// The shared input reservation remains charged while the peer is live.
	// Assert actual per-owner lease/input/result credit, not a fabricated
	// reduction of that shared reservation on the first acknowledgment.
	ready.Ack()
	one := runtime.Snapshot()
	oneCredit := runtime.ExecutionMemory().Snapshot()
	if one.Retained != 1 || one.Ready != 1 || one.ResultBytes <= 0 || one.ResultBytes != after.ResultBytes-readyOwned.ResultBytes || oneCredit.Count != 1 || oneCredit.Terminal != 1 || oneCredit.InputUsedBytes == 0 || oneCredit.InputUsedBytes != credit.InputUsedBytes-readyCredit.InputUsedBytes || oneCredit.InputReservedBytes != credit.InputReservedBytes || oneCredit.UsedBytes == 0 || oneCredit.UsedBytes > credit.UsedBytes {
		t.Fatal("real ready Ack 2->1", one, oneCredit)
	}
	t.Logf("sent_read phase=backend_real_ready_Ack_2_to_1 snapshot=%+v memory=%+v", one, oneCredit)
	session.Close()
	zero := runtime.Snapshot()
	zeroCredit := runtime.ExecutionMemory().Snapshot()
	if zero.Retained != 0 || zero.Ready != 0 || zero.ResultBytes != 0 || zero.WorkingBytes != 0 || zero.Publishers != 0 || zeroCredit.Count != 0 || zeroCredit.UsedBytes != 0 || zeroCredit.InputUsedBytes != 0 || zeroCredit.InputReservedBytes != 0 || zeroCredit.ResidentBytes != 0 || zeroCredit.WireBytes != 0 {
		t.Fatal("real owning Session.Close 1->0", zero, zeroCredit)
	}
	emission.Release()
	t.Logf("sent_read case=backend_retirement real_session_Close_1_to_0=true snapshot=%+v memory=%+v", zero, zeroCredit)
	return watches, luaWatches
}
