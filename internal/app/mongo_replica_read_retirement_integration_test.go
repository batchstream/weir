//go:build integration

package app

import (
	"context"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir/internal/testutil/testmongo"
)

type replicaReadReceipt struct {
	err               error
	failure, document bool
}
type replicaCLIRead struct {
	ctx      context.Context
	cancel   context.CancelFunc
	deadline time.Time
	done     chan replicaReadReceipt
	watch    *testmongo.ReadWatch
}

func replicaCLIReadArm(t *testing.T, lifetime *replyLossLifetime, id string) *testmongo.ReadWatch {
	t.Helper()
	watch, err := lifetime.proxy.WatchRead(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := lifetime.proxy.ArmReadHold(lifetime.primary, watch); err != nil {
		t.Fatal(err)
	}
	return watch
}
func replicaCLIReadStart(t *testing.T, lifetime *replyLossLifetime, watch *testmongo.ReadWatch, id string) *replicaCLIRead {
	t.Helper()
	// Preparation is outside the original 12s caller clock; never reset it.
	request := &weirclient.ReadRequest{Resource: lifetime.fixture.DB + "/records/s:" + id}
	opts := weirclient.ReadOneOptions{StoreName: "mongo", Request: request}
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	deadline, _ := ctx.Deadline()
	call := &replicaCLIRead{ctx: ctx, cancel: cancel, deadline: deadline, done: make(chan replicaReadReceipt, 1), watch: watch}
	go func() {
		result, err := lifetime.client.ReadOne(ctx, opts)
		receipt := replicaReadReceipt{err: err}
		if result != nil {
			receipt.failure = result.Failure != nil
			receipt.document = result.Document != nil
		}
		call.done <- receipt
	}()
	return call
}
func replicaCLIReadBarrier(t *testing.T, watch *testmongo.ReadWatch, cleanup bool, deadline time.Time) {
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
		t.Fatal("CLI real read reply hold missing", cleanup, watch.Snapshot())
	}
	if s := watch.Snapshot(); s.Error != "" || s.Expired {
		t.Fatal("CLI read observation", s)
	}
}
func replicaCLIReadFacts(t *testing.T, w *testmongo.ReadWatch, phase string) {
	t.Helper()
	s := w.Snapshot()
	if s.FindRequests != 1 || s.FindWrites != 1 || s.FindReplies != 1 || s.FindForwarded != 0 || s.CleanupRequests != 1 || s.CleanupWrites != 1 || s.CleanupReplies != 1 || s.CleanupForwarded != 0 || !s.SessionPresent || !s.Held || !s.CleanupHeld || s.GetMore != 0 || s.KillCursors != 0 || s.Expired || s.Error != "" {
		t.Fatal("persistent CLI sent/unreceived read facts", phase, s)
	}
	t.Logf("sent_read phase=%s unknown_to_Weir_cursor_state=true observation=%+v", phase, s)
}
func replicaCLIReadPending(t *testing.T, call *replicaCLIRead, remaining time.Duration) {
	t.Helper()
	if call.ctx.Err() != nil || time.Until(call.deadline) < remaining {
		t.Fatal("original live read deadline insufficient", call.ctx.Err(), time.Until(call.deadline))
	}
	select {
	case receipt := <-call.done:
		t.Fatal("caller returned before actual SIGTERM", receipt)
	default:
	}
	s := call.watch.Snapshot()
	if s.FindRequests != 1 || s.FindWrites != 1 || s.FindReplies != 1 || !s.Held || s.CleanupRequests != 0 || s.CleanupHeld || s.Closed || s.Expired || s.Error != "" {
		t.Fatal("early cleanup/observer intervention before SIGTERM", s)
	}
}
func replicaCLISentReadRetirement(t *testing.T, lifetime *replyLossLifetime) (processStopResult, []*testmongo.ReadWatch, []*testmongo.LuaWatch) {
	t.Helper()
	replicaCLIIdle(t, lifetime.service, time.Now().Add(3*time.Second))
	replyLossGauges(t, lifetime, "read_idle_baseline", false)
	external, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	primary, _, err := lifetime.fixture.WaitUsable(external, -1)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	lifetime.primary = primary
	luaWatches := replicaCLILuaLifecycleCancel(t, lifetime)
	first := replicaCLIReadArm(t, lifetime, "cli-cancel-read")
	// Prepare the final watch now, before the final fresh caller's 12s clock.
	final := replicaCLIReadArm(t, lifetime, "cli-retire-read")
	call := replicaCLIReadStart(t, lifetime, first, "cli-cancel-read")
	defer call.cancel()
	replicaCLIReadBarrier(t, first, false, time.Now().Add(3*time.Second))
	replyLossGauges(t, lifetime, "read_cancel_active", true)
	cancelled := time.Now()
	call.cancel()
	replicaCLIReadBarrier(t, first, true, cancelled.Add(3*time.Second))
	timer := time.NewTimer(time.Until(cancelled.Add(3 * time.Second)))
	var receipt replicaReadReceipt
	select {
	case receipt = <-call.done:
	case <-timer.C:
		t.Fatal("canceled SDK read did not complete within original 3s")
	}
	timer.Stop()
	if receipt.document || receipt.err == nil && !receipt.failure {
		t.Fatal("canceled sent read invented successful document receipt", receipt)
	}
	replicaCLIIdle(t, lifetime.service, cancelled.Add(3*time.Second))
	replicaCLIReadFacts(t, first, "CLI_cancel_caller_and_product_credit_joined")
	t.Logf("sent_read case=CLI_cancel same_PID=%d original_cancel_elapsed=%s caller_error=%v caller_failure=%t document=false", lifetime.pid, time.Since(cancelled), receipt.err, receipt.failure)

	external, cancel = context.WithTimeout(t.Context(), 10*time.Second)
	primary, usable, err := lifetime.fixture.WaitUsable(external, -1)
	cancel()
	if err != nil {
		t.Fatal("independent current primary-majority usable", err)
	}
	if primary != lifetime.primary {
		t.Fatal("unexpected primary changed under fixed-member read watch", primary, lifetime.primary)
	}
	healthy, stop := context.WithDeadline(t.Context(), usable.Add(5*time.Second))
	if err := replicaCLIHealth(healthy, lifetime.client, lifetime.fixture.DB); err != nil {
		stop()
		t.Fatal("same SDK immutable health recovery", err)
	}
	replyLossNewWork(t, healthy, lifetime, "read-recovery-new-id")
	stop()
	if time.Since(usable) > 5*time.Second || lifetime.service.command.Process.Pid != lifetime.pid {
		t.Fatal("same PID/SDK read recovery budget")
	}
	t.Logf("sent_read phase=CLI_same_PID_recovery same_PID=%d same_SDK=true independent_primary=%d primary_majority_usable=true immutable_health=true new_ID=read-recovery-new-id business_success=true new_work_elapsed=%s", lifetime.pid, primary, time.Since(usable))
	replicaCLIReadFacts(t, first, "CLI_after_same_PID_health_and_new_work")
	replyLossOracles(t, lifetime, "read_recovery_old_operations_unchanged")
	replicaCLIIdle(t, lifetime.service, time.Now().Add(3*time.Second))
	replyLossGauges(t, lifetime, "read_idle_after_recovery", false)
	// All topology/health/old-effect preparation precedes the fresh caller.
	lua := replicaCLILuaLifecycleStart(t, lifetime, testmongo.LuaRetireCommit)
	if err := lua.watch.ValidateCommitHeld(); err != nil {
		t.Fatal(err)
	}
	retirement := replicaCLIReadStart(t, lifetime, final, "cli-retire-read")
	defer retirement.cancel()
	replicaCLIReadBarrier(t, final, false, time.Now().Add(3*time.Second))
	replyLossGauges(t, lifetime, "read_actual_SIGTERM_active", true)
	replicaCLIReadPending(t, retirement, 8*time.Second)
	t.Logf("sent_read phase=CLI_immediately_before_SIGTERM same_PID=%d caller_pending=true uncancelled=true original_deadline_remaining=%s find_reply_held=true cleanup_not_started=true", lifetime.pid, time.Until(retirement.deadline))
	if lua.ctx.Err() != nil {
		t.Fatal("Lua caller canceled before actual SIGTERM")
	}
	luaDeadline, _ := lua.ctx.Deadline()
	if time.Until(luaDeadline) < 8*time.Second {
		t.Fatal("original Lua caller insufficient SIGTERM budget")
	}
	select {
	case receipt := <-lua.done:
		t.Fatal("Lua returned before actual SIGTERM", receipt)
	default:
	}
	signalStarted := time.Now()
	shutdownDeadline := signalStarted.Add(8 * time.Second)
	shutdown, stopShutdown := context.WithDeadline(t.Context(), shutdownDeadline)
	defer stopShutdown()
	terminal := lifetime.service.stop(t)
	if !terminal.successful() || terminal.Elapsed < 5*time.Second {
		t.Fatal("actual sent-read CLI full drain/default retirement exit", terminal)
	}
	// Neither hold has been touched. Only actual process Wait establishes the
	// product-driver lifetime boundary; CLI private sessions are not inspectable.
	if retirement.ctx.Err() != nil {
		t.Fatal("original 12s caller expired during actual Wait", retirement.ctx.Err())
	}
	replicaCLIReadBarrier(t, final, true, shutdownDeadline)
	replicaCLIReadFacts(t, first, "CLI_actual_process_Wait")
	replicaCLIReadFacts(t, final, "CLI_actual_process_Wait")
	for _, watch := range []*testmongo.ReadWatch{first, final} {
		if s := watch.Snapshot(); s.Closed || s.Joined != 0 {
			t.Fatal("observer intervened before actual CLI Wait", s)
		}
	}
	if err := testmongo.CheckLuaLifecycleDeadline(shutdown); err != nil {
		t.Fatal("CLI read SDK join original shutdown deadline", err)
	}
	select {
	case receipt = <-retirement.done:
	case <-shutdown.Done():
		t.Fatal("SDK read worker did not join inside original shutdown 8s", shutdown.Err())
	}
	if err := testmongo.CheckLuaLifecycleDeadline(shutdown); err != nil {
		t.Fatal("CLI read SDK join exceeded original shutdown 8s", err)
	}
	if receipt.document || receipt.err == nil && !receipt.failure {
		t.Fatal("retired read invented successful document receipt", receipt)
	}
	t.Logf("sent_read case=CLI_retirement same_PID=%d actual_Wait=true wait_error=%v exit_code_valid=%t exit_code=%d timed_out=%t elapsed=%s original_caller_alive=true caller_error=%v caller_failure=%t", lifetime.pid, terminal.WaitErr, terminal.ExitCodeValid, terminal.ExitCode, terminal.TimedOut, terminal.Elapsed, receipt.err, receipt.failure)
	replicaCLILuaRetired(t, lua, shutdownDeadline)
	if err := testmongo.CheckLuaLifecycleDeadline(shutdown); err != nil {
		t.Fatal("CLI final read/Lua shutdown joins exceeded original 8s", err)
	}
	if retirement.ctx.Err() != nil {
		t.Fatal("original read 12s caller expired before shutdown joins", retirement.ctx.Err())
	}
	luaWatches = append(luaWatches, lua.watch)
	watches := []*testmongo.ReadWatch{first, final}
	return terminal, watches, luaWatches
}
