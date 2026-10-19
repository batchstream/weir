package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
)

type metricAdapter struct{ lifecycleAdapter }

func (a *metricAdapter) Execute(_ context.Context, plans []*execution.Plan, emit execution.Emit) {
	results := make([]*pb.Event, len(plans))
	outcomes := []pb.MutationOutcome{pb.MutationOutcome_APPLIED, pb.MutationOutcome_APPLIED, pb.MutationOutcome_NOT_APPLIED, pb.MutationOutcome_UNKNOWN}
	for i, p := range plans {
		var failure *pb.Failure
		if i > 0 {
			failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "private backend error")
		}
		results[i] = execution.FailedEvent(p.Command, outcomes[i], failure)
	}
	for i, result := range results {
		output := result
		_ = emit(plans[i], output)
	}
	return
}
func TestMetricsExactBatchOutcomesAdmissionAndConfiguredCapacity(t *testing.T) {
	limits := DefaultLimits()
	limits.QueueBytes = (9 << 20)
	limits.BatchOperations = 4
	limits.BatchBytes = 8 << 20
	adapter := &metricAdapter{}
	r := newRuntime(adapter, limits, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var tickets []*Ticket
	for i, key := range []string{"a", "b", "c", "d", "e"} {
		p := plan(uint64(i), key, false)
		p.Bytes = limits.QueueBytes / 5
		ticket, f, _ := r.Submit(ctx, p, nil)
		if f != nil {
			t.Fatal(f)
		}
		tickets = append(tickets, ticket)
	}
	p := plan(5, "rejected", false)
	p.Bytes = limits.QueueBytes / 5
	if _, f, _ := r.Submit(ctx, p, nil); f == nil {
		t.Fatal("missing rejection")
	}
	session := r.NewSession()
	defer session.Close()
	for range 5 {
		if _, f, _ := r.Submit(ctx, p, session); f == nil {
			t.Fatal("missing capacity wait")
		}
	}
	assertRecordFailures(t, r, "mutate", 0)
	if testmetrics.Sum(testmetrics.Gather(t, r), "weir_store_records_total") != 0 {
		t.Fatal("admission rejection counted as a completed record")
	}
	r.mu.Lock()
	b, _ := r.selectLocked(time.Now())
	r.mu.Unlock()
	r.execute(b)
	ready := testmetrics.Gather(t, r)
	if testmetrics.Sum(ready, "weir_store_retained_results") != 4 || testmetrics.Sum(ready, "weir_store_retained_result_reserved_bytes") != 4*execution.ResultOverheadBytes {
		t.Fatal("ready result byte reservations")
	}
	assertRecordFailures(t, r, "mutate", 3)
	cancel()
	r.mu.Lock()
	r.cancelQueuedLocked()
	r.mu.Unlock()
	for _, ticket := range tickets {
		ticket.Ack()
	}
	for range 3 {
		families := testmetrics.Gather(t, r)
		for _, ticket := range tickets {
			ticket.Ack()
		}
		assertRecordFailures(t, r, "mutate", 4)
		for _, outcome := range []string{"applied", "not_applied", "unknown", "not_started"} {
			labels := map[string]string{"operation": "mutate", "outcome": outcome}
			want := float64(1)
			if outcome == "applied" {
				want = 2
			}
			if got := testmetrics.Sample(families, "weir_store_records_total", labels).GetCounter().GetValue(); got != want {
				t.Fatal(outcome, got)
			}
		}
		if testmetrics.Sum(families, "weir_store_records_total") != float64(len(tickets)) {
			t.Fatal("admission rejection counted as a completed record")
		}
		if testmetrics.Sum(families, "weir_store_executions_total") != 1 || testmetrics.Sum(families, "weir_store_rejections_total") != 6 {
			t.Fatal("physical/rejection counts")
		}
		h := testmetrics.Sample(families, "weir_store_batch_operations", nil).GetHistogram()
		if h.GetSampleCount() != 1 || h.GetSampleSum() != 4 {
			t.Fatal("batch count/size", h)
		}
		if testmetrics.Sum(families, "weir_store_pending_entries") != 0 || testmetrics.Sum(families, "weir_store_result_reserved_entries") != 0 {
			t.Fatal("ledger not released")
		}
	}
	families := testmetrics.Gather(t, r)
	if testmetrics.Sum(families, "weir_store_pending_entries_limit") != float64(limits.QueueOperations) {
		t.Fatal("configured concurrency metric changed")
	}
	if testmetrics.Sum(families, "weir_store_active_executions_limit") != float64(limits.MaxInFlight) {
		t.Fatal("configured in-flight batch limit missing")
	}
	if testmetrics.Sum(families, "weir_store_batch_max_wait_seconds") != limits.BatchWait.Seconds() {
		t.Fatal("configured collection wait missing")
	}
	if testmetrics.Sum(families, "weir_store_batch_max_operations") != float64(limits.BatchOperations) || testmetrics.Sum(families, "weir_store_batch_max_bytes") != float64(limits.BatchBytes) {
		t.Fatal("configured batch count or byte limit missing")
	}
	if testmetrics.Series(families) != 57 {
		t.Fatal("Store series changed", testmetrics.Series(families))
	}
}

func assertRecordFailures(t *testing.T, r *Runtime, operation string, want float64) {
	t.Helper()
	families := testmetrics.Gather(t, r)
	family := families["weir_store_record_failures_total"]
	if len(family.GetMetric()) != 2 {
		t.Fatal("failure counter must have exactly two static series", family)
	}
	for _, metric := range family.GetMetric() {
		if len(metric.Label) != 1 || metric.Label[0].GetName() != "operation" {
			t.Fatal("failure counter acquired dynamic labels", metric)
		}
		label := metric.Label[0].GetValue()
		if label != "read" && label != "mutate" {
			t.Fatal("failure counter acquired dynamic operation", label)
		}
		expected := float64(0)
		if label == operation {
			expected = want
		}
		if got := metric.GetCounter().GetValue(); got != expected {
			t.Fatalf("%s failures = %v, want %v", label, got, expected)
		}
	}
}

func TestMetricsQueuedReadAndNativeCancellation(t *testing.T) {
	for _, operation := range []string{"read", "native"} {
		t.Run(operation, func(t *testing.T) {
			r := newRuntime(nil, DefaultLimits(), nil)
			work := plan(0, "queued", true)
			var session *Session
			if operation == "native" {
				work.Command = nativeCall()
				session = r.NewSession()
				defer session.Close()
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ticket, failure, _ := r.Submit(ctx, work, session)
			if failure != nil {
				t.Fatal(failure)
			}
			defer ticket.Ack()
			cancel()
			r.mu.Lock()
			r.cancelQueuedLocked()
			r.mu.Unlock()
			final := recordEvent(t, ticket)
			want := float64(1)
			failure = final.GetReadResult().GetFailure()
			if operation == "native" {
				want = 0
				failure = final.GetNativeEnd().GetFailure()
			}
			if failure.GetCode() != pb.FailureCode_CANCELLED {
				t.Fatal("queued cancellation lost final failure", final)
			}
			for range 3 {
				ticket.Ack()
				assertRecordFailures(t, r, "read", want)
				families := testmetrics.Gather(t, r)
				if testmetrics.Sum(families, "weir_store_records_total") != want || testmetrics.Sum(families, "weir_store_executions_total") != 0 {
					t.Fatal("queued read/Native cancellation changed typed record or invocation counts")
				}
			}
		})
	}
}

func TestMetricsFinalRecordFailureAfterResultNormalization(t *testing.T) {
	document := &pb.Document{ContentType: "application/json", Data: []byte(`{"ok":true}`)}
	largeDocument := &pb.Document{Data: make([]byte, 4096)}
	backendFailure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "private backend error")
	largeFailure := protocol.Fail(pb.FailureCode_UNAVAILABLE, strings.Repeat("private backend error", 512))
	invalidMutation := &pb.MutationResult{Outcome: pb.MutationOutcome(999), Failure: largeFailure}
	missingOutcome := &pb.MutationResult{Failure: largeFailure}
	cases := []struct {
		name        string
		read        *pb.ReadResult
		mutation    *pb.MutationResult
		wantOutcome pb.MutationOutcome
		replace     bool
		wantCount   float64
	}{
		{name: "document", read: protocol.ReadDocument(document)},
		{name: "missing", read: protocol.Missing()},
		{name: "read_failure", read: protocol.ReadFailure(backendFailure), wantCount: 1},
		{name: "read_result_budget", read: protocol.ReadDocument(largeDocument), replace: true, wantCount: 1},
		{name: "read_failure_replaced", read: protocol.ReadFailure(largeFailure), replace: true, wantCount: 1},
		{name: "applied_success", mutation: protocol.Mutation(pb.MutationOutcome_APPLIED, nil), wantOutcome: pb.MutationOutcome_APPLIED},
		{name: "applied_failure", mutation: protocol.Mutation(pb.MutationOutcome_APPLIED, backendFailure), wantOutcome: pb.MutationOutcome_APPLIED, wantCount: 1},
		{name: "not_applied_without_failure", mutation: protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, nil), wantOutcome: pb.MutationOutcome_NOT_APPLIED},
		{name: "not_started_without_failure", mutation: protocol.Mutation(pb.MutationOutcome_NOT_STARTED, nil), wantOutcome: pb.MutationOutcome_NOT_STARTED},
		{name: "unknown_without_failure", mutation: protocol.Mutation(pb.MutationOutcome_UNKNOWN, nil), wantOutcome: pb.MutationOutcome_UNKNOWN},
		{name: "applied_failure_replaced", mutation: protocol.Mutation(pb.MutationOutcome_APPLIED, largeFailure), wantOutcome: pb.MutationOutcome_APPLIED, replace: true, wantCount: 1},
		{name: "unknown_failure_replaced", mutation: protocol.Mutation(pb.MutationOutcome_UNKNOWN, largeFailure), wantOutcome: pb.MutationOutcome_UNKNOWN, replace: true, wantCount: 1},
		{name: "not_applied_failure_replaced", mutation: protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, largeFailure), wantOutcome: pb.MutationOutcome_NOT_APPLIED, replace: true, wantCount: 1},
		{name: "not_started_failure_replaced", mutation: protocol.Mutation(pb.MutationOutcome_NOT_STARTED, largeFailure), wantOutcome: pb.MutationOutcome_NOT_STARTED, replace: true, wantCount: 1},
		{name: "invalid_outcome_failure_replaced", mutation: invalidMutation, wantOutcome: pb.MutationOutcome_UNKNOWN, replace: true, wantCount: 1},
		{name: "missing_outcome_failure_replaced", mutation: missingOutcome, wantOutcome: pb.MutationOutcome_UNKNOWN, replace: true, wantCount: 1},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			event := &pb.Event{}
			operation, outcome := "read", "success"
			if test.read != nil {
				event.Value = &pb.Event_ReadResult{ReadResult: test.read}
				if test.wantCount != 0 {
					outcome = "failure"
				}
			} else {
				operation, outcome = "mutate", strings.ToLower(test.wantOutcome.String())
				event.Value = &pb.Event_MutationResult{MutationResult: test.mutation}
			}
			adapter := &growingReadResultAdapter{events: []*pb.Event{event}}
			memory := testExecutionMemory(128 << 20)
			limits := DefaultLimits()
			limits.BatchWait = 0
			r := newRuntime(adapter, limits, memory)
			work := plan(0, "record", test.read != nil)
			work.ResultBytes = execution.ResultOverheadBytes
			ticket, failure, _ := r.Submit(t.Context(), work, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			defer ticket.Ack()
			selected := selectCrossBatch(r)
			if selected == nil {
				t.Fatal("record not dispatched")
			}
			if test.replace {
				// Revoke future growth after admission/dispatch. The terminal
				// failure still fits the lease's prepaid result window.
				snapshot := overload.Snapshot{RecoveryAllowanceKnown: true}
				memory.ObserveMemory(snapshot)
			}
			r.execute(selected)
			final := recordEvent(t, ticket)
			if test.replace {
				var failure *pb.Failure
				if test.read != nil {
					failure = final.GetReadResult().GetFailure()
				} else {
					failure = final.GetMutationResult().GetFailure()
					if final.GetMutationResult().GetOutcome() != test.wantOutcome {
						t.Fatalf("result-budget failure changed mutation evidence: got %v, want %v", final.GetMutationResult().GetOutcome(), test.wantOutcome)
					}
				}
				if final == event || failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
					t.Fatal("test did not exercise finishResult replacement", final)
				}
			} else if final != event {
				t.Fatal("unexpected result replacement", final)
			}
			assertRecordFailures(t, r, operation, test.wantCount)
			for range 3 {
				ticket.Ack()
				assertRecordFailures(t, r, operation, test.wantCount)
				families := testmetrics.Gather(t, r)
				labels := map[string]string{"operation": operation, "outcome": outcome}
				if got := testmetrics.Sample(families, "weir_store_records_total", labels).GetCounter().GetValue(); got != 1 {
					t.Fatal("record metric did not use final result", got)
				}
				if testmetrics.Sum(families, "weir_store_records_total") != 1 {
					t.Fatal("record counted more than once")
				}
			}
		})
	}
}
