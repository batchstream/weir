package store

import (
	"context"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
)

type metricAdapter struct{ scanTestAdapter }

func (a *metricAdapter) Execute(_ context.Context, plans []*execution.Plan, emit execution.Emit) execution.Feedback {
	results := make([]*pb.Result, len(plans))
	outcomes := []pb.MutationOutcome{pb.MutationOutcome_APPLIED, pb.MutationOutcome_NOT_APPLIED, pb.MutationOutcome_UNKNOWN}
	for i, p := range plans {
		var failure *pb.Failure
		if i > 0 {
			failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "private backend error")
		}
		results[i] = protocol.ResultError(p.Operation, outcomes[i], failure)
	}
	for i, result := range results {
		output := &execution.Output{Result: result}
		_ = emit(plans[i], output)
	}
	return execution.Congested
}
func TestMetricsExactBatchOutcomesAdmissionAndConfiguredCapacity(t *testing.T) {
	limits := DefaultLimits()
	limits.PendingOperations = 4
	limits.BatchOperations = 3
	adapter := &metricAdapter{}
	r := newRuntime(adapter, limits)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var tickets []*Ticket
	for i, key := range []string{"a", "b", "c", "d"} {
		p := plan(uint64(i), key, false)
		ticket, f, _ := r.Submit(ctx, p, nil)
		if f != nil {
			t.Fatal(f)
		}
		tickets = append(tickets, ticket)
	}
	p := plan(4, "rejected", false)
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
	r.mu.Lock()
	b := r.selectLocked(time.Now())
	r.mu.Unlock()
	r.execute(b)
	ready := testmetrics.Gather(t, r)
	if testmetrics.Sum(ready, "weir_store_retained_results") != 3 || testmetrics.Sum(ready, "weir_store_retained_result_reserved_bytes") != 3*protocol.ResultOverhead {
		t.Fatal("ready result byte reservations")
	}
	cancel()
	r.mu.Lock()
	r.cancelQueuedLocked()
	r.mu.Unlock()
	for _, ticket := range tickets {
		ticket.Ack()
	}
	for range 3 {
		families := testmetrics.Gather(t, r)
		for _, outcome := range []string{"applied", "not_applied", "unknown", "not_started"} {
			labels := map[string]string{"operation": "mutate", "outcome": outcome}
			if got := testmetrics.Sample(families, "weir_store_records_total", labels).GetCounter().GetValue(); got != 1 {
				t.Fatal(outcome, got)
			}
		}
		if testmetrics.Sum(families, "weir_store_executions_total") != 1 || testmetrics.Sum(families, "weir_store_rejections_total") != 1 {
			t.Fatal("physical/rejection counts")
		}
		h := testmetrics.Sample(families, "weir_store_batch_operations", nil).GetHistogram()
		if h.GetSampleCount() != 1 || h.GetSampleSum() != 3 {
			t.Fatal("batch count/size", h)
		}
		if testmetrics.Sum(families, "weir_store_pending_entries") != 0 || testmetrics.Sum(families, "weir_store_result_reserved_entries") != 0 {
			t.Fatal("ledger not released")
		}
	}
	families := testmetrics.Gather(t, r)
	if testmetrics.Sum(families, "weir_store_concurrency_limit") != float64(limits.Concurrency) {
		t.Fatal("configured concurrency metric changed")
	}
	for _, obsolete := range []string{"weir_store_window", "weir_store_window_limit", "weir_store_cooldown", "weir_store_window_changes_total", "weir_store_backpressure_events_total", "weir_store_latency_baseline_seconds"} {
		if families[obsolete] != nil {
			t.Fatal("removed adaptive metric still exposed", obsolete)
		}
	}
	if testmetrics.Series(families) != 63 {
		t.Fatal("Store series changed", testmetrics.Series(families))
	}
}
