package store

import (
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/prometheus/client_golang/prometheus"
)

// These metrics belong to the ledger, not the RPC or relay that borrows it.
// Every vector is populated from a closed vocabulary at construction.
type runtimeMetrics struct {
	records, executions, rejections, changes, native, scans *prometheus.CounterVec
	queue, duration                                         *prometheus.HistogramVec
	batch, exchange                                         prometheus.Histogram
	feedback                                                execution.Feedback
	observed                                                bool
}

var durationBuckets = []float64{.001, .01, .1, 1, 10}

func newRuntimeMetrics() runtimeMetrics {
	recordOpts := prometheus.CounterOpts{Name: "weir_store_records_total", Help: "Admitted record terminal evidence; not delivery receipts."}
	execOpts := prometheus.CounterOpts{Name: "weir_store_executions_total", Help: "Physical adapter invocations, once per batch, fetch or Native exchange."}
	rejectOpts := prometheus.CounterOpts{Name: "weir_store_rejections_total", Help: "Local admission/preparation denial decisions; excludes Bulk capacity denials."}
	changeOpts := prometheus.CounterOpts{Name: "weir_store_window_changes_total", Help: "Actual AIMD window increases or decreases; excludes stale feedback and reductions at C=1."}
	nativeOpts := prometheus.CounterOpts{Name: "weir_store_native_completions_total", Help: "Admitted Native completion observed locally, not client acknowledgement."}
	scanOpts := prometheus.CounterOpts{Name: "weir_store_scan_terminations_total", Help: "Admitted local Scan traversal evidence after cursor cleanup, not delivery."}
	queueOpts := prometheus.HistogramOpts{Name: "weir_store_queue_wait_seconds", Help: "Time from admission/continuation readiness to dispatch per dispatched item.", Buckets: durationBuckets}
	durationOpts := prometheus.HistogramOpts{Name: "weir_store_execution_seconds", Help: "Record batch or Scan fetch adapter call duration; excludes client delivery.", Buckets: durationBuckets}
	batchOpts := prometheus.HistogramOpts{Name: "weir_store_record_batch_operations", Help: "Logical record operations per physical record adapter invocation.", Buckets: []float64{1, 2, 4, 8, 16, 128}}
	exchangeOpts := prometheus.HistogramOpts{Name: "weir_store_native_exchange_seconds", Help: "Native adapter exchange including upload, backend I/O and response delivery; not backend latency.", Buckets: durationBuckets}
	m := runtimeMetrics{
		records:    prometheus.NewCounterVec(recordOpts, []string{"operation", "outcome"}),
		executions: prometheus.NewCounterVec(execOpts, []string{"kind"}),
		rejections: prometheus.NewCounterVec(rejectOpts, []string{"reason"}),
		changes:    prometheus.NewCounterVec(changeOpts, []string{"direction"}),
		native:     prometheus.NewCounterVec(nativeOpts, []string{"completion"}),
		scans:      prometheus.NewCounterVec(scanOpts, []string{"result"}),
		queue:      prometheus.NewHistogramVec(queueOpts, []string{"kind"}),
		duration:   prometheus.NewHistogramVec(durationOpts, []string{"kind"}),
		batch:      prometheus.NewHistogram(batchOpts), exchange: prometheus.NewHistogram(exchangeOpts),
	}
	for _, outcome := range []string{"applied", "not_applied", "not_started", "unknown", "invalid"} {
		m.records.WithLabelValues("mutate", outcome)
	}
	for _, outcome := range []string{"success", "failure"} {
		m.records.WithLabelValues("read", outcome)
	}
	for _, kind := range []string{"record", "scan", "native"} {
		m.executions.WithLabelValues(kind)
		m.queue.WithLabelValues(kind)
	}
	for _, kind := range []string{"record", "scan"} {
		m.duration.WithLabelValues(kind)
	}
	for _, reason := range []string{"draining", "overload", "canceled", "session", "live_session", "budget", "capacity", "prepare"} {
		m.rejections.WithLabelValues(reason)
	}
	for _, direction := range []string{"increase", "decrease"} {
		m.changes.WithLabelValues(direction)
	}
	for _, completion := range []string{"not_started", "response_complete", "response_incomplete", "invalid"} {
		m.native.WithLabelValues(completion)
	}
	for _, result := range []string{"exhausted", "failure", "interrupted"} {
		m.scans.WithLabelValues(result)
	}
	return m
}

// Collect takes one short ledger snapshot. Encoding and channel sends are outside
// the Runtime lock. Different Stores do not form an atomic process snapshot.
func (r *Runtime) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(r, ch) }
func (r *Runtime) Collect(ch chan<- prometheus.Metric) {
	s := r.Snapshot()
	values := map[string]float64{
		"pending_entries": float64(s.Pending), "pending_reserved_bytes": float64(s.PendingBytes),
		"result_reserved_entries": float64(s.Retained), "result_reserved_bytes": float64(s.ResultBytes),
		"retained_results": float64(s.Ready), "retained_result_reserved_bytes": float64(s.ReadyBytes), "active_executions": float64(s.Active), "window": float64(s.Window),
		"live_sessions": float64(s.LiveSessions), "scan_sessions": float64(s.ScanSessions), "native_sessions": float64(s.NativeSessions),
		"scan_page_reserved_bytes": float64(s.ScanPageBytes), "native_reserved_bytes": float64(s.NativeBytes),
		"scan_pages": float64(s.ScanPages), "scan_cleanups": float64(s.ScanCleanups),
		"pending_entries_limit": float64(r.limits.PendingOperations), "pending_reserved_bytes_limit": float64(r.limits.PendingBytes),
		"result_reserved_entries_limit": float64(r.limits.ResultOperations), "result_reserved_bytes_limit": float64(r.limits.ResultBytes),
		"window_limit": float64(r.limits.Concurrency), "live_sessions_limit": 1,
		"cooldown": boolValue(s.Cooldown), "draining": boolValue(s.Draining), "closed": boolValue(s.Closed), "overloaded": boolValue(s.Overloaded),
	}
	for name, value := range values {
		desc := prometheus.NewDesc("weir_store_"+name, "Local ledger state/limit; byte values are reservations, not heap or RSS.", nil, nil)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value)
	}
	desc := prometheus.NewDesc("weir_store_feedback", "Last execution feedback observed, not a backend health guarantee.", []string{"feedback"}, nil)
	for _, label := range []string{"unobserved", "healthy", "congested", "neutral"} {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, boolValue(s.Feedback == label), label)
	}
	if collector, ok := r.adapter.(prometheus.Collector); ok {
		collector.Collect(ch)
	}
	m := &r.metrics
	collectors := []prometheus.Collector{m.records, m.executions, m.rejections, m.changes, m.native, m.scans, m.queue, m.duration, m.batch, m.exchange}
	for _, collector := range collectors {
		collector.Collect(ch)
	}
}
func boolValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func (r *Runtime) observeLocked(b *batch, fb execution.Feedback) {
	before := r.controller.window
	r.controller.observe(b, fb, r.limits.Concurrency, time.Now())
	r.metrics.feedback, r.metrics.observed = fb, true
	if r.controller.window > before {
		r.metrics.changes.WithLabelValues("increase").Inc()
	}
	if r.controller.window < before {
		r.metrics.changes.WithLabelValues("decrease").Inc()
	}
}
func (r *Runtime) terminalLocked(t *Ticket, result *pb.BulkResult) {
	if t.plan.Native {
		label := "invalid"
		switch t.nativeEnd.GetCompletion() {
		case pb.NativeCompletion_NATIVE_NOT_STARTED:
			label = "not_started"
		case pb.NativeCompletion_RESPONSE_COMPLETE:
			label = "response_complete"
		case pb.NativeCompletion_RESPONSE_INCOMPLETE:
			label = "response_incomplete"
		}
		r.metrics.native.WithLabelValues(label).Inc()
		return
	}
	if t.plan.Operation.GetRead() != nil {
		label := "success"
		if result.GetRead().GetFailure() != nil {
			label = "failure"
		}
		r.metrics.records.WithLabelValues("read", label).Inc()
		return
	}
	label := "invalid"
	switch result.GetMutation().GetOutcome() {
	case pb.MutationOutcome_APPLIED:
		label = "applied"
	case pb.MutationOutcome_NOT_APPLIED:
		label = "not_applied"
	case pb.MutationOutcome_NOT_STARTED:
		label = "not_started"
	case pb.MutationOutcome_UNKNOWN:
		label = "unknown"
	}
	r.metrics.records.WithLabelValues("mutate", label).Inc()
}
