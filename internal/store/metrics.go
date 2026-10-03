package store

import (
	pb "github.com/batchstream/weir-protocol/api/weir/v1"

	"github.com/batchstream/weir/internal/execution"
	"github.com/prometheus/client_golang/prometheus"
)

type runtimeMetrics struct {
	executions, rejections, records *prometheus.CounterVec
	queue, duration                 *prometheus.HistogramVec
	batch                           prometheus.Histogram
	feedback                        execution.Feedback
	observed                        bool
}

func newRuntimeMetrics() runtimeMetrics {
	recordOptions := prometheus.CounterOpts{Name: "weir_store_records_total", Help: "Per-record backend terminal evidence; not delivery acknowledgements."}
	execOptions := prometheus.CounterOpts{Name: "weir_store_executions_total", Help: "Physical unified adapter invocations."}
	rejectOptions := prometheus.CounterOpts{Name: "weir_store_rejections_total", Help: "Preparation and admission denials."}
	queueOptions := prometheus.HistogramOpts{Name: "weir_store_queue_wait_seconds", Help: "Time from admission to dispatch.", Buckets: []float64{.001, .01, .1, 1, 10}}
	durationOptions := prometheus.HistogramOpts{Name: "weir_store_execution_seconds", Help: "Adapter invocation duration including bounded streaming output.", Buckets: []float64{.001, .01, .1, 1, 10}}
	batchOptions := prometheus.HistogramOpts{Name: "weir_store_batch_operations", Help: "Operations per bounded invocation.", Buckets: []float64{1, 2, 4, 8, 16, 128}}
	metrics := runtimeMetrics{records: prometheus.NewCounterVec(recordOptions, []string{"operation", "outcome"}), executions: prometheus.NewCounterVec(execOptions, []string{"kind"}), rejections: prometheus.NewCounterVec(rejectOptions, []string{"reason"}), queue: prometheus.NewHistogramVec(queueOptions, []string{"kind"}), duration: prometheus.NewHistogramVec(durationOptions, []string{"kind"}), batch: prometheus.NewHistogram(batchOptions)}
	for _, outcome := range []string{"applied", "not_applied", "not_started", "unknown", "invalid"} {
		metrics.records.WithLabelValues("mutate", outcome)
	}
	for _, outcome := range []string{"success", "failure"} {
		metrics.records.WithLabelValues("read", outcome)
	}
	metrics.executions.WithLabelValues("route")
	metrics.queue.WithLabelValues("route")
	metrics.duration.WithLabelValues("route")
	for _, reason := range []string{"prepare", "capacity", "budget", "draining", "overload", "canceled", "session"} {
		metrics.rejections.WithLabelValues(reason)
	}
	return metrics
}
func (r *Runtime) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(r, ch) }
func (r *Runtime) Collect(ch chan<- prometheus.Metric) {
	snapshot := r.Snapshot()
	values := map[string]float64{
		"pending_entries": float64(snapshot.Pending), "pending_reserved_bytes": float64(snapshot.PendingBytes),
		"result_reserved_entries": float64(snapshot.Retained), "result_reserved_bytes": float64(snapshot.ResultBytes),
		"working_reserved_bytes": float64(snapshot.WorkingBytes), "retained_results": float64(snapshot.Ready), "retained_result_reserved_bytes": float64(snapshot.ReadyBytes),
		"active_executions": float64(snapshot.Active), "publishers": float64(snapshot.Publishers),
		"pending_entries_limit": float64(r.limits.PendingOperations), "pending_reserved_bytes_limit": float64(r.limits.PendingBytes),
		"result_reserved_entries_limit": float64(r.limits.ResultOperations), "result_reserved_bytes_limit": float64(r.limits.ResultBytes),
		"working_reserved_bytes_limit": float64(r.limits.WorkingBytes),
		"concurrency_limit":            float64(snapshot.ConcurrencyLimit), "draining": boolValue(snapshot.Draining), "closed": boolValue(snapshot.Closed), "overloaded": boolValue(snapshot.Overloaded),
	}
	for name, value := range values {
		description := prometheus.NewDesc("weir_store_"+name, "Bounded local admission reservations; bytes do not represent heap or RSS.", nil, nil)
		ch <- prometheus.MustNewConstMetric(description, prometheus.GaugeValue, value)
	}
	description := prometheus.NewDesc("weir_store_feedback", "Last observed execution feedback.", []string{"feedback"}, nil)
	for _, label := range []string{"unobserved", "healthy", "congested", "neutral", "completed"} {
		ch <- prometheus.MustNewConstMetric(description, prometheus.GaugeValue, boolValue(snapshot.Feedback == label), label)
	}
	if collector, ok := r.adapter.(prometheus.Collector); ok {
		collector.Collect(ch)
	}
	collectors := []prometheus.Collector{r.metrics.records, r.metrics.executions, r.metrics.rejections, r.metrics.queue, r.metrics.duration, r.metrics.batch}
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
func (r *Runtime) terminalLocked(t *Ticket, result *pb.Result) {
	r.terminalResultLocked(t.plan, result)
}

func (r *Runtime) terminalResultLocked(plan *execution.Plan, result *pb.Result) {
	if plan.Operation == nil {
		return
	}
	if plan.Operation.GetRead() != nil {
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
