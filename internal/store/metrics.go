package store

import (
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/prometheus/client_golang/prometheus"
)

type runtimeMetrics struct {
	records, failures, nativeCompletions, rejections *prometheus.CounterVec
	executions                                       prometheus.Counter
	queueWait, duration, batch                       prometheus.Histogram
}

func newRuntimeMetrics() runtimeMetrics {
	recordOptions := prometheus.CounterOpts{Name: "weir_store_records_total", Help: "Final record results by mutation outcome or read success; applied does not imply business success."}
	failureOptions := prometheus.CounterOpts{Name: "weir_store_record_failures_total", Help: "Final record results carrying a Failure, including applied mutations."}
	nativeOptions := prometheus.CounterOpts{Name: "weir_store_native_completions_total", Help: "Final Native completion evidence; not backend business success or client receipt."}
	rejectOptions := prometheus.CounterOpts{Name: "weir_store_rejections_total", Help: "Operations refused before queueing."}
	execOptions := prometheus.CounterOpts{Name: "weir_store_executions_total", Help: "Adapter invocations."}
	queueOptions := prometheus.HistogramOpts{Name: "weir_store_queue_wait_seconds", Help: "Time from queueing to dispatch.", Buckets: []float64{.001, .01, .1, 1, 10}}
	durationOptions := prometheus.HistogramOpts{Name: "weir_store_execution_seconds", Help: "Adapter invocation duration, including Native response delivery.", Buckets: []float64{.001, .01, .1, 1, 10}}
	batchOptions := prometheus.HistogramOpts{Name: "weir_store_batch_operations", Help: "Operations per adapter invocation.", Buckets: []float64{1, 2, 4, 8, 16, 128}}
	metrics := runtimeMetrics{
		records:           prometheus.NewCounterVec(recordOptions, []string{"operation", "outcome"}),
		failures:          prometheus.NewCounterVec(failureOptions, []string{"operation"}),
		nativeCompletions: prometheus.NewCounterVec(nativeOptions, []string{"completion"}),
		rejections:        prometheus.NewCounterVec(rejectOptions, []string{"reason"}),
		executions:        prometheus.NewCounter(execOptions),
		queueWait:         prometheus.NewHistogram(queueOptions),
		duration:          prometheus.NewHistogram(durationOptions),
		batch:             prometheus.NewHistogram(batchOptions),
	}
	for _, outcome := range []string{"applied", "not_applied", "not_started", "unknown"} {
		metrics.records.WithLabelValues("mutate", outcome)
	}
	for _, outcome := range []string{"success", "failure"} {
		metrics.records.WithLabelValues("read", outcome)
	}
	for _, operation := range []string{"read", "mutate"} {
		metrics.failures.WithLabelValues(operation)
	}
	for _, completion := range []string{"not_started", "response_complete", "response_incomplete"} {
		metrics.nativeCompletions.WithLabelValues(completion)
	}
	for _, reason := range []string{"prepare", "draining", "overload"} {
		metrics.rejections.WithLabelValues(reason)
	}
	return metrics
}

func (m runtimeMetrics) observe(command *pb.Command, event *pb.Event) {
	switch {
	case command.GetNative() != nil:
		label := "response_incomplete"
		switch event.GetNativeEnd().GetCompletion() {
		case pb.NativeCompletion_NATIVE_NOT_STARTED:
			label = "not_started"
		case pb.NativeCompletion_RESPONSE_COMPLETE:
			label = "response_complete"
		}
		m.nativeCompletions.WithLabelValues(label).Inc()
	case command.GetRead() != nil:
		label := "success"
		if event.GetReadResult().GetFailure() != nil {
			label = "failure"
			m.failures.WithLabelValues("read").Inc()
		}
		m.records.WithLabelValues("read", label).Inc()
	default:
		if event.GetMutationResult().GetFailure() != nil {
			m.failures.WithLabelValues("mutate").Inc()
		}
		label := "unknown"
		switch event.GetMutationResult().GetOutcome() {
		case pb.MutationOutcome_APPLIED:
			label = "applied"
		case pb.MutationOutcome_NOT_APPLIED:
			label = "not_applied"
		case pb.MutationOutcome_NOT_STARTED:
			label = "not_started"
		}
		m.records.WithLabelValues("mutate", label).Inc()
	}
}

func (r *Runtime) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(r, ch) }

// Stats is the Store's current queue and execution state.
type Stats struct {
	Queued, QueuedBytes, Active int
	Draining, Overloaded        bool
}

func (r *Runtime) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	stats := Stats{Queued: len(r.queue), QueuedBytes: r.queueBytes, Active: len(r.batches), Draining: r.draining, Overloaded: r.overloaded}
	return stats
}

func (r *Runtime) Collect(ch chan<- prometheus.Metric) {
	stats := r.Stats()
	values := map[string]float64{
		"queued_operations":       float64(stats.Queued),
		"queued_bytes":            float64(stats.QueuedBytes),
		"active_executions":       float64(stats.Active),
		"active_executions_limit": float64(r.limits.MaxInFlight),
		"draining":                boolValue(stats.Draining),
		"overloaded":              boolValue(stats.Overloaded),
	}
	for name, value := range values {
		description := prometheus.NewDesc("weir_store_"+name, "Store queue and execution state.", nil, nil)
		ch <- prometheus.MustNewConstMetric(description, prometheus.GaugeValue, value)
	}
	if collector, ok := r.adapter.(prometheus.Collector); ok {
		collector.Collect(ch)
	}
	collectors := []prometheus.Collector{r.metrics.records, r.metrics.failures, r.metrics.nativeCompletions, r.metrics.rejections, r.metrics.executions, r.metrics.queueWait, r.metrics.duration, r.metrics.batch}
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
