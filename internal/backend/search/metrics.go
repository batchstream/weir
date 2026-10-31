package search

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// searchMetrics is created by Open. A nil searchMetrics records nothing.
type searchMetrics struct {
	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec
	httpRecords  *prometheus.HistogramVec
	httpBytes    *prometheus.HistogramVec
}

type httpObservation struct {
	call     exchange
	request  *http.Request
	duration time.Duration
	status   int
	err      error
}

func newSearchMetrics() *searchMetrics {
	labels := []string{"operation", "outcome", "visibility"}
	requestOptions := prometheus.CounterOpts{
		Name: "weir_search_http_requests_total",
		Help: "Completed Record and startup qualification HTTP exchange attempts, including failures; excludes Native relay and pre-dispatch rejection. Success describes HTTP status and valid JSON, not individual bulk items.",
	}
	httpRequests := prometheus.NewCounterVec(requestOptions, labels)
	durationOptions := prometheus.HistogramOpts{
		Name:    "weir_search_http_request_duration_seconds",
		Help:    "Record and startup qualification HTTP exchange elapsed time from dispatch through body consumption and JSON validation, including failures and bulk refresh=wait_for; excludes Native relay. Concurrent durations overlap and are not wall time or isolated refresh wait.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
	}
	httpDuration := prometheus.NewHistogramVec(durationOptions, labels)
	recordOptions := prometheus.HistogramOpts{
		Name:    "weir_search_http_request_records",
		Help:    "Actual record count in each attempted Record mget or bulk HTTP request, including failed exchanges; excludes pre-dispatch rejection and Native relay.",
		Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
	}
	httpRecords := prometheus.NewHistogramVec(recordOptions, []string{"operation", "visibility"})
	byteOptions := prometheus.HistogramOpts{
		Name:    "weir_search_http_request_body_bytes",
		Help:    "Framed body bytes in each attempted Record or startup qualification HTTP request, including failed exchanges; excludes headers and Native relay. Attempts do not prove delivery or acknowledgement.",
		Buckets: []float64{1024, 4096, 16384, 65536, 262144, 1 << 20, 4 << 20, 8 << 20, 16 << 20, 32 << 20},
	}
	httpBytes := prometheus.NewHistogramVec(byteOptions, []string{"operation", "visibility"})
	metrics := &searchMetrics{httpRequests: httpRequests, httpDuration: httpDuration, httpRecords: httpRecords, httpBytes: httpBytes}
	return metrics
}

func (m *searchMetrics) observeHTTP(observation httpObservation) {
	if m == nil {
		return
	}
	operation := observation.call.operation
	switch operation {
	case "metadata", "mget", "bulk", "refresh":
	default:
		operation = "other"
	}
	visibility := "none"
	if operation == "bulk" {
		switch observation.request.URL.Query().Get("refresh") {
		case "wait_for":
			visibility = "wait_for"
		case "true":
			visibility = "immediate"
		}
	}
	outcome := "success"
	if observation.err != nil {
		switch {
		case errors.Is(observation.err, errTimeout), errors.Is(observation.request.Context().Err(), context.DeadlineExceeded):
			outcome = "timeout"
		case errors.Is(observation.err, errCanceled), errors.Is(observation.request.Context().Err(), context.Canceled):
			outcome = "canceled"
		case errors.Is(observation.err, errTransport), errors.Is(observation.err, errWriteNotSent):
			outcome = "transport_error"
		default:
			outcome = "invalid_response"
		}
	} else if observation.status < 200 || observation.status >= 300 {
		outcome = "http_error"
	}
	m.httpRequests.WithLabelValues(operation, outcome, visibility).Inc()
	m.httpDuration.WithLabelValues(operation, outcome, visibility).Observe(observation.duration.Seconds())
	m.httpBytes.WithLabelValues(operation, visibility).Observe(float64(len(observation.call.body)))
	if (operation == "mget" || operation == "bulk") && observation.call.records > 0 {
		m.httpRecords.WithLabelValues(operation, visibility).Observe(float64(observation.call.records))
	}
}

func (a *Adapter) Describe(ch chan<- *prometheus.Desc) {
	if m := a.metrics; m != nil {
		m.httpRequests.Describe(ch)
		m.httpDuration.Describe(ch)
		m.httpRecords.Describe(ch)
		m.httpBytes.Describe(ch)
	}
	a.lua.Describe(ch)
}

func (a *Adapter) Collect(ch chan<- prometheus.Metric) {
	if m := a.metrics; m != nil {
		m.httpRequests.Collect(ch)
		m.httpDuration.Collect(ch)
		m.httpRecords.Collect(ch)
		m.httpBytes.Collect(ch)
	}
	a.lua.Collect(ch)
}
