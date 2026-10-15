package search

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type searchMetrics struct {
	once         sync.Once
	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec
	httpRecords  *prometheus.HistogramVec
	httpBytes    *prometheus.HistogramVec
	luaDuration  *prometheus.HistogramVec
}

type httpObservation struct {
	call     exchange
	request  *http.Request
	duration time.Duration
	status   int
	err      error
}

func (m *searchMetrics) initialize() {
	m.once.Do(func() {
		labels := []string{"operation", "outcome", "visibility"}
		requestOptions := prometheus.CounterOpts{
			Name: "weir_search_http_requests_total",
			Help: "Completed Record/Scan and startup qualification HTTP exchange attempts, including failures; excludes Native relay and pre-dispatch rejection. Success describes HTTP status and valid JSON, not individual bulk items.",
		}
		m.httpRequests = prometheus.NewCounterVec(requestOptions, labels)
		durationOptions := prometheus.HistogramOpts{
			Name:    "weir_search_http_request_duration_seconds",
			Help:    "Record/Scan and startup qualification HTTP exchange elapsed time from dispatch through body consumption and JSON validation, including failures and bulk refresh=wait_for; excludes Native relay. Concurrent durations overlap and are not wall time or isolated refresh wait.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}
		m.httpDuration = prometheus.NewHistogramVec(durationOptions, labels)
		recordOptions := prometheus.HistogramOpts{
			Name:    "weir_search_http_request_records",
			Help:    "Actual record count in each attempted Record mget or bulk HTTP request, including failed exchanges; excludes pre-dispatch rejection and Native relay.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
		}
		m.httpRecords = prometheus.NewHistogramVec(recordOptions, []string{"operation", "visibility"})
		byteOptions := prometheus.HistogramOpts{
			Name:    "weir_search_http_request_body_bytes",
			Help:    "Framed body bytes in each attempted Record/Scan or startup qualification HTTP request, including failed exchanges; excludes headers and Native relay. Attempts do not prove delivery or acknowledgement.",
			Buckets: []float64{1024, 4096, 16384, 65536, 262144, 1 << 20, 4 << 20, 8 << 20, 16 << 20, 32 << 20},
		}
		m.httpBytes = prometheus.NewHistogramVec(byteOptions, []string{"operation", "visibility"})
		luaOptions := prometheus.HistogramOpts{
			Name:    "weir_search_lua_evaluation_duration_seconds",
			Help:    "Main-process luaengine.Evaluate elapsed time including compile and conversion, excluding external JSON decoding/encoding and HTTP; concurrent durations overlap.",
			Buckets: []float64{.000001, .00001, .0001, .001, .01, .1, 1, 10},
		}
		m.luaDuration = prometheus.NewHistogramVec(luaOptions, []string{"outcome"})
	})
}

func (m *searchMetrics) observeHTTP(observation httpObservation) {
	operation := observation.call.operation
	switch operation {
	case "metadata", "mget", "bulk", "refresh", "search":
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

func (m *searchMetrics) observeLua(duration time.Duration, err error) {
	outcome := "success"
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		outcome = "timeout"
	case errors.Is(err, context.Canceled):
		outcome = "canceled"
	case err != nil:
		outcome = "error"
	}
	m.luaDuration.WithLabelValues(outcome).Observe(duration.Seconds())
}

func (a *Adapter) Describe(ch chan<- *prometheus.Desc) {
	a.metrics.initialize()
	a.metrics.httpRequests.Describe(ch)
	a.metrics.httpDuration.Describe(ch)
	a.metrics.httpRecords.Describe(ch)
	a.metrics.httpBytes.Describe(ch)
	a.metrics.luaDuration.Describe(ch)
	for _, name := range []string{"owned", "peak", "acquired", "released"} {
		ch <- connectionMetricDescription(name)
	}
}

func connectionMetricDescription(name string) *prometheus.Desc {
	description := prometheus.NewDesc(
		"weir_backend_connections_"+name,
		"Local backend connection ownership from DNS to raw Close completion; excludes remote tails and separate OCSP I/O.",
		nil,
		nil,
	)
	return description
}

func (a *Adapter) Collect(ch chan<- prometheus.Metric) {
	a.metrics.initialize()
	a.metrics.httpRequests.Collect(ch)
	a.metrics.httpDuration.Collect(ch)
	a.metrics.httpRecords.Collect(ch)
	a.metrics.httpBytes.Collect(ch)
	a.metrics.luaDuration.Collect(ch)
	d := a.dialer
	values := map[string]float64{"owned": 0, "peak": 0, "acquired": 0, "released": 0}
	if d != nil {
		d.mu.Lock()
		values["owned"] = float64(d.owned)
		values["peak"] = float64(d.peak)
		values["acquired"] = float64(d.acquired)
		values["released"] = float64(d.released)
		d.mu.Unlock()
	}
	for name, value := range values {
		desc := connectionMetricDescription(name)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value)
	}
}

func (a *Adapter) logConnections() {
	d := a.dialer
	d.mu.Lock()
	defer d.mu.Unlock()
	slog.Info(
		"backend_connections_closed",
		"backend",
		"search",
		"store",
		a.config.Store,
		"owned",
		d.owned,
		"peak",
		d.peak,
		"acquired",
		d.acquired,
		"released",
		d.released,
	)
}
