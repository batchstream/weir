package luaengine

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics times evaluations by outcome. A nil Metrics records nothing.
type Metrics struct {
	duration *prometheus.HistogramVec
}

func NewMetrics() *Metrics {
	opts := prometheus.HistogramOpts{
		Name:    "weir_lua_evaluation_seconds",
		Help:    "Lua evaluation time, including compilation and document conversion.",
		Buckets: []float64{.000001, .00001, .0001, .001, .01, .1, 1, 10},
	}
	metrics := &Metrics{duration: prometheus.NewHistogramVec(opts, []string{"outcome"})}
	return metrics
}

// Evaluate runs the package-level Evaluate and records its duration.
func (m *Metrics) Evaluate(ctx context.Context, program Program) (Result, error) {
	if m == nil {
		return Evaluate(ctx, program)
	}
	started := time.Now()
	result, err := Evaluate(ctx, program)
	m.duration.WithLabelValues(evaluationOutcome(err)).Observe(time.Since(started).Seconds())
	return result, err
}

func evaluationOutcome(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "error"
	}
}

func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	if m != nil {
		m.duration.Describe(ch)
	}
}

func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	if m != nil {
		m.duration.Collect(ch)
	}
}
