package server

import (
	"context"
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var metricStatuses = []string{"ok", "canceled", "deadline", "non_ok"}

func statusLabel(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
		return "deadline"
	}
	return "non_ok"
}

type transportMetrics struct {
	rpcs, watchdogs, rejections *prometheus.CounterVec
}

func newTransportMetrics() transportMetrics {
	rpcOpts := prometheus.CounterOpts{
		Name: "weir_rpc_completions_total",
		Help: "Execute RPC completions by status; not client delivery acknowledgement.",
	}
	watchOpts := prometheus.CounterOpts{
		Name: "weir_watchdog_expirations_total",
		Help: "RPCs canceled because input or output made no progress within the stall timeout.",
	}
	rejectionOpts := prometheus.CounterOpts{
		Name: "weir_admission_rejections_total",
		Help: "RPCs refused before reading request data.",
	}
	m := transportMetrics{
		rpcs:       prometheus.NewCounterVec(rpcOpts, []string{"status"}),
		watchdogs:  prometheus.NewCounterVec(watchOpts, []string{"phase"}),
		rejections: prometheus.NewCounterVec(rejectionOpts, []string{"reason"}),
	}
	for _, label := range metricStatuses {
		m.rpcs.WithLabelValues(label)
	}
	for _, phase := range []string{"input", "output"} {
		m.watchdogs.WithLabelValues(phase)
	}
	for _, reason := range []string{"draining", "overload", "method", "execute"} {
		m.rejections.WithLabelValues(reason)
	}
	return m
}

func (s *Server) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(s, ch) }

func (s *Server) Collect(ch chan<- prometheus.Metric) {
	s.metrics.rpcs.Collect(ch)
	s.metrics.watchdogs.Collect(ch)
	s.metrics.rejections.Collect(ch)
	sessions := prometheus.NewDesc("weir_ingress_sessions", "Active business RPCs.", nil, nil)
	ch <- prometheus.MustNewConstMetric(sessions, prometheus.GaugeValue, float64(s.activeRPCs.Load()))
}
