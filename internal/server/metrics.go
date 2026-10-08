package server

import (
	"context"
	"errors"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var metricMethods = []string{"execute", "resolve_store", "sync_directory", "other"}
var metricStatuses = []string{"ok", "canceled", "deadline", "non_ok"}

func methodLabel(method string) string {
	switch method {
	case pb.StoreService_Execute_FullMethodName:
		return "execute"
	case pb.StoreService_ResolveStore_FullMethodName:
		return "resolve_store"
	case peerpb.PeerDiscoveryService_SyncDirectory_FullMethodName:
		return "sync_directory"
	default:
		return "other"
	}
}

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
	rpcs, failures, watchdogs, forced *prometheus.CounterVec
}

func newTransportMetrics() transportMetrics {
	rpcOpts := prometheus.CounterOpts{
		Name: "weir_rpc_completions_total",
		Help: "gRPC handler/transport completion status; not client delivery acknowledgement.",
	}
	failureOpts := prometheus.CounterOpts{
		Name: "weir_transport_failures_total",
		Help: "At most one observed I/O failure per direction per admitted RPC, independent of execution evidence.",
	}
	watchOpts := prometheus.CounterOpts{
		Name: "weir_watchdog_expirations_total",
		Help: "Explicit Execute watchdog expiry; input_or_result does not attribute blame to the client or backend.",
	}
	forcedOpts := prometheus.CounterOpts{
		Name: "weir_transport_forced_closes_total",
		Help: "Actual transport force-close actions, not inferred lost operations.",
	}
	m := transportMetrics{
		rpcs:      prometheus.NewCounterVec(rpcOpts, []string{"method", "status"}),
		failures:  prometheus.NewCounterVec(failureOpts, []string{"method", "phase"}),
		watchdogs: prometheus.NewCounterVec(watchOpts, []string{"phase"}),
		forced:    prometheus.NewCounterVec(forcedOpts, []string{"reason"}),
	}
	for _, method := range metricMethods {
		for _, label := range metricStatuses {
			m.rpcs.WithLabelValues(method, label)
		}
		for _, phase := range []string{"input", "output"} {
			m.failures.WithLabelValues(method, phase)
		}
	}
	for _, phase := range []string{"open", "input_or_result", "output"} {
		m.watchdogs.WithLabelValues(phase)
	}
	for _, reason := range []string{"drain", "abort", "open"} {
		m.forced.WithLabelValues(reason)
	}
	return m
}

func (s *Server) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(s, ch) }

func (s *Server) Collect(ch chan<- prometheus.Metric) {
	s.metrics.rpcs.Collect(ch)
	s.metrics.failures.Collect(ch)
	s.metrics.watchdogs.Collect(ch)
	s.metrics.forced.Collect(ch)
}

func (a *Admission) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(a, ch) }

func (a *Admission) Collect(ch chan<- prometheus.Metric) {
	values := map[string]int{
		"connections":           int(a.activeConnections.Load()),
		"sessions":              int(a.activeRPCs.Load()),
		"queued_response_bytes": int(a.wireBytes.Load()),
	}
	for name, value := range values {
		desc := prometheus.NewDesc("weir_ingress_"+name, "Shared application/peer ingress occupancy.", nil, nil)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(value))
	}
	a.rejections.Collect(ch)
}
