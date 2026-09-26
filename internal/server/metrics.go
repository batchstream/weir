package server

import (
	"context"
	"errors"
	"io"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var metricMethods = []string{"Read", "Mutate", "Bulk", "Scan", "Native", "other"}
var metricStatuses = []string{"ok", "canceled", "deadline", "non_ok"}

func methodLabel(method string) string {
	switch method {
	case pb.Weir_Read_FullMethodName:
		return "Read"
	case pb.Weir_Mutate_FullMethodName:
		return "Mutate"
	case pb.Weir_Bulk_FullMethodName:
		return "Bulk"
	case pb.Weir_Scan_FullMethodName:
		return "Scan"
	case pb.Weir_Native_FullMethodName:
		return "Native"
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
	rpcOpts := prometheus.CounterOpts{Name: "weir_rpc_completions_total", Help: "gRPC handler/transport completion status; not client delivery acknowledgement."}
	failureOpts := prometheus.CounterOpts{Name: "weir_transport_failures_total", Help: "At most one observed I/O failure per direction per admitted RPC, independent of execution evidence."}
	watchOpts := prometheus.CounterOpts{Name: "weir_watchdog_expirations_total", Help: "Explicit Bulk watchdog expiry; input_or_result does not attribute blame to the client or backend."}
	forcedOpts := prometheus.CounterOpts{Name: "weir_transport_forced_closes_total", Help: "Actual transport force-close actions, not inferred lost operations."}
	m := transportMetrics{rpcs: prometheus.NewCounterVec(rpcOpts, []string{"method", "status"}), failures: prometheus.NewCounterVec(failureOpts, []string{"method", "phase"}), watchdogs: prometheus.NewCounterVec(watchOpts, []string{"phase"}), forced: prometheus.NewCounterVec(forcedOpts, []string{"reason"})}
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
	for _, reason := range []string{"drain", "abort"} {
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
	values := map[string]int{"connections": len(a.connections), "connections_limit": cap(a.connections), "sessions": len(a.slots), "sessions_limit": cap(a.slots)}
	for name, value := range values {
		desc := prometheus.NewDesc("weir_ingress_"+name, "Shared application/peer admission occupancy or limit.", nil, nil)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(value))
	}
	a.rejections.Collect(ch)
}

func (r *RemoteWeir) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(r, ch) }
func (r *RemoteWeir) Collect(ch chan<- prometheus.Metric) {
	values := map[string]int{"relays": len(r.slots), "relays_limit": cap(r.slots), "sockets": len(r.sockets), "sockets_limit": cap(r.sockets)}
	for name, value := range values {
		desc := prometheus.NewDesc("weir_remote_"+name, "RemoteWeir reserved relay/socket occupancy or limit; sockets include in-progress dial.", nil, nil)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(value))
	}
	desc := prometheus.NewDesc("weir_remote_connectivity", "Current gRPC connectivity state, not database health; scrape never connects.", []string{"state"}, nil)
	state := r.conn.GetState().String()
	for _, label := range []string{"IDLE", "CONNECTING", "READY", "TRANSIENT_FAILURE", "SHUTDOWN"} {
		value := 0.0
		if label == state {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, label)
	}
	r.terminations.Collect(ch)
	r.incompletes.Collect(ch)
	r.rejections.Collect(ch)
}
func (r *RemoteWeir) incomplete(method string, err error, message string) error {
	r.incompletes.WithLabelValues(method).Inc()
	return incomplete(err, message)
}
func (d *delivery) ioFailure(phase string, err error) {
	if err == nil || errors.Is(err, io.EOF) || d.metrics == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	seen := &d.inputFailed
	if phase == "output" {
		seen = &d.outputFailed
	}
	if !*seen {
		*seen = true
		d.metrics.failures.WithLabelValues(d.method, phase).Inc()
	}
}
