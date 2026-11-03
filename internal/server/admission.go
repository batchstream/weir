package server

import (
	"sync"
	"sync/atomic"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
)

// A stream turned away before any of it was read carries a Failure as a status
// detail. UNAVAILABLE alone also reports a lost connection, after which records
// may have taken effect; the detail tells a client that none did.
var (
	errDraining   = refusedStream("draining")
	errOverloaded = refusedStream("process overloaded")
)

func refusedStream(message string) error {
	failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, message)
	refused, err := status.New(codes.Unavailable, message).WithDetails(protoadapt.MessageV1Of(failure))
	if err != nil {
		panic(err)
	}
	return refused.Err()
}

// Admission is the process-wide gate for business RPCs.
type Admission struct {
	draining   chan struct{}
	once       sync.Once
	overloaded atomic.Bool
	activeRPCs atomic.Int64
	rejections *prometheus.CounterVec
}

func NewAdmission() *Admission {
	opts := prometheus.CounterOpts{
		Name: "weir_admission_rejections_total",
		Help: "RPCs refused before reading request data.",
	}
	a := &Admission{draining: make(chan struct{}), rejections: prometheus.NewCounterVec(opts, []string{"reason"})}
	for _, reason := range []string{"draining", "overload", "method", "execute"} {
		a.rejections.WithLabelValues(reason)
	}
	return a
}

func (a *Admission) SetOverloaded(value bool) { a.overloaded.Store(value) }

func (a *Admission) BeginDrain() { a.once.Do(func() { close(a.draining) }) }

func (a *Admission) check() error {
	select {
	case <-a.draining:
		a.rejections.WithLabelValues("draining").Inc()
		return errDraining
	default:
	}
	if a.overloaded.Load() {
		a.rejections.WithLabelValues("overload").Inc()
		return errOverloaded
	}
	return nil
}

func (a *Admission) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(a, ch) }

func (a *Admission) Collect(ch chan<- prometheus.Metric) {
	desc := prometheus.NewDesc("weir_ingress_sessions", "Active business RPCs.", nil, nil)
	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(a.activeRPCs.Load()))
	a.rejections.Collect(ch)
}
