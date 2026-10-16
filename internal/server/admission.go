package server

import (
	"sync"
	"sync/atomic"

	"github.com/batchstream/weir/internal/overload"
	"github.com/batchstream/weir/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Admission struct {
	draining          chan struct{}
	once              sync.Once
	overloaded        atomic.Bool
	rejections        *prometheus.CounterVec
	wireBytes         atomic.Int64
	activeRPCs        atomic.Int64
	activeConnections atomic.Int64
	received          sync.Map
	responses         sync.Map
	reservations      sync.Map
	memory            *store.ExecutionMemory
}

func NewAdmission(l Limits) (*Admission, error) {
	if err := l.Validate(); err != nil {
		return nil, err
	}
	a := &Admission{draining: make(chan struct{})}
	a.memory = store.NewExecutionMemory()
	guard := overload.New(nil, overload.DefaultLimits())
	a.memory.ObserveMemory(guard.Snapshot())
	opts := prometheus.CounterOpts{
		Name: "weir_admission_rejections_total",
		Help: "Process ingress rejection branches; no client-controlled label values.",
	}
	a.rejections = prometheus.NewCounterVec(opts, []string{"reason"})
	for _, reason := range []string{"draining", "overload", "ingress", "method", "execute"} {
		a.rejections.WithLabelValues(reason)
	}
	return a, nil
}

// SetExecutionMemory binds all Node listeners to the same process ledger before
// serving. Standalone servers use the admission's bounded default ledger.
func (a *Admission) SetExecutionMemory(memory *store.ExecutionMemory) { a.memory = memory }

func (a *Admission) SetOverloaded(value bool) { a.overloaded.Store(value) }

func (a *Admission) BeginDrain() { a.once.Do(func() { close(a.draining) }) }

func (a *Admission) check() error {
	select {
	case <-a.draining:
		a.rejections.WithLabelValues("draining").Inc()
		return status.Error(codes.Unavailable, "draining")
	default:
	}
	if a.overloaded.Load() {
		a.rejections.WithLabelValues("overload").Inc()
		return status.Error(codes.ResourceExhausted, "process overloaded")
	}
	return nil
}
