package server

import (
	"sync"
	"sync/atomic"

	"github.com/batchstream/weir/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service is a closed choice between the two static execution destinations.
// Assembly owns both; listeners only borrow references.
type Service struct {
	LocalStore *store.Runtime
	RemoteWeir *RemoteWeir
}

type Admission struct {
	slots, connections chan struct{}
	draining           chan struct{}
	once               sync.Once
	overloaded         atomic.Bool
	rejections         *prometheus.CounterVec
}

func NewAdmission(l Limits) (*Admission, error) {
	if err := l.Validate(); err != nil {
		return nil, err
	}
	a := &Admission{slots: make(chan struct{}, l.Sessions), connections: make(chan struct{}, l.Connections), draining: make(chan struct{})}
	opts := prometheus.CounterOpts{Name: "weir_admission_rejections_total", Help: "Process ingress rejection branches; no client-controlled label values."}
	a.rejections = prometheus.NewCounterVec(opts, []string{"reason"})
	for _, reason := range []string{"connections", "sessions", "draining", "overload", "ingress", "method", "route", "operation", "hop"} {
		a.rejections.WithLabelValues(reason)
	}
	return a, nil
}
func (a *Admission) SetOverloaded(value bool) { a.overloaded.Store(value) }
func (a *Admission) BeginDrain()              { a.once.Do(func() { close(a.draining) }) }
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

func (r *RemoteWeir) enter(d *delivery) error {
	select {
	case r.slots <- struct{}{}:
		d.mu.Lock()
		if d.finished {
			d.mu.Unlock()
			<-r.slots
			return status.Error(codes.Canceled, "delivery closed")
		}
		d.remoteSlots = r.slots
		d.mu.Unlock()
		return nil
	default:
		r.rejections.WithLabelValues("relay").Inc()
		return status.Error(codes.ResourceExhausted, "peer relay bound")
	}
}
