package store

import (
	"context"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func (r *Runtime) StartNative(ctx context.Context, open *pb.NativeOpen, exchange *execution.NativeExchange) (*Ticket, *pb.Failure) {
	plan, failure := r.adapter.PrepareNative(open)
	if failure != nil {
		return nil, failure
	}
	plan.Exchange = exchange
	ticket, failure, _ := r.Submit(ctx, plan, nil)
	return ticket, failure
}

func (t *Ticket) WaitNative() *pb.NativeEnd {
	<-t.ready
	return t.nativeEnd
}

func (r *Runtime) executeNative(b *batch) {
	t := b.items[0]
	exchange := t.plan.Exchange
	interrupted := make(chan struct{})
	stop := context.AfterFunc(b.ctx, func() { _ = exchange.Source.Close(); exchange.Sink.Interrupt(); close(interrupted) })
	end, feedback := r.adapter.ExecuteNative(b.ctx, t.plan, exchange)
	if !stop() {
		<-interrupted
	}
	_ = exchange.Source.Close()
	b.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active--
	delete(r.batches, b)
	t.nativeEnd = end
	r.completeLocked(t, nil)
	// Upload/output stall and arbitrary native errors are not DB congestion.
	// The adapter supplies at most one explicit sample for the whole exchange.
	r.controller.observe(b, feedback, r.limits.Concurrency, time.Now())
	r.notifyLocked()
}
