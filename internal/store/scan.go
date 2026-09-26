package store

import (
	"context"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

const maxScanPageBytes = 128 << 20
const scanCleanupTimeout = 2 * time.Second

type scanState struct {
	page           *execution.ScanPage
	done           chan struct{}
	cleanupFailure *pb.Failure
	cleaned        bool
}

func (r *Runtime) StartScan(ctx context.Context, req *pb.ScanRequest) (*Ticket, *pb.Failure) {
	p, f := r.adapter.PrepareScan(req)
	if f != nil {
		return nil, f
	}
	t, f, _ := r.Submit(ctx, p, nil)
	return t, f
}

// WaitPage borrows the one reserved page until AdvanceScan or CloseScan. The
// caller must finish all sends before advancing; it must never retain old pages.
func (t *Ticket) WaitPage(ctx context.Context) (*execution.ScanPage, error) {
	r := t.runtime
	r.mu.Lock()
	ready := t.ready
	r.mu.Unlock()
	select {
	case <-ready:
		r.mu.Lock()
		defer r.mu.Unlock()
		if t.scan.page != nil && !t.abandoned && t.ctx.Err() == nil && !r.closed {
			return t.scan.page, nil
		}
		return nil, context.Canceled
	case <-t.scan.done:
		return nil, context.Canceled
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *Ticket) AdvanceScan() bool {
	r := t.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	page := t.scan.page
	if t.state != 2 || page == nil || page.Exhausted || page.Failure != nil || t.ctx.Err() != nil || t.abandoned || r.closed {
		return false
	}
	t.scan.page = nil
	t.state = 0
	t.eligible = time.Time{}
	t.ready = make(chan struct{})
	// Keep the same entry and charges; only its FIFO position changes. New
	// overload/drain admission restrictions cannot reject this continuation.
	for i, queued := range r.queue {
		if queued == t {
			copy(r.queue[i:], r.queue[i+1:])
			r.queue[len(r.queue)-1] = t
			break
		}
	}
	r.notifyLocked()
	return true
}

// CloseScan cancels outstanding I/O and joins the one bounded cleanup. It is
// idempotent and does not depend on application admission or output progress.
func (t *Ticket) CloseScan() *pb.Failure {
	r := t.runtime
	r.mu.Lock()
	t.abandoned = true
	if t.scan.cleaned && !t.acked {
		r.releaseScanLocked(t)
	}
	r.notifyLocked()
	r.mu.Unlock()
	<-t.scan.done
	return t.scan.cleanupFailure
}

func (r *Runtime) fetchScan(b *batch) {
	t := b.items[0]
	page, fb := r.adapter.FetchScan(b.ctx, t.plan)
	if b.timeoutOwned && b.ctx.Err() == context.DeadlineExceeded {
		fb = execution.Congested
	}
	b.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active--
	delete(r.batches, b)
	t.state = 2
	t.scan.page = page
	close(t.ready)
	// Exactly one sample per open/fetch; emission and cleanup are not samples.
	r.controller.observe(b, fb, r.limits.Concurrency, time.Now())
	r.notifyLocked()
}

func (r *Runtime) cleanupScan(t *Ticket) {
	ctx, cancel := context.WithTimeout(context.Background(), scanCleanupTimeout)
	f := r.adapter.CloseScan(ctx, t.plan)
	cancel()
	r.mu.Lock()
	defer r.mu.Unlock()
	t.scan.cleanupFailure = f
	t.scan.page = nil
	t.scan.cleaned = true
	if t.abandoned {
		r.releaseScanLocked(t)
	}
	close(t.scan.done)
	r.notifyLocked()
}

// Cancellation may finish native cleanup while the transport still borrows the
// page. Keep the reservation until its consumer has also called CloseScan.
func (r *Runtime) releaseScanLocked(t *Ticket) {
	for i, queued := range r.queue {
		if queued == t {
			copy(r.queue[i:], r.queue[i+1:])
			r.queue[len(r.queue)-1] = nil
			r.queue = r.queue[:len(r.queue)-1]
			break
		}
	}
	r.pendingBytes -= t.plan.Bytes
	r.scan = nil
	if t.stopWatch != nil {
		t.stopWatch()
	}
	r.releaseLocked(t)
}
