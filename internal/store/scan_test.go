package store

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
)

// This adapter is an offline scheduling dependency seam, not backend qualification.
type scanTestAdapter struct {
	fetches, cleanups atomic.Int32
	fetching          chan struct{}
	fetchGate         <-chan struct{}
	cleanupGate       <-chan struct{}
}

func (a *scanTestAdapter) Prepare(*pb.BulkOperation) (*execution.Plan, *pb.Failure) { return nil, nil }
func (a *scanTestAdapter) PrepareScan(*pb.ScanRequest) (*execution.Plan, *pb.Failure) {
	p := &execution.Plan{Scan: true, Bytes: 1024, ResultBytes: protocol.MaxDocument + 512, PageBytes: 1 << 20, Key: "scan", Token: "scan"}
	return p, nil
}
func (a *scanTestAdapter) FetchScan(ctx context.Context, _ *execution.Plan) (*execution.ScanPage, execution.Feedback) {
	a.fetches.Add(1)
	if a.fetching != nil {
		select {
		case a.fetching <- struct{}{}:
		default:
		}
	}
	page := &execution.ScanPage{}
	if a.fetchGate != nil {
		select {
		case <-a.fetchGate:
		case <-ctx.Done():
			page.Failure = protocol.ContextFailure(ctx)
		}
	}
	if page.Failure == nil {
		doc := &pb.Document{MediaType: "application/octet-stream", Data: []byte("opaque")}
		page.Documents = []*pb.Document{doc}
	}
	return page, execution.Healthy
}
func (a *scanTestAdapter) Execute(_ context.Context, plans []*execution.Plan) ([]*pb.BulkResult, execution.Feedback) {
	results := make([]*pb.BulkResult, len(plans))
	for i, p := range plans {
		results[i] = protocol.ResultError(p.Operation, pb.MutationOutcome_APPLIED, nil)
	}
	return results, execution.Healthy
}
func (a *scanTestAdapter) CloseScan(ctx context.Context, _ *execution.Plan) *pb.Failure {
	a.cleanups.Add(1)
	if a.cleanupGate != nil {
		select {
		case <-a.cleanupGate:
		case <-ctx.Done():
			return protocol.ContextFailure(ctx)
		}
	}
	return nil
}
func (a *scanTestAdapter) Close() error { return nil }

func scanRuntime(t *testing.T, a *scanTestAdapter) *Runtime {
	t.Helper()
	limits := DefaultLimits()
	limits.Concurrency = 1
	limits.Collect = 0
	r, err := New(a, limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return r
}
func TestScanContinuationReleasesPermitAndKeepsCharges(t *testing.T) {
	a := &scanTestAdapter{}
	r := scanRuntime(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req := &pb.ScanRequest{}
	ticket, f := r.StartScan(ctx, req)
	if f != nil {
		t.Fatal(f)
	}
	defer ticket.CloseScan()
	page, err := ticket.WaitPage(ctx)
	if err != nil || len(page.Documents) != 1 {
		t.Fatal(page, err)
	}
	snap := r.Snapshot()
	if snap.Active != 0 || snap.Pending != 1 || snap.PendingBytes != 1024 || snap.ScanSessions != 1 || snap.ScanPages != 1 {
		t.Fatal(snap)
	}
	if _, f := r.StartScan(ctx, req); f == nil || f.Code != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("session limit", f)
	}
	p := plan(0, "read", true)
	read, f, _ := r.Submit(ctx, p, nil)
	if f != nil {
		t.Fatal(f)
	}
	if _, err := read.Wait(ctx); err != nil {
		t.Fatal("read blocked by idle scan", err)
	}
	read.Ack()
	r.SetOverloaded(true)
	r.BeginDrain()
	for i := 0; i < 5; i++ {
		page = nil
		if !ticket.AdvanceScan() {
			t.Fatal("continuation rejected under overload/drain")
		}
		page, err = ticket.WaitPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got := r.Snapshot()
		if got.PendingBytes != snap.PendingBytes || got.ResultBytes != snap.ResultBytes || got.Retained != 1 || got.Active != 0 {
			t.Fatal("duplicate accounting", got)
		}
	}
	page = nil
	if f := ticket.CloseScan(); f != nil {
		t.Fatal(f)
	}
	ticket.CloseScan()
	got := r.Snapshot()
	if got.Pending != 0 || got.PendingBytes != 0 || got.ResultBytes != 0 || got.ScanSessions != 0 || a.cleanups.Load() != 1 || a.fetches.Load() != 6 {
		t.Fatal(got, a.cleanups.Load(), a.fetches.Load())
	}
}
func TestScanCancellationCleanupKeepsBorrowedPageBudget(t *testing.T) {
	a := &scanTestAdapter{}
	r := scanRuntime(t, a)
	ctx, cancel := context.WithCancel(context.Background())
	req := &pb.ScanRequest{}
	ticket, f := r.StartScan(ctx, req)
	if f != nil {
		t.Fatal(f)
	}
	defer ticket.CloseScan()
	wait, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, err := ticket.WaitPage(wait); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-ticket.scan.done:
	case <-wait.Done():
		t.Fatal("cleanup did not finish")
	}
	snap := r.Snapshot()
	if snap.ScanSessions != 1 || snap.ScanPageBytes == 0 {
		t.Fatal("borrowed page reservation released early", snap)
	}
	if ticket.AdvanceScan() {
		t.Fatal("fetch after cancel")
	}
	ticket.CloseScan()
	if r.Snapshot().Retained != 0 || a.fetches.Load() != 1 {
		t.Fatal("leak/restart")
	}
}
func TestScanCancelDuringFetchAndDrain(t *testing.T) {
	for _, drain := range []bool{false, true} {
		gate := make(chan struct{})
		started := make(chan struct{}, 1)
		a := &scanTestAdapter{fetchGate: gate, fetching: started}
		r := scanRuntime(t, a)
		ctx, cancel := context.WithCancel(context.Background())
		req := &pb.ScanRequest{}
		ticket, f := r.StartScan(ctx, req)
		if f != nil {
			t.Fatal(f)
		}
		<-started
		if drain {
			closed := make(chan error, 1)
			go func() {
				timeout, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer stop()
				closed <- r.Close(timeout)
			}()
			go ticket.CloseScan()
			if err := <-closed; err != nil {
				t.Fatal(err)
			}
		} else {
			cancel()
			ticket.CloseScan()
		}
		cancel()
		ticket.CloseScan()
		if a.fetches.Load() != 1 || a.cleanups.Load() != 1 || r.Snapshot().Retained != 0 {
			t.Fatal("cleanup/permit leak", r.Snapshot())
		}
	}
}

func TestScanQueuedCancelAndFIFOYield(t *testing.T) {
	a := &scanTestAdapter{}
	l := DefaultLimits()
	l.Concurrency = 1
	l.Collect = 0
	r := newRuntime(a, l)
	ctx, cancel := context.WithCancel(context.Background())
	req := &pb.ScanRequest{}
	scan, f := r.StartScan(ctx, req)
	if f != nil {
		t.Fatal(f)
	}
	cancel()
	r.mu.Lock()
	r.cancelQueuedLocked()
	r.mu.Unlock()
	scan.CloseScan()
	if a.fetches.Load() != 0 || a.cleanups.Load() != 1 || r.Snapshot().PendingBytes != 0 {
		t.Fatal("cancelled queued scan fetched/leaked")
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	scan, f = r.StartScan(ctx, req)
	if f != nil {
		t.Fatal(f)
	}
	r.mu.Lock()
	batch := r.selectLocked(time.Now())
	r.mu.Unlock()
	r.fetchScan(batch)
	p := plan(0, "next", true)
	read, f, _ := r.Submit(ctx, p, nil)
	if f != nil {
		t.Fatal(f)
	}
	if !scan.AdvanceScan() {
		t.Fatal("advance")
	}
	r.mu.Lock()
	batch = r.selectLocked(time.Now())
	r.mu.Unlock()
	if len(batch.items) != 1 || batch.items[0] != read {
		t.Fatal("continuation did not yield to FIFO tail")
	}
	r.execute(batch)
	read.Ack()
	cancel()
	r.mu.Lock()
	r.cancelQueuedLocked()
	r.mu.Unlock()
	scan.CloseScan()
	if r.Snapshot().Retained != 0 {
		t.Fatal("leak")
	}
}

func TestScanCleanupDeadlineAndReservation(t *testing.T) {
	gate := make(chan struct{})
	a := &scanTestAdapter{cleanupGate: gate}
	r := scanRuntime(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.ScanRequest{}
	ticket, f := r.StartScan(ctx, req)
	if f != nil {
		t.Fatal(f)
	}
	if _, err := ticket.WaitPage(ctx); err != nil {
		t.Fatal(err)
	}
	stopped := make(chan *pb.Failure, 1)
	start := time.Now()
	go func() { stopped <- ticket.CloseScan() }()
	for a.cleanups.Load() == 0 && time.Since(start) < time.Second {
		time.Sleep(time.Millisecond)
	}
	snap := r.Snapshot()
	if snap.ScanCleanups != 1 || snap.ScanSessions != 1 || snap.Active != 0 {
		t.Fatal("cleanup lost budget/held permit", snap)
	}
	r.SetOverloaded(true)
	select {
	case failure := <-stopped:
		if failure.GetCode() != pb.FailureCode_DEADLINE_EXCEEDED || time.Since(start) > 3*time.Second {
			t.Fatal(failure, time.Since(start))
		}
	case <-ctx.Done():
		t.Fatal("cleanup unbounded")
	}
	if snap := r.Snapshot(); snap.ScanSessions != 0 || snap.Retained != 0 || snap.ResultBytes != 0 {
		t.Fatal("cleanup failure leaked local budget", snap)
	}
}
