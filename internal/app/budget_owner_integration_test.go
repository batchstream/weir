//go:build integration

package app

import (
	"context"
	"fmt"
	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir/internal/testutil"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
)

func budgetOwner(t *testing.T, p *process, locals, limit int) int {
	t.Helper()
	metrics := testmetrics.Scrape(t, p.diagnostic)
	stores := []string{"records"}
	if locals == 2 {
		stores = append(stores, "extra")
	}
	owned := 0
	for _, store := range stores {
		labels := map[string]string{"store": store}
		current := testmetrics.Sample(metrics, "weir_backend_connections_owned", labels).GetGauge().GetValue()
		peak := testmetrics.Sample(metrics, "weir_backend_connections_peak", labels).GetGauge().GetValue()
		cap := testmetrics.Sample(metrics, "weir_backend_connections_limit", labels).GetGauge().GetValue()
		if cap != float64(limit) || peak < current || peak > cap || peak < 1 {
			t.Fatal("local owner bound", store, current, peak, cap)
		}
		owned += int(current)
	}
	return owned
}

// The owner's final one-time log is emitted after raw Close and dial-worker join.
// Read only after Wait, when exec has joined the stderr copier. It covers startup
// and all shutdown transitions that a final pre-SIGTERM scrape would miss.
func budgetClosedOwner(t *testing.T, p *process, locals, limit int) {
	t.Helper()
	seen := make(map[string]bool)
	for _, line := range strings.Split(p.stderr.String(), "\n") {
		if !strings.Contains(line, "backend_connections_closed ") {
			continue
		}
		values := make(map[string]string)
		for _, field := range strings.Fields(line) {
			key, val, ok := strings.Cut(field, "=")
			if ok {
				values[key] = val
			}
		}
		store := values["store"]
		if seen[store] || store != "records" && store != "extra" {
			t.Fatal("unexpected owner", store)
		}
		seen[store] = true
		current, e1 := strconv.Atoi(values["owned"])
		peak, e2 := strconv.Atoi(values["peak"])
		cap, e3 := strconv.Atoi(values["limit"])
		acquired, e4 := strconv.ParseUint(values["acquired"], 10, 64)
		released, e5 := strconv.ParseUint(values["released"], 10, 64)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || current != 0 || peak < 1 || peak > cap || cap != limit || acquired != released || acquired == 0 {
			t.Fatal("final owner proof", line)
		}
		if values["dialing"] != "" && values["dialing"] != "0" || values["closing"] != "" && values["closing"] != "0" {
			t.Fatal("unfinished owner", line)
		}
		t.Logf("PID=%d %s", p.command.Process.Pid, line)
	}
	if len(seen) != locals {
		t.Fatal("missing final local owner", p.command.Process.Pid, len(seen), locals)
	}
}

type budgetReadTarget struct {
	process       *process
	client        pb.StoreServiceClient
	root          string
	locals, limit int
}

func budgetOverlap(t *testing.T, targets []budgetReadTarget, o *budgetObservation) {
	t.Helper()
	budgetWait(t, "before five Local barrier", func() bool {
		a, _, _, _ := o.snapshot()
		return a == 0
	})
	gate := make(chan struct{})
	o.hold(gate, 0)
	var release sync.Once
	defer release.Do(func() {
		close(gate)
		o.hold(nil, 0)
	})
	var workers sync.WaitGroup
	for _, target := range targets {
		workers.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			req := &pb.ReadRequest{Resource: target.root + "/s:overlap"}
			routedResult105, err := weirclient.Record(ctx, target.client, testutil.RecordCall(req))
			result := routedResult105.GetRead()
			if err != nil || result.GetFailure() != nil {
				t.Error("overlap Read", err, result)
			}
		})
	}
	budgetWait(t, "five Local simultaneous requests", func() bool {
		a, _, _, _ := o.snapshot()
		return a == 5
	})
	seen := make(map[*process]bool)
	owned, limit, active := 0, 0, 0
	for _, target := range targets {
		if seen[target.process] {
			continue
		}
		seen[target.process] = true
		n := budgetOwner(t, target.process, target.locals, target.limit)
		owned += n
		limit += target.locals * target.limit
		a := int(testmetrics.Sum(testmetrics.Scrape(t, target.process.diagnostic), "weir_store_active_executions"))
		active += a
		t.Logf("overlap barrier PID=%d locals=%d owned=%d limit=%d active=%d", target.process.command.Process.Pid, target.locals, n, target.locals*target.limit, a)
	}
	if len(seen) != 4 || active != 5 || limit != 14 || owned > limit {
		t.Fatal("four process/five Local overlap missing", len(seen), active, owned, limit)
	}
	t.Logf("simultaneous 4 processes / 5 Local: active=%d owned=%d derived ceiling=%d; observer request gate still closed", active, owned, limit)
	release.Do(func() {
		close(gate)
		o.hold(nil, 0)
	})
	workers.Wait()
}

func budgetReplacementReads(t *testing.T, targets []budgetReadTarget) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	var workers sync.WaitGroup
	for _, target := range targets {
		for range 8 {
			workers.Go(func() {
				for n := 0; ctx.Err() == nil; n++ {
					req := &pb.ReadRequest{Resource: target.root + fmt.Sprintf("/s:replacement%d", n)}
					_, _ = weirclient.Record(ctx, target.client, testutil.RecordCall(req))
				}
			})
		}
	}
	workers.Wait()
	cancel()
	for _, target := range targets {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		req := &pb.ReadRequest{Resource: target.root + "/s:recovered"}
		for {
			routedResult160, err := weirclient.Record(ctx, target.client, testutil.RecordCall(req))
			result := routedResult160.GetRead()
			if err == nil && result.GetFailure() == nil {
				break
			}
			if ctx.Err() != nil {
				t.Fatal("replacement recovery", err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		cancel()
		budgetOwner(t, target.process, target.locals, target.limit)
	}
	t.Log("both replacement Local stores: 16 continuous readers/100ms including caller cancellation, then fresh reads within original 3s recovery budget")
}
