package server

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/testmetrics"
)

func TestMetricsTwoHopsCountOnlyFinalExecution(t *testing.T) {
	f := newChain(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := f.client.Mutate(ctx, testMutation("secret-payload"))
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal(result, err)
	}
	for range 3 {
		local := testmetrics.Gather(t, f.runtime)
		if testmetrics.Sum(local, "weir_store_executions_total") != 1 || testmetrics.Sum(local, "weir_store_records_total") != 1 {
			t.Fatal("execution count")
		}
		for _, remote := range f.remotes {
			families := testmetrics.Gather(t, remote)
			if testmetrics.Sum(families, "weir_relay_terminations_total") != 1 || families["weir_store_executions_total"] != nil {
				t.Fatal("relay repeated execution")
			}
		}
	}
}

func TestMetricsConcurrentScrapesBusinessAndCancellation(t *testing.T) {
	f := newChain(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := f.client.Read(ctx, testRequest()); err != nil {
		t.Fatal(err)
	}
	baseline := runtime.NumGoroutine()
	for round := 0; round < 3; round++ {
		var group sync.WaitGroup
		for range 2 {
			group.Go(func() {
				for range 10 {
					testmetrics.Gather(t, f.runtime)
					for _, remote := range f.remotes {
						testmetrics.Gather(t, remote)
					}
				}
			})
		}
		group.Go(func() {
			for i := 0; i < 30; i++ {
				call, stop := context.WithCancel(ctx)
				if i%3 == 0 {
					stop()
				}
				_, _ = f.client.Read(call, testRequest())
				stop()
			}
		})
		group.Wait()
		until := time.Now().Add(time.Second)
		for f.runtime.Snapshot().Retained != 0 {
			if time.Now().After(until) {
				t.Fatal("retained work")
			}
			time.Sleep(time.Millisecond)
		}
		for _, remote := range f.remotes {
			if len(remote.slots) > cap(remote.slots) || len(remote.sockets) > cap(remote.sockets) {
				t.Fatal("relay bound")
			}
		}
		runtime.GC()
		t.Logf("round=%d baseline goroutines=%d current=%d ledger=%+v", round, baseline, runtime.NumGoroutine(), f.runtime.Snapshot())
		if runtime.NumGoroutine() > baseline+20 {
			t.Fatal("goroutines grew across bounded scrape/load rounds")
		}
	}
}
