package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenLoopFullSlotsTimeoutAndCancellation(t *testing.T) {
	var calls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		select {
		case <-r.Context().Done():
		case <-time.After(1200 * time.Millisecond):
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	c, err := newClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	opts := TrialOptions{Rate: 1000, Seconds: 1, Workers: 1, Prefix: "test"}
	started := time.Now()
	trial, err := runTrial(context.Background(), c, opts)
	if err != nil || trial.Measure.All.Planned != 1000 || trial.Measure.All.Drop < 900 || trial.Measure.All.Completed+trial.Measure.All.Drop != 1000 || trial.Measure.All.Arrival.MaxNS < int64(900*time.Millisecond) || time.Since(started) > 2500*time.Millisecond {
		t.Fatal(trial.Measure.All, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	trial, err = runTrial(ctx, c, opts)
	if err == nil || trial.Measure.All.Drop != 1000 || trial.Measure.All.Started != 0 {
		t.Fatal(err, trial.Measure.All)
	}
	if calls.Load() > 2 {
		t.Fatal("unbounded catch-up", calls.Load())
	}
}

func TestTrialBounds(t *testing.T) {
	for _, rate := range []int{0, 6401} {
		opts := TrialOptions{Rate: rate, Seconds: 120, Workers: 64}
		if _, err := runTrial(context.Background(), nil, opts); err == nil {
			t.Fatal(rate)
		}
	}
}

func TestLateDropHasNoFabricatedLatency(t *testing.T) {
	var w Window
	w.plan(true, true, true)
	if w.All.Planned != 1 || w.All.Drop != 1 || w.All.Late != 1 || w.Put.Drop != 1 || w.Read.Planned != 0 || w.All.Arrival.Percentile(99) != -1 || w.All.Lag.Percentile(99) != -1 {
		t.Fatal(w)
	}
}
