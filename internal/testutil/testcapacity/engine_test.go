package main

import (
	"context"
	"encoding/json"
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
	c, err := newClient(server.URL, "", 62)
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

func TestWriteOnlyTimingLedgerAndSuccessLatency(t *testing.T) {
	opts := TrialOptions{Rate: 1000, Seconds: 1, Workers: 64, Prefix: "writes", WriteEvery: 1, TimingOnly: true}
	trial, err := runTrial(context.Background(), nil, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(trial.Ledger) != 1000 || trial.Measure.Read.Planned != 0 || trial.Measure.Put.Planned != 1000 || trial.Measure.Put.Completed+trial.Measure.Put.Drop != 1000 {
		t.Fatal("write-only count or ledger mismatch", len(trial.Ledger), trial.Measure)
	}
	var appliedCount uint64
	for _, outcome := range trial.Ledger {
		if outcome == applied {
			appliedCount++
		}
	}
	var latencyCount uint64
	for _, count := range trial.Measure.Put.SuccessArrival.Counts {
		latencyCount += count
	}
	if appliedCount != trial.Measure.Put.Success || latencyCount != appliedCount {
		t.Fatal("success/ledger/latency mismatch", appliedCount, latencyCount, trial.Measure.Put.Success)
	}
}

func TestMaximumDispersedTrialEvidenceBound(t *testing.T) {
	var m Metrics
	histograms := []*Histogram{&m.Wake, &m.Decision, &m.Construct, &m.Handoff, &m.WorkerStart, &m.SuccessArrival, &m.SuccessDispatch, &m.Arrival, &m.Dispatch, &m.Lag}
	for _, histogram := range histograms {
		for i := range histogram.Counts {
			histogram.Counts[i] = 1
		}
	}
	window := Window{All: m, Read: m, Put: m}
	opts := TrialOptions{Seconds: 120, WarmSeconds: 20}
	trial := Trial{Options: opts, Warm: window, Measure: window, Windows: make([]Window, 12)}
	for i := range trial.Windows {
		trial.Windows[i] = window
	}
	value := map[string]any{"type": "trial", "trial": trial, "run_error": "<nil>"}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= 1<<20 || len(raw) > 32<<20 {
		t.Fatal("unexpected worst-case evidence size", len(raw))
	}
	t.Log("maximum dispersed histogram trial bytes", len(raw))
}

func TestLateDropHasNoFabricatedLatency(t *testing.T) {
	var w Window
	now := time.Now()
	op := Operation{Write: true, Planned: now.Add(-21 * time.Millisecond), Decision: now}
	d := Decision{Operation: op, Wake: now, Constructed: now, Reason: "expired"}
	w.plan(d)
	if w.All.Planned != 1 || w.All.Drop != 1 || w.All.Late != 1 || w.Put.Drop != 1 || w.Read.Planned != 0 || w.All.Arrival.Percentile(99) != -1 || w.All.Lag.Percentile(99) != -1 {
		t.Fatal(w)
	}
}

func TestDueAndCancelledTimingConservation(t *testing.T) {
	opts := TrialOptions{Rate: 1000, Seconds: 1, Workers: 64, Prefix: "timing", TimingOnly: true}
	trial, err := runTrial(context.Background(), nil, opts)
	if err != nil {
		t.Fatal(err)
	}
	m := trial.Measure.All
	count := func(h Histogram) uint64 {
		var n uint64
		for _, c := range h.Counts {
			n += c
		}
		return n
	}
	if m.Due != 1000 || m.CancelledFuture != 0 || count(m.Wake) != 1000 || count(m.Decision) != 1000 || count(m.Construct) != 1000 || count(m.Handoff) != m.Started+m.WorkerExpired || count(m.Lag) != m.Started {
		t.Fatal("timing count identity", m.Planned, m.Due, m.Started, m.Drop)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	trial, err = runTrial(ctx, nil, opts)
	m = trial.Measure.All
	if err == nil || m.CancelledFuture != 1000 || m.Due != 0 || count(m.Wake) != 0 || count(m.Decision) != 0 || m.Started != 0 {
		t.Fatal("future cancellation fabricated observations", m.CancelledFuture, m.Due, err)
	}
}

func TestLateSLODoesNotFabricateCompletion(t *testing.T) {
	now := time.Now()
	op := Operation{Planned: now.Add(-7 * time.Millisecond), Decision: now, Write: true}
	d := Decision{Operation: op, Wake: now.Add(-time.Millisecond), Constructed: now}
	var w Window
	w.plan(d)
	if w.All.Drop != 0 || w.All.Wake.Percentile(99) != 6000 || w.All.Decision.Percentile(99) != 7000 || w.All.Arrival.Percentile(99) != -1 {
		t.Fatal("arrival timing is not completion or per-item5ms expiry")
	}
}
