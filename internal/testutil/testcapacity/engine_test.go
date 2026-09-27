package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHistogramMergeBounds(t *testing.T) {
	var a, b Histogram
	for _, d := range []time.Duration{time.Microsecond, 5 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond, 2*time.Second + 1} {
		a.Add(d)
		b.Add(d)
	}
	a.Merge(b)
	if a.Percentile(99) != -1 || a.MaxNS != int64(2*time.Second+1) {
		t.Fatal(a)
	}
	var c Histogram
	c.Add(100 * time.Millisecond)
	if c.Percentile(95) != 100000 {
		t.Fatal(c)
	}
	c.Add(100*time.Millisecond + 1)
	if c.Percentile(99) != 101000 {
		t.Fatal(c)
	}
	raw, err := json.Marshal(a)
	if err != nil || !strings.Contains(string(raw), `"upper_us":-1`) {
		t.Fatal(string(raw), err)
	}
}
func TestDocumentExactAndDistinct(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("read-%04d", i)
		p := payload(id)
		if len(p) != 1024 || !json.Valid(p) || seen[string(p)] || !validPayload(p, id) {
			t.Fatal(i)
		}
		seen[string(p)] = true
	}
}
func TestFullResponseAndNoMutationRetry(t *testing.T) {
	var calls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Length", "1024")
		fmt.Fprint(w, `{"errors":false`)
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	c, err := newClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	op := Operation{ID: "test", Write: true}
	res := c.Call(context.Background(), op)
	if res.Outcome != unknown || calls.Load() != 1 {
		t.Fatal(res, calls.Load())
	}
}
func TestOversizeAndBulkCorrelation(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, strings.Repeat("x", (256<<10)+1)) })
	server := httptest.NewServer(handler)
	defer server.Close()
	c, err := newClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, raw, err := c.request(context.Background(), "GET", "/", nil)
	if err == nil || len(raw) != 0 {
		t.Fatal(err, len(raw))
	}
	raw = []byte(`{"errors":false,"took":1,"items":[{"index":{"_index":"records","_id":"wrong","status":201}}]}`)
	if r := parseBulk(200, raw, "right"); r.Outcome != unknown || r.Class != "correlation" {
		t.Fatal(r)
	}
	raw = []byte(`{"errors":false,"took":1,"items":[{"index":{"_index":"records","_id":"right","_version":1,"status":201,"result":"created","_seq_no":0,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}]}`)
	if r := parseBulk(200, raw, "right"); r.Class != "ok" {
		t.Fatal(r)
	}
	if r := parseBulk(200, append(raw, []byte("x")...), "right"); r.Outcome != unknown {
		t.Fatal(r)
	}
}
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
func TestDeadlineBeforeDispatchNotUnknown(t *testing.T) {
	c, err := newClient("http://127.0.0.1:1", "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	op := Operation{Write: true}
	r := c.Call(ctx, op)
	if r.Outcome != notStarted {
		t.Fatal(r)
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
