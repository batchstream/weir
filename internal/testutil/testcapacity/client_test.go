package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFullResponseAndNoMutationRetry(t *testing.T) {
	var calls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Length", "1024")
		fmt.Fprint(w, `{"errors":false`)
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	c, err := newClient(server.URL, "", 62)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	op := Operation{ID: "test", Write: true}
	res := c.Execute(context.Background(), op)
	if res.Outcome != unknown || calls.Load() != 1 {
		t.Fatal(res, calls.Load())
	}
}

func TestOversizeAndBulkCorrelation(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, strings.Repeat("x", (256<<10)+1)) })
	server := httptest.NewServer(handler)
	defer server.Close()
	c, err := newClient(server.URL, "", 62)
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

func TestKnownNativeRejectionEvidence(t *testing.T) {
	for _, code := range []int{429, 503} {
		kind := "es_rejected_execution_exception"
		if code == 503 {
			kind = "unavailable_shards_exception"
		}
		envelope := []byte(fmt.Sprintf(`{"error":{"type":%q}}`, kind))
		r := parseBulk(code, envelope, "test")
		if r.Outcome != notApplied || r.Class != "backend_rejected_not_applied" {
			t.Fatal("known envelope rejection", r)
		}
		item := []byte(fmt.Sprintf(`{"errors":true,"took":0,"items":[{"index":{"_index":"records","_id":"test","status":%d,"error":{"type":%q}}}]}`, code, kind))
		r = parseBulk(200, item, "test")
		if r.Outcome != notApplied || r.Class != "backend_rejected_not_applied" {
			t.Fatal("known per-item rejection", r)
		}
		other := []byte(`{"error":{"type":"unqualified_error"}}`)
		r = parseBulk(code, other, "test")
		if r.Outcome != unknown {
			t.Fatal("unqualified rejection acquired application certainty", r)
		}
	}
}

func TestDeadlineBeforeDispatchNotUnknown(t *testing.T) {
	c, err := newClient("http://127.0.0.1:1", "", 62)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	op := Operation{Write: true}
	r := c.Execute(ctx, op)
	if r.Outcome != notStarted {
		t.Fatal(r)
	}
}

func TestWeirMongoClientDoesNotOpenDatabasePool(t *testing.T) {
	// The unusable Mongo URI must never reach the driver on a Weir-only path.
	c, err := newClient("mongodb://not a valid authority", "127.0.0.1:1", 4)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if !c.MongoBackend || c.Mongo != nil || len(c.RPC) != 4 {
		t.Fatal("Weir caller retained a direct database client")
	}
}
