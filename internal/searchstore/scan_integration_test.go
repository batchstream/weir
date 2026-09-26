//go:build integration

package searchstore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testsearch"
)

func scanWork(t *testing.T, a *Adapter, hint uint32) *execution.Plan {
	t.Helper()
	req := &pb.ScanRequest{Resource: "weir://search/" + a.config.Index, FetchItemsHint: hint}
	p, f := a.PrepareScan(req)
	if f != nil {
		t.Fatal(f)
	}
	return p
}
func TestSearchScanTraversal(t *testing.T) {
	for _, size := range []int{0, 1, 8, 35} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			a, b := setupSearch(t)
			for i := 0; i < size; i++ {
				status, _ := b.Do(t, "PUT", fmt.Sprintf("/%s/_doc/%d", b.Index, i), `{"n":9223372036854775807,"kind":"scan"}`)
				if status != 201 {
					t.Fatal(status)
				}
			}
			b.Do(t, "POST", "/"+b.Index+"/_refresh", "")
			p := scanWork(t, a, 8)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			defer a.CloseScan(ctx, p)
			seen := map[string]bool{}
			for calls := 0; ; calls++ {
				if calls > 20 {
					t.Fatal("did not exhaust")
				}
				page, _ := a.FetchScan(ctx, p)
				if page.Failure != nil {
					t.Fatalf("fetch %d: %v", calls, page.Failure)
				}
				for _, doc := range page.Documents {
					var hit struct {
						ID     string          `json:"_id"`
						Index  string          `json:"_index"`
						Source json.RawMessage `json:"_source"`
						Sort   []int64
					}
					if json.Unmarshal(doc.Data, &hit) != nil || hit.ID == "" || hit.Index != b.Index || len(hit.Sort) != 1 || !strings.Contains(string(hit.Source), "9223372036854775807") || seen[hit.ID] {
						t.Fatalf("hit fidelity/duplicate: %s", doc.Data)
					}
					seen[hit.ID] = true
				}
				if page.Exhausted {
					break
				}
			}
			if len(seen) != size {
				t.Fatal("omissions", len(seen), size)
			}
			if f := a.CloseScan(ctx, p); f != nil {
				t.Fatal(f)
			}
			if f := a.CloseScan(ctx, p); f != nil {
				t.Fatal(f)
			}
			t.Logf("%s: complete native hit traversal %d records", b.Profile, size)
		})
	}
}

type scanProxyFault struct {
	target, mode string
	at           int32
}

func scanFaultProxy(t *testing.T, b *testsearch.Backend, fault scanProxyFault) (string, *atomic.Int32) {
	t.Helper()
	calls := &atomic.Int32{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := r.URL.Path == "/_search" && fault.target == "fetch" || r.Method == http.MethodPost && (strings.HasSuffix(r.URL.Path, "/_pit") || strings.HasSuffix(r.URL.Path, "/_search/point_in_time")) && fault.target == "open"
		number := int32(0)
		if target {
			number = calls.Add(1)
		}
		request, err := http.NewRequestWithContext(r.Context(), r.Method, b.URL+r.URL.RequestURI(), r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(502)
			return
		}
		request.Header = r.Header.Clone()
		request.GetBody = nil
		response, err := b.Client.Do(request)
		if err != nil {
			w.WriteHeader(502)
			return
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(response.Body, responseLimit+1))
		if err != nil {
			w.WriteHeader(502)
			return
		}
		if target && number == fault.at {
			if fault.mode == "block" {
				select {
				case <-r.Context().Done():
					return
				case <-time.After(500 * time.Millisecond):
				}
			}
			if fault.mode == "drop" {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				_ = conn.Close()
				return
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Error(err)
				w.WriteHeader(502)
				return
			}
			switch fault.mode {
			case "timeout":
				fields["timed_out"] = json.RawMessage(`true`)
			case "early":
				fields["terminated_early"] = json.RawMessage(`true`)
			case "shards":
				fields["_shards"] = json.RawMessage(`{"total":1,"successful":0,"skipped":0,"failed":1,"failures":[{"reason":{"type":"unavailable_shards_exception"}}]}`)
			case "missing":
				delete(fields, "_shards")
			case "hits_missing":
				fields["hits"] = json.RawMessage(`{"max_score":null}`)
			case "error_tail":
				fields["error"] = json.RawMessage(`{"type":"late_error"}`)
			}
			raw, _ = json.Marshal(fields)
			if fault.mode == "truncate" {
				raw = raw[:len(raw)-1]
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(raw)
	})
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)
	return proxy.URL, calls
}
func TestSearchScanFaultPages(t *testing.T) {
	for _, target := range []string{"open", "fetch"} {
		for _, at := range []int32{1, 2} {
			if target == "open" && at == 2 {
				continue
			}
			for _, mode := range []string{"timeout", "early", "shards", "missing", "hits_missing", "error_tail", "truncate", "drop"} {
				if target == "open" && mode == "hits_missing" {
					continue
				}
				t.Run(fmt.Sprintf("%s_%d_%s", target, at, mode), func(t *testing.T) {
					base, b := setupSearch(t)
					for i := 0; i < 3; i++ {
						b.Do(t, "PUT", fmt.Sprintf("/%s/_doc/%d", b.Index, i), `{"n":1}`)
					}
					b.Do(t, "POST", "/"+b.Index+"/_refresh", "")
					fault := scanProxyFault{target: target, mode: mode, at: at}
					endpoint, calls := scanFaultProxy(t, b, fault)
					cfg := base.config
					cfg.URL = endpoint
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					a, err := Open(ctx, cfg)
					if err != nil {
						t.Fatal(err)
					}
					defer a.Close()
					p := scanWork(t, a, 1)
					defer a.CloseScan(ctx, p)
					count := 0
					failed := false
					for i := 0; i < 4; i++ {
						page, _ := a.FetchScan(ctx, p)
						if page.Failure != nil {
							if len(page.Documents) != 0 || page.Exhausted {
								t.Fatal("failed page leaked hits", page)
							}
							failed = true
							break
						}
						count += len(page.Documents)
					}
					want := 0
					if target == "fetch" {
						want = int(at) - 1
					}
					if !failed || count != want || calls.Load() != at {
						t.Fatal("fault/retry contract", failed, count, want, calls.Load())
					}
					_ = a.CloseScan(ctx, p)
					// An open reply loss can hide the new ID; its finite keep_alive is the
					// remote fallback. This test does not assert zero remote residue.
				})
			}
		}
	}
}
func TestSearchScanPITInvalidationAndCancellation(t *testing.T) {
	a, b := setupSearch(t)
	for i := 0; i < 3; i++ {
		b.Do(t, "PUT", fmt.Sprintf("/%s/_doc/%d", b.Index, i), `{"n":1}`)
	}
	b.Do(t, "POST", "/"+b.Index+"/_refresh", "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p := scanWork(t, a, 1)
	defer a.CloseScan(ctx, p)
	page, _ := a.FetchScan(ctx, p)
	if page.Failure != nil {
		t.Fatal(page)
	}
	n := p.Backend.(*scanPlan)
	endpoint := "/_pit"
	body := map[string]any{"id": n.pit}
	if b.Profile == "opensearch-2.19.0" {
		endpoint = "/_search/point_in_time"
		body = map[string]any{"pit_id": []string{n.pit}}
	}
	raw, _ := json.Marshal(body)
	status, _ := b.Do(t, "DELETE", endpoint, string(raw))
	if status != 200 {
		t.Fatal("native PIT invalidation", status)
	}
	page, _ = a.FetchScan(ctx, p)
	if page.Failure == nil || len(page.Documents) != 0 {
		t.Fatal("expired PIT silently restarted", page)
	}
	_ = a.CloseScan(ctx, p)
	for _, stage := range []string{"open", "fetch"} {
		p := scanWork(t, a, 1)
		if stage == "fetch" {
			page, _ := a.FetchScan(ctx, p)
			if page.Failure != nil {
				t.Fatal(page)
			}
		}
		stopped, stop := context.WithCancel(ctx)
		stop()
		page, _ := a.FetchScan(stopped, p)
		if page.Failure == nil || len(page.Documents) != 0 {
			t.Fatal("cancel ignored")
		}
		_ = a.CloseScan(ctx, p)
	}
}

func TestSearchScanNativeQueryWithFinalPipeline(t *testing.T) {
	a, b := setupSearch(t)
	pipeline := b.Index + "_scan"
	status, _ := b.Do(t, "PUT", "/_ingest/pipeline/"+pipeline, `{"processors":[{"set":{"field":"pipeline_native","value":true}}]}`)
	if status != 200 {
		t.Fatal(status)
	}
	t.Cleanup(func() { b.Do(t, "DELETE", "/_ingest/pipeline/"+pipeline, "") })
	settings := fmt.Sprintf(`{"index.final_pipeline":%q}`, pipeline)
	status, _ = b.Do(t, "PUT", "/"+b.Index+"/_settings", settings)
	if status != 200 {
		t.Fatal(status)
	}
	for i := 0; i < 3; i++ {
		b.Do(t, "PUT", fmt.Sprintf("/%s/_doc/%d", b.Index, i), fmt.Sprintf(`{"n":%d}`, i))
	}
	b.Do(t, "POST", "/"+b.Index+"/_refresh", "")
	selector := &pb.Document{MediaType: "application/json", Data: []byte(`{"query":{"range":{"n":{"gte":1}}}}`)}
	req := &pb.ScanRequest{Resource: "weir://search/" + b.Index, Selector: selector, FetchItemsHint: 1}
	p, f := a.PrepareScan(req)
	if f != nil {
		t.Fatal(f)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer a.CloseScan(ctx, p)
	count := 0
	for step := 0; ; step++ {
		if step > 5 {
			t.Fatal("did not exhaust")
		}
		page, _ := a.FetchScan(ctx, p)
		if page.Failure != nil {
			t.Fatal(page.Failure)
		}
		for _, doc := range page.Documents {
			if !strings.Contains(string(doc.Data), `"pipeline_native":true`) {
				t.Fatal("native source missing pipeline effect")
			}
			count++
		}
		if page.Exhausted {
			break
		}
	}
	if count != 2 {
		t.Fatal("native query semantics", count)
	}
	if f := a.CloseScan(ctx, p); f != nil {
		t.Fatal(f)
	}
}

func TestSearchScanCancelInFlight(t *testing.T) {
	for _, target := range []string{"open", "fetch"} {
		t.Run(target, func(t *testing.T) {
			base, b := setupSearch(t)
			fault := scanProxyFault{target: target, mode: "block", at: 1}
			endpoint, calls := scanFaultProxy(t, b, fault)
			cfg := base.config
			cfg.URL = endpoint
			a, err := Open(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			p := scanWork(t, a, 1)
			if target == "fetch" {
				ctx, stop := context.WithTimeout(context.Background(), time.Second)
				page, _ := a.FetchScan(ctx, p)
				stop()
				if page.Failure != nil {
					t.Fatal(page)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			ended := make(chan *execution.ScanPage, 1)
			go func() { page, _ := a.FetchScan(ctx, p); ended <- page }()
			until := time.Now().Add(700 * time.Millisecond)
			for calls.Load() == 0 && time.Now().Before(until) {
				time.Sleep(time.Millisecond)
			}
			if calls.Load() != 1 {
				cancel()
				t.Fatal("backend request did not start")
			}
			// Proxy has observed the actual request, not just a queued local operation.
			time.Sleep(20 * time.Millisecond)
			cancel()
			select {
			case page := <-ended:
				if page.Failure == nil || len(page.Documents) != 0 {
					t.Fatal("cancelled fetch succeeded", page)
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("cancelled backend I/O did not stop")
			}
			cleanup, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			failure := a.CloseScan(cleanup, p)
			if target == "open" && failure == nil {
				t.Fatal("lost allocation ID was reported as confirmed cleanup")
			}
			if target == "fetch" && failure != nil {
				t.Fatal("known PIT cleanup failed", failure)
			}
			if calls.Load() != 1 {
				t.Fatal("cancelled fetch retried", calls.Load())
			}
		})
	}
}
