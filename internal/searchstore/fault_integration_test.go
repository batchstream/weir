//go:build integration

package searchstore

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestSearchCommittedReplyLossAndIncompleteBulk(t *testing.T) {
	for _, mode := range []string{"drop", "truncate", "missing_item", "reordered", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			base, b := setupSearch(t)
			var calls atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/_bulk" {
					calls.Add(1)
					if mode == "redirect" {
						w.Header().Set("Location", b.URL+r.URL.RequestURI())
						w.WriteHeader(307)
						return
					}
				}
				request, err := http.NewRequestWithContext(r.Context(), r.Method, b.URL+r.URL.RequestURI(), r.Body)
				if err != nil {
					t.Error(err)
					w.WriteHeader(502)
					return
				}
				request.Header = r.Header.Clone()
				response, err := b.Client.Do(request)
				if err != nil {
					t.Error(err)
					w.WriteHeader(502)
					return
				}
				defer response.Body.Close()
				body, err := io.ReadAll(io.LimitReader(response.Body, responseLimit+1))
				if err != nil {
					t.Error(err)
					w.WriteHeader(502)
					return
				}
				if r.URL.Path == "/_bulk" {
					switch mode {
					case "drop":
						connection, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						_ = connection.Close()
						return
					case "truncate":
						body = body[:len(body)/2]
					case "missing_item", "reordered":
						var parsed map[string]json.RawMessage
						if err := json.Unmarshal(body, &parsed); err != nil {
							t.Error(err)
							w.WriteHeader(502)
							return
						}
						var items []json.RawMessage
						if err := json.Unmarshal(parsed["items"], &items); err != nil {
							t.Error(err)
							w.WriteHeader(502)
							return
						}
						if len(items) != 2 {
							t.Error("expected two real native results")
							w.WriteHeader(502)
							return
						}
						if mode == "missing_item" {
							items = items[:1]
						} else {
							items[0], items[1] = items[1], items[0]
						}
						parsed["items"], _ = json.Marshal(items)
						body, _ = json.Marshal(parsed)
					}
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(response.StatusCode)
				_, _ = w.Write(body)
			})
			proxy := httptest.NewServer(handler)
			defer proxy.Close()
			cfg := base.config
			cfg.URL = proxy.URL
			a, err := Open(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			// Startup establishes a reusable connection before the ambiguous POST.
			first := searchPlan(t, a, "put", "first")
			second := searchPlan(t, a, "put", "second")
			second.Operation.Index = 1
			works := []*execution.Plan{first, second}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			results, _ := a.Execute(ctx, works)
			for _, result := range results {
				assertOutcome(t, result, pb.MutationOutcome_UNKNOWN, pb.FailureCode_UNAVAILABLE)
			}
			if calls.Load() != 1 {
				t.Fatal("ambiguous mutation replayed", calls.Load())
			}
			for _, id := range []string{"first", "second"} {
				status, raw := b.Do(t, "GET", "/"+b.Index+"/_doc/"+id, "")
				if mode == "redirect" {
					if status != 404 {
						t.Fatal("redirect followed", status)
					}
					continue
				}
				var observed struct {
					Version int `json:"_version"`
					Found   bool
				}
				if status != 200 || json.Unmarshal(raw, &observed) != nil || !observed.Found || observed.Version != 1 {
					t.Fatal("real exactly-one write not observed", status, string(raw))
				}
			}
		})
	}
}

func TestSearchReplaceNativeCompetition(t *testing.T) {
	for _, mode := range []string{"update", "delete", "delete_recreate"} {
		t.Run(mode, func(t *testing.T) {
			base, b := setupSearch(t)
			assertOutcome(t, runSearch(t, base, searchPlan(t, base, "put", "race")), pb.MutationOutcome_APPLIED, 0)
			var raced atomic.Bool
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				request, err := http.NewRequestWithContext(r.Context(), r.Method, b.URL+r.URL.RequestURI(), r.Body)
				if err != nil {
					t.Error(err)
					w.WriteHeader(502)
					return
				}
				request.Header = r.Header.Clone()
				response, err := b.Client.Do(request)
				if err != nil {
					t.Error(err)
					w.WriteHeader(502)
					return
				}
				defer response.Body.Close()
				raw, err := io.ReadAll(io.LimitReader(response.Body, responseLimit))
				if err != nil {
					t.Error(err)
					w.WriteHeader(502)
					return
				}
				if strings.Contains(r.URL.Path, "/_doc/race") && r.Method == "GET" && raced.CompareAndSwap(false, true) {
					if mode != "update" {
						status, _ := b.Do(t, "DELETE", "/"+b.Index+"/_doc/race", "")
						if status != 200 {
							t.Error("native delete", status)
						}
					}
					if mode != "delete" {
						status, _ := b.Do(t, "PUT", "/"+b.Index+"/_doc/race", `{"n":7,"native":true}`)
						if status != 200 && status != 201 {
							t.Error("native write", status)
						}
					}
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(response.StatusCode)
				_, _ = w.Write(raw)
			})
			proxy := httptest.NewServer(handler)
			defer proxy.Close()
			cfg := base.config
			cfg.URL = proxy.URL
			a, err := Open(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			result := runSearch(t, a, searchPlan(t, a, "replace", "race"))
			assertOutcome(t, result, pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_CONFLICT)
			status, raw := b.Do(t, "GET", "/"+b.Index+"/_doc/race", "")
			if mode == "delete" {
				if status != 404 {
					t.Fatal("Replace recreated deleted record", string(raw))
				}
			} else if status != 200 || !strings.Contains(string(raw), `"native":true`) {
				t.Fatal("native writer overwritten", string(raw))
			}
			if !raced.Load() {
				t.Fatal("competition not exercised")
			}
		})
	}
}

func TestSearchIngestAndQualification(t *testing.T) {
	a, b := setupSearch(t)
	target := b.Index + "_target"
	b.Create(t, target, `{"settings":{"number_of_shards":1,"number_of_replicas":0}}`)
	pipeline := b.Index + "_retarget"
	body := `{"processors":[{"set":{"field":"_index","value":"` + target + `"}},{"set":{"field":"changed","value":true}}]}`
	status, _ := b.Do(t, "PUT", "/_ingest/pipeline/"+pipeline, body)
	if status != 200 {
		t.Fatal(status)
	}
	t.Cleanup(func() {
		_, _ = b.Do(t, "PUT", "/"+b.Index+"/_settings", `{"index.default_pipeline":"_none","index.final_pipeline":"_none"}`)
		status, _ := b.Do(t, "DELETE", "/_ingest/pipeline/"+pipeline, "")
		if status != 200 {
			t.Error(status)
		}
	})
	status, _ = b.Do(t, "PUT", "/"+b.Index+"/_settings", `{"index.default_pipeline":"`+pipeline+`"}`)
	if status != 200 {
		t.Fatal(status)
	}
	status, _ = b.Do(t, "PUT", "/"+b.Index+"/_doc/native", `{"n":1}`)
	if status != 201 {
		t.Fatal(status)
	}
	status, raw := b.Do(t, "GET", "/"+target+"/_doc/native", "")
	if status != 200 || !strings.Contains(string(raw), `"changed":true`) {
		t.Fatal("native default pipeline not exercised", status, string(raw))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	create := searchPlan(t, a, "create", "direct")
	put := searchPlan(t, a, "put", "other")
	put.Operation.Index = 1
	works := []*execution.Plan{create, put}
	results, _ := a.Execute(ctx, works)
	for _, result := range results {
		assertOutcome(t, result, pb.MutationOutcome_APPLIED, 0)
	}
	assertOutcome(t, runSearch(t, a, searchPlan(t, a, "replace", "direct")), pb.MutationOutcome_APPLIED, 0)
	for _, id := range []string{"direct", "other"} {
		result := runSearch(t, a, searchPlan(t, a, "read", id)).GetRead()
		if result.GetDocument() == nil || strings.Contains(string(result.GetDocument().Data), "changed") {
			t.Fatal("default pipeline touched Weir source", result)
		}
		status, _ = b.Do(t, "GET", "/"+target+"/_doc/"+id, "")
		if status != 404 {
			t.Fatal("Weir write retargeted")
		}
	}
	status, _ = b.Do(t, "PUT", "/"+b.Index+"/_settings", `{"index.final_pipeline":"`+pipeline+`"}`)
	if status != 200 {
		t.Fatal(status)
	}
	for _, action := range []string{"put", "create", "replace"} {
		assertOutcome(t, runSearch(t, a, searchPlan(t, a, action, "direct")), pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_UNSUPPORTED)
	}
	if runSearch(t, a, searchPlan(t, a, "read", "direct")).GetRead().GetDocument() == nil {
		t.Fatal("final pipeline disabled reads")
	}
	assertOutcome(t, runSearch(t, a, searchPlan(t, a, "delete", "direct")), pb.MutationOutcome_APPLIED, 0)
	// The same no-pipeline flag does not skip final ingest for direct native callers.
	status, raw = b.Do(t, "PUT", "/"+b.Index+"/_doc/final?pipeline=_none", `{"n":1}`)
	if status < 400 {
		t.Fatal("final retarget pipeline unexpectedly bypassed", status, string(raw))
	}
	for _, spec := range []struct{ name, body string }{
		{"source", `{"mappings":{"_source":{"enabled":false}}}`},
		{"pruned", `{"mappings":{"_source":{"excludes":["hidden"]}}}`},
		{"routing", `{"mappings":{"_routing":{"required":true}}}`},
		{"shards", `{"settings":{"number_of_shards":2,"number_of_replicas":0}}`},
	} {
		t.Run(spec.name, func(t *testing.T) {
			index := b.Index + "_" + spec.name
			b.Create(t, index, spec.body)
			cfg := a.config
			cfg.Index = index
			other, err := Open(context.Background(), cfg)
			if spec.name == "routing" || spec.name == "shards" {
				if err == nil {
					_ = other.Close()
					t.Fatal("required routing accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			if runSearch(t, other, searchPlan(t, other, "read", "a")).GetRead().GetFailure().GetCode() != pb.FailureCode_UNSUPPORTED {
				t.Fatal("lossy source read allowed")
			}
			assertOutcome(t, runSearch(t, other, searchPlan(t, other, "put", "a")), pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_UNSUPPORTED)
			assertOutcome(t, runSearch(t, other, searchPlan(t, other, "delete", "a")), pb.MutationOutcome_APPLIED, 0)
		})
	}
	alias := b.Index + "_alias"
	status, _ = b.Do(t, "POST", "/_aliases", `{"actions":[{"add":{"index":"`+b.Index+`","alias":"`+alias+`"}}]}`)
	if status != 200 {
		t.Fatal(status)
	}
	cfg := a.config
	cfg.Index = alias
	invalid, err := Open(context.Background(), cfg)
	if err == nil {
		_ = invalid.Close()
		t.Fatal("alias accepted")
	}
	cfg = a.config
	cfg.Profile = "opensearch-2.19.0"
	if a.config.Profile == cfg.Profile {
		cfg.Profile = "elasticsearch-8.17.0"
	}
	invalid, err = Open(context.Background(), cfg)
	if err == nil {
		_ = invalid.Close()
		t.Fatal("wrong product accepted")
	}
}
