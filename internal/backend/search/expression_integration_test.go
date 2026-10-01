//go:build integration

package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func searchExpression(t *testing.T, a *Adapter, index, raw string) *execution.Plan {
	t.Helper()
	op := expressionOperation(searchResource(index, "counter"), raw)
	p, f := a.prepareRecord(op)
	if f != nil {
		t.Fatal(f)
	}
	return p
}

func TestSearchExpressionMergeMissingAndNoop(t *testing.T) {
	a, b := setupSearch(t)
	p := searchExpression(t, a, b.Index, `{"doc":{"n":9007199254740993,"nested":{"change":null},"array":[2]}}`)
	assertOutcome(t, runSearch(t, a, p), pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_PRECONDITION_FAILED)
	status, _ := b.Do(t, "GET", "/"+b.Index+"/_doc/counter", "")
	if status != 404 {
		t.Fatal("upsert", status)
	}
	status, _ = b.Do(t, "PUT", "/"+b.Index+"/_doc/counter", `{"n":1,"keep":"untouched","nested":{"keep":1,"change":2},"array":[1,3]}`)
	if status != 201 {
		t.Fatal(status)
	}
	assertOutcome(t, runSearch(t, a, p), pb.MutationOutcome_APPLIED, 0)
	source := runSearch(t, a, searchPlan(t, a, "read", searchResource(b.Index, "counter"))).GetRead().GetDocument().GetData()
	var got map[string]json.RawMessage
	if json.Unmarshal(source, &got) != nil || string(got["n"]) != "9007199254740993" || string(got["keep"]) != `"untouched"` || string(got["array"]) != "[2]" || !strings.Contains(string(got["nested"]), `"keep":1`) || !strings.Contains(string(got["nested"]), `"change":null`) {
		t.Fatal(string(source))
	}
	for _, body := range []string{`{"doc":{}}`, string(p.Backend.(*plan).source)} {
		assertOutcome(t, runSearch(t, a, searchExpression(t, a, b.Index, body)), pb.MutationOutcome_APPLIED, 0)
	}
	assertOutcome(t, runSearch(t, a, searchExpression(t, a, b.Index, `{"doc":{"n":"bad number"}}`)), pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_PRECONDITION_FAILED)
}

func TestSearchExpressionRealReplyFaultsAndNativeCompetition(t *testing.T) {
	for _, mode := range []string{"drop", "truncate", "missing", "redirect", "update", "replace", "delete", "recreate"} {
		t.Run(mode, func(t *testing.T) {
			a, b := setupSearch(t)
			status, _ := b.Do(t, "PUT", "/"+b.Index+"/_doc/counter", `{"n":1,"keep":"old"}`)
			if status != 201 {
				t.Fatal(status)
			}
			var writes, reads atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				isUpdate := r.URL.Path == "/_bulk"
				if strings.Contains(r.URL.Path, "/_doc/") {
					reads.Add(1)
				}
				if isUpdate {
					writes.Add(1)
					body, bodyErr := io.ReadAll(r.Body)
					r.Body = io.NopCloser(bytes.NewReader(body))
					lines := strings.Split(strings.TrimSpace(string(body)), "\n")
					if bodyErr != nil || r.Method != "POST" || r.URL.Query().Get("pipeline") != "_none" || r.Header.Get("Content-Type") != "application/x-ndjson" || len(lines) != 2 || !strings.Contains(lines[0], `"update"`) || !strings.Contains(lines[0], `"retry_on_conflict":0`) || strings.Contains(lines[0], `"pipeline"`) || strings.Contains(lines[1], "upsert") {
						t.Error("unsafe update request", r.URL, string(body))
					}
					switch mode {
					case "update":
						b.Do(t, "POST", "/"+b.Index+"/_update/counter", `{"doc":{"native":true}}`)
					case "replace":
						b.Do(t, "PUT", "/"+b.Index+"/_doc/counter", `{"n":20,"native":true}`)
					case "delete", "recreate":
						b.Do(t, "DELETE", "/"+b.Index+"/_doc/counter", "")
						if mode == "recreate" {
							b.Do(t, "PUT", "/"+b.Index+"/_doc/counter", `{"n":20,"native":true}`)
						}
					case "redirect":
						w.Header().Set("Location", b.URL+r.URL.RequestURI())
						w.WriteHeader(307)
						return
					}
				}
				req, err := http.NewRequestWithContext(r.Context(), r.Method, b.URL+r.URL.RequestURI(), r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				req.Header = r.Header.Clone()
				req.GetBody = nil
				response, err := b.Client.Do(req)
				if err != nil {
					t.Error(err)
					return
				}
				defer response.Body.Close()
				raw, err := io.ReadAll(io.LimitReader(response.Body, metadataLimit+1))
				if err != nil {
					t.Error(err)
					return
				}
				if isUpdate && mode == "drop" {
					if response.StatusCode != 200 {
						t.Error("not real success", response.StatusCode, string(raw))
					}
					conn, _, err := w.(http.Hijacker).Hijack()
					if err == nil {
						conn.Close()
					}
					return
				}
				if isUpdate && mode == "truncate" {
					raw = raw[:len(raw)-1]
				}
				if isUpdate && mode == "missing" {
					var fields map[string]json.RawMessage
					if json.Unmarshal(raw, &fields) != nil {
						t.Error("reply")
					}
					var items []map[string]map[string]json.RawMessage
					if json.Unmarshal(fields["items"], &items) != nil || len(items) != 1 {
						t.Error("bulk update items")
					} else {
						delete(items[0]["update"], "_seq_no")
						fields["items"], _ = json.Marshal(items)
					}
					raw, _ = json.Marshal(fields)
				}
				w.WriteHeader(response.StatusCode)
				_, _ = w.Write(raw)
			})
			proxy := httptest.NewServer(handler)
			defer proxy.Close()
			cfg := a.config
			cfg.URL = proxy.URL
			through, err := Open(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer through.Close()
			p := searchExpression(t, through, b.Index, `{"doc":{"added":true}}`)
			result := runSearch(t, through, p)
			switch mode {
			case "drop", "truncate", "missing", "redirect":
				assertOutcome(t, result, pb.MutationOutcome_UNKNOWN, pb.FailureCode_UNAVAILABLE)
			case "delete":
				assertOutcome(t, result, pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_PRECONDITION_FAILED)
			default:
				assertOutcome(t, result, pb.MutationOutcome_APPLIED, 0)
			}
			if writes.Load() != 1 || reads.Load() != 0 {
				t.Fatal("read-back or replay", writes.Load(), reads.Load())
			}
			status, raw := b.Do(t, "GET", "/"+b.Index+"/_doc/counter", "")
			if mode == "delete" {
				if status != 404 {
					t.Fatal(status)
				}
				return
			}
			if status != 200 || mode != "redirect" && !strings.Contains(string(raw), `"added":true`) {
				t.Fatal(status, string(raw))
			}
			if (mode == "replace" || mode == "recreate" || mode == "update") && !strings.Contains(string(raw), `"native":true`) {
				t.Fatal("lost native data", string(raw))
			}
		})
	}
}

func TestSearchExpressionPipelineAndSourceQualification(t *testing.T) {
	a, b := setupSearch(t)
	pipeline := b.Index + "_expression"
	status, _ := b.Do(t, "PUT", "/_ingest/pipeline/"+pipeline, `{"processors":[{"set":{"field":"pipeline_marker","value":true}}]}`)
	if status != 200 {
		t.Fatal(status)
	}
	t.Cleanup(func() {
		b.Do(t, "PUT", "/"+b.Index+"/_settings", `{"index.default_pipeline":"_none","index.final_pipeline":"_none"}`)
		b.Do(t, "DELETE", "/_ingest/pipeline/"+pipeline, "")
	})
	b.Do(t, "PUT", "/"+b.Index+"/_doc/counter", `{"n":1}`)
	for _, setting := range []string{"default", "final"} {
		t.Run(setting, func(t *testing.T) {
			// Set the single pipeline under test, preserving both API distinctions.
			values := map[string]string{"index.default_pipeline": "_none", "index.final_pipeline": "_none"}
			values["index."+setting+"_pipeline"] = pipeline
			raw, _ := json.Marshal(values)
			settings := string(raw)
			status, _ := b.Do(t, "PUT", "/"+b.Index+"/_settings", settings)
			if status != 200 {
				t.Fatal(status)
			}
			assertOutcome(t, runSearch(t, a, searchExpression(t, a, b.Index, `{"doc":{"expression":true}}`)), pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_UNSUPPORTED)
			source := runSearch(t, a, searchPlan(t, a, "read", searchResource(b.Index, "counter"))).GetRead().GetDocument().GetData()
			if strings.Contains(string(source), "expression") {
				t.Fatal("disallowed update sent")
			}
			status, reply := b.Do(t, "POST", "/"+b.Index+"/_update/counter?pipeline=_none", `{"doc":{"probe":true}}`)
			if status != 400 {
				t.Fatal("unexpected Update bypass support", status, string(reply))
			}
			body := `{"doc":{"native_probe":"` + setting + `","pipeline_marker":false}}`
			status, reply = b.Do(t, "POST", "/"+b.Index+"/_update/counter", body)
			if status != 200 {
				t.Fatal(status, string(reply))
			}
			_, observed := b.Do(t, "GET", "/"+b.Index+"/_doc/counter", "")
			t.Logf("%s %s native Update pipeline marker=%v; pipeline=_none rejected", b.Product, setting, strings.Contains(string(observed), `"pipeline_marker":true`))
			// Ordinary APIs keep their separate established pipeline policy.
			if setting == "default" {
				assertOutcome(t, runSearch(t, a, searchPlan(t, a, "put", searchResource(b.Index, "ordinary"))), pb.MutationOutcome_APPLIED, 0)
			}
		})
	}
	for _, kind := range []string{"disabled", "pruned"} {
		t.Run(kind, func(t *testing.T) {
			index := b.Index + "_" + kind
			source := `{"enabled":false}`
			if kind == "pruned" {
				source = `{"excludes":["hidden"]}`
			}
			b.Create(t, index, `{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"_source":`+source+`}}`)
			assertOutcome(t, runSearch(t, a, searchExpression(t, a, index, `{"doc":{}}`)), pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_UNSUPPORTED)
		})
	}
}

func TestSearchExpressionRealCapacity(t *testing.T) {
	a, b := setupSearch(t)
	body := `{"n":0,"pad":"` + strings.Repeat("x", 200<<10) + `"}`
	status, _ := b.Do(t, "PUT", "/"+b.Index+"/_doc/counter", body)
	if status != 201 {
		t.Fatal(status)
	}
	var applied, conflicts, congested atomic.Int32
	for wave := 0; wave < 8 && congested.Load() == 0; wave++ {
		start := make(chan struct{})
		var group sync.WaitGroup
		for i := 0; i < 8; i++ {
			p := searchExpression(t, a, b.Index, fmt.Sprintf(`{"doc":{"n":%d}}`, wave*8+i+1))
			group.Go(func() {
				<-start
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				rs, sample := a.executeRecords(ctx, []*execution.Plan{p})
				r := rs[0].GetMutation()
				switch {
				case r.Outcome == pb.MutationOutcome_APPLIED:
					applied.Add(1)
				case r.Outcome == pb.MutationOutcome_NOT_APPLIED && r.GetFailure().GetCode() == pb.FailureCode_CONFLICT:
					conflicts.Add(1)
					if sample != execution.Neutral {
						t.Error("conflict congestion")
					}
				case r.Outcome == pb.MutationOutcome_NOT_APPLIED && r.GetFailure().GetCode() == pb.FailureCode_UNAVAILABLE && sample == execution.Congested:
					congested.Add(1)
				default:
					t.Error("unexpected contention result", r, sample)
				}
			})
		}
		close(start)
		group.Wait()
	}
	t.Logf("real Update contention applied=%d conflict=%d congestion=%d", applied.Load(), conflicts.Load(), congested.Load())
	if applied.Load() == 0 || congested.Load() == 0 {
		t.Fatal("real Update capacity rejection not exercised")
	}
}

func TestSearchExpressionRealConflictEvidence(t *testing.T) {
	a, b := setupSearch(t)
	b.Do(t, "PUT", "/"+b.Index+"/_doc/counter", `{"n":0}`)
	_, raw := b.Do(t, "GET", "/"+b.Index+"/_doc/counter", "")
	var prior struct {
		Seq  int64 `json:"_seq_no"`
		Term int64 `json:"_primary_term"`
	}
	if json.Unmarshal(raw, &prior) != nil {
		t.Fatal("prior")
	}
	// A controlled stale native OCC condition obtains an actual backend 409.
	// The public profile does NOT send a caller-selected OCC condition. This
	// qualifies error evidence, not a claim that the proxy paused inside Update.
	b.Do(t, "PUT", "/"+b.Index+"/_doc/counter", `{"n":10}`)
	path := fmt.Sprintf("/%s/_update/counter?retry_on_conflict=0&if_seq_no=%d&if_primary_term=%d", b.Index, prior.Seq, prior.Term)
	code, raw := b.Do(t, "POST", path, `{"doc":{"n":20}}`)
	if code != 409 {
		t.Fatal(code, string(raw))
	}
	n := &plan{id: "counter"}
	opts := expressionReplyOptions{native: n, status: code, raw: raw}
	result, sample := a.expressionReply(opts)
	if result.Outcome != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_CONFLICT || sample != execution.Neutral {
		t.Fatal(result, sample)
	}
	_, after := b.Do(t, "GET", "/"+b.Index+"/_doc/counter", "")
	if !strings.Contains(string(after), `"n":10`) {
		t.Fatal("rejected update persisted", string(after))
	}
}
