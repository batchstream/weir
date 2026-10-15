//go:build integration

package search

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

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestSearchLuaBatchUsesNativeOCCWithoutBusinessMetadataOrReplay(t *testing.T) {
	for _, mode := range []string{"conflict", "lost_acknowledgement"} {
		t.Run(mode, func(t *testing.T) {
			base, backend := setupSearch(t)
			observedAt := time.Date(2026, time.October, 7, 1, 2, 3, 456789000, time.UTC)
			wantTime := `"` + observedAt.Format(time.RFC3339Nano) + `"`
			for _, id := range []string{"changed", "peer"} {
				status, raw := backend.Do(t, "PUT", "/"+backend.Index+"/_doc/"+id, `{"n":1,"keep":"business"}`)
				if status != http.StatusCreated {
					t.Fatal("fixture write", status, string(raw))
				}
			}
			var reads, writes atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				readAttempt := int32(0)
				writeAttempt := int32(0)
				if strings.HasSuffix(r.URL.Path, "/_mget") {
					readAttempt = reads.Add(1)
					var request struct{ IDs []string }
					if json.Unmarshal(body, &request) != nil {
						t.Error("invalid mget body")
						return
					}
					want := "changed,peer"
					if readAttempt > 1 {
						want = "changed"
					}
					if strings.Join(request.IDs, ",") != want {
						t.Error("Lua conflict reread unrelated peer", request.IDs)
					}
				}
				if r.URL.Path == "/_bulk" {
					writeAttempt = writes.Add(1)
					lines := strings.Split(strings.TrimSpace(string(body)), "\n")
					want := 4
					if writeAttempt > 1 {
						want = 2
						if strings.Contains(string(body), `"_id":"peer"`) {
							t.Error("acknowledged peer was replayed")
						}
					}
					if len(lines) != want || !strings.Contains(lines[0], `"if_seq_no":`) || !strings.Contains(lines[0], `"if_primary_term":`) {
						t.Error("Lua writes did not share native conditional bulk", string(body))
					}
					for i := 1; i < len(lines); i += 2 {
						var source map[string]json.RawMessage
						if json.Unmarshal([]byte(lines[i]), &source) != nil || len(source) != 3 || string(source["keep"]) != `"business"` || string(source["updated_at"]) != wantTime {
							t.Error("custom metadata injected into business source", lines[i])
						}
					}
				}
				request, err := http.NewRequestWithContext(r.Context(), r.Method, backend.URL+r.URL.RequestURI(), strings.NewReader(string(body)))
				if err != nil {
					t.Error(err)
					return
				}
				request.Header = r.Header.Clone()
				request.GetBody = nil
				response, err := backend.Client.Do(request)
				if err != nil {
					t.Error(err)
					return
				}
				defer response.Body.Close()
				raw, err := io.ReadAll(io.LimitReader(response.Body, batchBodyLimit+1))
				if err != nil {
					t.Error(err)
					return
				}
				if mode == "conflict" && readAttempt == 1 {
					status, raw := backend.Do(t, "PUT", "/"+backend.Index+"/_doc/changed", `{"n":10,"keep":"business"}`)
					if status != http.StatusOK {
						t.Error("concurrent direct database write", status, string(raw))
					}
				}
				w.WriteHeader(response.StatusCode)
				if mode == "lost_acknowledgement" && writeAttempt == 1 {
					// Backend has committed both documents. An incomplete acknowledgement
					// must produce UNKNOWN rather than replaying either Lua increment.
					_, _ = io.WriteString(w, `{"errors":false,"took":1,"items":[]}`)
				} else {
					_, _ = w.Write(raw)
				}
			})
			proxy := httptest.NewServer(handler)
			defer proxy.Close()
			config := base.config
			config.URL = proxy.URL
			adapter, err := Open(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			defer adapter.Close()
			works := make([]*execution.Plan, 0, 2)
			for _, id := range []string{"changed", "peer"} {
				work := batchTestPlan(t, adapter, "program", searchResource(backend.Index, id))
				work.Backend.(*plan).program.Source = `return function(current, incoming) current.n = current.n + 1; current.updated_at = weir.time.now(); return current end`
				work.Backend.(*plan).program.ObservedAt = observedAt
				if work.Command.GetScan() != nil || work.Command.GetNative() != nil {
					t.Fatal("Lua plan cannot enter scheduler batch")
				}
				works = append(works, work)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			results := adapter.executeRecords(ctx, works)
			wantCalls := int32(2)
			outcome := pb.MutationOutcome_APPLIED
			code := pb.FailureCode(0)
			if mode == "lost_acknowledgement" {
				wantCalls, outcome, code = 1, pb.MutationOutcome_UNKNOWN, pb.FailureCode_UNAVAILABLE
			}
			if reads.Load() != wantCalls || writes.Load() != wantCalls {
				t.Fatal("Lua conflict subset or unknown no-replay", reads.Load(), writes.Load())
			}
			for i, id := range []string{"changed", "peer"} {
				assertOutcome(t, results[i], outcome, code)
				status, raw := backend.Do(t, "GET", "/"+backend.Index+"/_doc/"+id, "")
				var observed struct {
					Source  map[string]json.RawMessage `json:"_source"`
					Version int                        `json:"_version"`
				}
				wantCount, wantVersion := "2", 2
				if mode == "conflict" && id == "changed" {
					wantCount, wantVersion = "11", 3
				}
				if status != http.StatusOK || json.Unmarshal(raw, &observed) != nil || observed.Version != wantVersion || len(observed.Source) != 3 || string(observed.Source["n"]) != wantCount || string(observed.Source["keep"]) != `"business"` || string(observed.Source["updated_at"]) != wantTime {
					t.Fatal("directly observed business state/version", id, status, string(raw))
				}
			}
			t.Logf("%s mode=%s: two Lua items shared _mget and native conditional _bulk; only confirmed conflicting item retried; business source stayed unchanged in shape", backend.Product, mode)
		})
	}
}
