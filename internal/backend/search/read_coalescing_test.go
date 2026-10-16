package search

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

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/execution"
)

func TestMgetDuplicateReadsShareOneObservationAcrossReadWindows(t *testing.T) {
	var reads atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/records" {
			_, _ = io.WriteString(w, testIndexReply)
			return
		}
		reads.Add(1)
		var request struct {
			IDs []string `json:"ids"`
		}
		if r.URL.Path != "/records/_mget" || json.NewDecoder(r.Body).Decode(&request) != nil || len(request.IDs) != 1 || request.IDs[0] != "same" {
			t.Error("duplicate reads reached the backend", r.URL.Path, request.IDs)
			return
		}
		_, _ = io.WriteString(w, `{"docs":[{"_index":"records","_id":"same","found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":9007199254740993}}]}`)
	})
	adapter := testHTTPAdapter(t, handler)
	settings := backend.DefaultOptions()
	settings.ExchangeBytes, settings.BatchBytes = 128<<10, 32<<10
	adapter.config.Options = &settings
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	works := make([]*execution.Plan, 65)
	for i := range works {
		works[i] = batchTestPlan(t, adapter, "read", "records/s:same")
	}
	works[0].Context = cancelled
	results := adapter.executeRecords(t.Context(), works)
	if results[0].GetReadResult().GetFailure().GetCode() != pb.FailureCode_CANCELLED {
		t.Fatal("cancelled duplicate lost its caller result", results[0])
	}
	for _, result := range results[1:] {
		if string(result.GetReadResult().GetDocument().GetData()) != `{"n":9007199254740993}` {
			t.Fatal("duplicate lost shared source or exact numbers", result)
		}
	}
	if reads.Load() != 1 {
		t.Fatal("duplicate callers consumed separate source windows", reads.Load())
	}
}

func TestMgetCrossIndexDedupKeepsItemFailuresAndCallerPositions(t *testing.T) {
	var reads atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads.Add(1)
		var request struct {
			Docs []struct {
				Index string `json:"_index"`
				ID    string `json:"_id"`
			} `json:"docs"`
		}
		if r.URL.Path != "/_mget" || r.URL.Query().Get("realtime") != "true" || json.NewDecoder(r.Body).Decode(&request) != nil || len(request.Docs) != 2 || request.Docs[0].Index != "left" || request.Docs[1].Index != "right" {
			t.Error("unexpected cross-index read", r.URL.Path, request)
			return
		}
		_, _ = io.WriteString(w, `{"docs":[{"_index":"left","_id":"same","found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1}},{"_index":"right","_id":"same","status":403,"error":{"type":"security_exception"}}]}`)
	})
	adapter := testHTTPAdapter(t, handler)
	works := []*execution.Plan{
		batchTestPlan(t, adapter, "read", "left/s:same"),
		batchTestPlan(t, adapter, "read", "right/s:same"),
		batchTestPlan(t, adapter, "read", "left/s:same"),
		batchTestPlan(t, adapter, "read", "right/s:same"),
	}
	results := adapter.mget(t.Context(), works)
	for i, result := range results {
		if i%2 == 0 {
			if result.failure != nil || string(result.reply.Source) != `{"n":1}` {
				t.Fatal("healthy source crossed index or inherited sibling failure", i, result)
			}
		} else if result.failure.GetCode() != pb.FailureCode_PERMISSION_DENIED || result.reply != nil {
			t.Fatal("per-index failure lost correspondence", i, result)
		}
	}
	if reads.Load() != 1 {
		t.Fatal("cross-index read was split or replayed", reads.Load())
	}
}

func coalescingReadResponse(ids []string, source string) string {
	var reply strings.Builder
	reply.WriteString(`{"docs":[`)
	for i, id := range ids {
		if i != 0 {
			reply.WriteByte(',')
		}
		_, _ = fmt.Fprintf(&reply, `{"_index":"records","_id":%q,"found":true,"_seq_no":1,"_primary_term":1,"_source":%s}`, id, source)
	}
	reply.WriteString("]}")
	return reply.String()
}

func TestPureReadUsesActualResponseByteWindow(t *testing.T) {
	cases := []struct {
		name               string
		sourceBytes, count int
		belowBoundary      bool
		wantRequests       int32
	}{
		{name: "many small sources", sourceBytes: 128, count: 128, wantRequests: 1},
		{name: "exact boundary", sourceBytes: 128, count: 2, wantRequests: 1},
		{name: "one byte over boundary", sourceBytes: 128, count: 2, belowBoundary: true, wantRequests: 3},
		{name: "single sources exceed window", sourceBytes: 2048, count: 2, belowBoundary: true, wantRequests: 3},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			ids := make([]string, test.count)
			for i := range ids {
				ids[i] = fmt.Sprintf("id-%d", i)
			}
			source := `{"payload":"` + strings.Repeat("x", test.sourceBytes) + `"}`
			settings := backend.DefaultOptions()
			settings.ExchangeBytes = len(coalescingReadResponse(ids, source))
			if test.belowBoundary {
				settings.ExchangeBytes--
				if test.sourceBytes > 1024 {
					settings.ExchangeBytes = 1024
				}
			}
			var requests atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				var input struct {
					IDs []string `json:"ids"`
				}
				if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
					t.Error(err)
					return
				}
				_, _ = io.WriteString(w, coalescingReadResponse(input.IDs, source))
			})
			adapter := testHTTPAdapter(t, handler)
			adapter.config.Options = &settings
			works := make([]*execution.Plan, len(ids))
			for i, id := range ids {
				works[i] = batchTestPlan(t, adapter, "read", "records/s:"+id)
			}
			for _, result := range adapter.mget(t.Context(), works) {
				if result.failure != nil || string(result.reply.Source) != source {
					t.Fatal("byte window rejected or shortened a legal source", result.failure)
				}
			}
			if requests.Load() != test.wantRequests {
				t.Fatal("unexpected actual response splitting", requests.Load(), test.wantRequests)
			}
		})
	}
}

func TestOversizeReadRetryKeepsLiveDuplicateAfterCallerCancellation(t *testing.T) {
	caller, cancel := context.WithCancel(t.Context())
	defer cancel()
	var requests atomic.Int32
	source := `{"payload":"` + strings.Repeat("x", 2048) + `"}`
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			cancel()
		}
		var input struct {
			IDs []string `json:"ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
			return
		}
		_, _ = io.WriteString(w, coalescingReadResponse(input.IDs, source))
	})
	adapter := testHTTPAdapter(t, handler)
	settings := backend.DefaultOptions()
	settings.ExchangeBytes = 1024
	adapter.config.Options = &settings
	works := []*execution.Plan{
		batchTestPlan(t, adapter, "read", "records/s:same"),
		batchTestPlan(t, adapter, "read", "records/s:same"),
		batchTestPlan(t, adapter, "read", "records/s:other"),
	}
	works[0].Context = caller
	results := adapter.mget(t.Context(), works)
	if results[0].failure.GetCode() != pb.FailureCode_CANCELLED {
		t.Fatal("cancelled duplicate lost caller result", results[0])
	}
	for _, result := range results[1:] {
		if result.failure != nil || string(result.reply.Source) != source {
			t.Fatal("cancelled representative suppressed a live peer", result.failure)
		}
	}
	if requests.Load() != 3 {
		t.Fatal("overflow was not retried as two independent reads", requests.Load())
	}
}

func BenchmarkMgetCoalescing(b *testing.B) {
	scenarios := []struct {
		name           string
		count, indexes int
		repeated       bool
	}{
		{name: "single", count: 1, indexes: 1},
		{name: "distinct_32", count: 32, indexes: 1},
		{name: "hot_read_32", count: 32, indexes: 1, repeated: true},
		{name: "indexes_8_distinct_32", count: 32, indexes: 8},
		{name: "indexes_8_hot_read_128", count: 128, indexes: 8, repeated: true},
	}
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			var requests atomic.Int64
			source := `{"payload":"` + strings.Repeat("x", 1024) + `"}`
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				var input struct {
					IDs  []string `json:"ids"`
					Docs []struct {
						Index string `json:"_index"`
						ID    string `json:"_id"`
					} `json:"docs"`
				}
				if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
					b.Error(err)
					return
				}
				_, _ = io.WriteString(w, `{"docs":[`)
				count := max(len(input.IDs), len(input.Docs))
				for i := range count {
					if i != 0 {
						_, _ = io.WriteString(w, ",")
					}
					index, id := "", ""
					if len(input.Docs) != 0 {
						index, id = input.Docs[i].Index, input.Docs[i].ID
					} else {
						index, id = strings.Split(r.URL.Path, "/")[1], input.IDs[i]
					}
					_, _ = fmt.Fprintf(w, `{"_index":%q,"_id":%q,"found":true,"_seq_no":1,"_primary_term":1,"_source":%s}`, index, id, source)
				}
				_, _ = io.WriteString(w, "]}")
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			config := Config{Store: "search", URL: server.URL}
			adapter := &Adapter{config: config, client: server.Client(), ctx: context.Background()}
			works := make([]*execution.Plan, scenario.count)
			groups := make([][]*execution.Plan, scenario.indexes)
			for i := range works {
				index := i % scenario.indexes
				id := fmt.Sprint(i)
				if scenario.repeated {
					id = "same"
				}
				native := &plan{index: fmt.Sprintf("index%d", index), id: id, action: "read"}
				works[i] = &execution.Plan{Backend: native}
				groups[index] = append(groups[index], works[i])
			}
			for _, baseline := range []bool{true, false} {
				name := "coalesced"
				if baseline {
					name = "per_index_without_dedup"
				}
				b.Run(name, func(b *testing.B) {
					requests.Store(0)
					b.ReportAllocs()
					for b.Loop() {
						if baseline {
							for _, group := range groups {
								// Preserve the previous 4 MiB/source window as the
								// comparison, including its per-index exchanges.
								for start := 0; start < len(group); start += 7 {
									end := min(start+7, len(group))
									for _, result := range adapter.mgetUnique(b.Context(), group[start:end]) {
										if result.failure != nil {
											b.Fatal(result.failure)
										}
									}
								}
							}
						} else {
							for _, result := range adapter.mget(b.Context(), works) {
								if result.failure != nil {
									b.Fatal(result.failure)
								}
							}
						}
					}
					b.ReportMetric(float64(requests.Load())/float64(b.N), "exchanges/op")
				})
			}
		})
	}
}
