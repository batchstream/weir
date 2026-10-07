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

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

const testIndexReply = `{"records":{"settings":{"index.uuid":"test","index.number_of_shards":"1"},"mappings":{"_source":{"enabled":true}}}}`

func batchTestPlan(t *testing.T, a *Adapter, action, resource string) *execution.Plan {
	t.Helper()
	opCommand := &pb.Command{}
	op := &pb.ExecuteRequest{Index: 1, Command: opCommand}
	if action == "read" {
		read := &pb.ReadRequest{Resource: resource}
		recordOperation1 := &pb.Command_Read{Read: read}
		op.Command.Operation = recordOperation1
	} else {
		document := &pb.Document{ContentType: "application/json", Data: []byte(`{"n":2}`)}
		mutation := &pb.MutateRequest{Resource: resource}
		switch action {
		case "put":
			mutation.Action = &pb.MutateRequest_Put{Put: document}
		case "create":
			mutation.Action = &pb.MutateRequest_Create{Create: document}
		case "replace":
			mutation.Action = &pb.MutateRequest_Replace{Replace: document}
		case "delete":
			empty := &pb.Empty{}
			mutation.Action = &pb.MutateRequest_Delete{Delete: empty}
		case "expression":
			op = expressionOperation(resource, `{"doc":{"n":2}}`)
		case "program":
			program := &pb.LuaTransform{Source: []byte(`return function(current, incoming) return {n = 3} end`)}
			form := &pb.Transform_Lua{Lua: program}
			transform := &pb.Transform{Form: form}
			mutation.Action = &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
		default:
			t.Fatal(action)
		}
		if action != "expression" {
			recordOperation2 := &pb.Command_Mutate{Mutate: mutation}
			op.Command.Operation = recordOperation2
		}
	}
	work, failure := prepareTestRecord(a, op)
	if failure != nil {
		t.Fatal(failure)
	}
	return work
}

func TestMixedRecordBatchMergesReadsAndEveryMutation(t *testing.T) {
	var qualifications, reads, writes atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/records":
			qualifications.Add(1)
			_, _ = io.WriteString(w, testIndexReply)
		case "/records/_mget":
			reads.Add(1)
			var request struct {
				IDs []string `json:"ids"`
			}
			if r.Method != "POST" || r.URL.Query().Get("realtime") != "true" || json.NewDecoder(r.Body).Decode(&request) != nil || strings.Join(request.IDs, ",") != "read,missing,replace,program" {
				t.Errorf("unexpected merged read: method=%s ids=%v", r.Method, request.IDs)
			}
			if strings.Join(request.IDs, ",") == "program" {
				_, _ = io.WriteString(w, `{"docs":[{"_index":"records","_id":"program","found":true,"_seq_no":3,"_primary_term":1,"_source":{"n":1}}]}`)
			} else {
				_, _ = io.WriteString(w, `{"docs":[{"_index":"records","_id":"read","found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":9007199254740993}},{"_index":"records","_id":"missing","found":false},{"_index":"records","_id":"replace","found":true,"_seq_no":2,"_primary_term":1,"_source":{"n":1}},{"_index":"records","_id":"program","found":true,"_seq_no":3,"_primary_term":1,"_source":{"n":1}}]}`)
			}
		case "/_bulk":
			writes.Add(1)
			raw, _ := io.ReadAll(r.Body)
			lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
			if len(lines) != 11 || !strings.Contains(lines[0], `"if_seq_no":2`) || !strings.Contains(lines[2], `"retry_on_conflict":0`) || strings.Contains(lines[2], `"pipeline"`) || !strings.Contains(lines[4], `"if_seq_no":3`) || lines[5] != `{"n":3}` {
				t.Errorf("unexpected mixed NDJSON: %s", raw)
			}
			_, _ = io.WriteString(w, `{"errors":false,"took":1,"items":[{"index":{"_index":"records","_id":"replace","status":200,"result":"updated","_seq_no":4,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}},{"update":{"_index":"records","_id":"expression","status":200,"result":"noop","_version":1,"_seq_no":5,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}},{"index":{"_index":"records","_id":"program","status":200,"result":"updated","_version":1,"_seq_no":6,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}},{"index":{"_index":"records","_id":"put","status":201,"result":"created","_seq_no":7,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}},{"create":{"_index":"records","_id":"create","status":201,"result":"created","_seq_no":8,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}},{"delete":{"_index":"records","_id":"delete","status":200,"result":"deleted","_seq_no":9,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}]}`)
		default:
			t.Errorf("unbatched request: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	cfg := Config{Store: "search", URL: server.URL, MaxReadSize: protocol.MaxDocument}
	a := &Adapter{dialect: ElasticsearchProduct, config: cfg, client: server.Client(), ctx: context.Background()}
	actions := []string{"read", "read", "replace", "expression", "program", "put", "create", "delete"}
	ids := []string{"read", "missing", "replace", "expression", "program", "put", "create", "delete"}
	works := make([]*execution.Plan, len(actions))
	for i, action := range actions {
		works[i] = batchTestPlan(t, a, action, "records/s:"+ids[i])
		works[i].ID = uint64(100 + i)
		if action == "replace" || action == "program" {
			if works[i].ResultBytes != execution.ResultOverheadBytes || works[i].WorkingBytes < 3*execution.BackendBatchBytes {
				t.Fatal("mutation result credits or batched pre-read working memory incorrect", works[i].ResultBytes, works[i].WorkingBytes)
			}
		}
	}
	results := a.executeRecords(context.Background(), works)
	if len(results) != len(works) || qualifications.Load() != 1 || reads.Load() != 1 || writes.Load() != 1 {
		t.Fatalf("batch counts/results: qualification=%d reads=%d writes=%d results=%v", qualifications.Load(), reads.Load(), writes.Load(), results)
	}
	if string(results[0].GetReadResult().GetDocument().GetData()) != `{"n":9007199254740993}` || results[1].GetReadResult().GetMissing() == nil {
		t.Fatal("read source/missing evidence", results[:2])
	}
	for i, result := range results {
		if i >= 2 && (result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || result.GetMutationResult().GetFailure() != nil) {
			t.Fatal("mutation not applied", result)
		}
	}
	if works[4].Backend.(*plan).action != "program" || works[4].Backend.(*plan).source != nil {
		t.Fatal("original Lua plan mutated")
	}
}

func TestMixedWriteShardEvidenceNeverReplaysUnknown(t *testing.T) {
	cases := []struct {
		name, shards  string
		applied       bool
		replicaFailed bool
	}{
		{name: "null", shards: `null`},
		{name: "missing total", shards: `{"successful":1,"failed":0}`},
		{name: "missing successful", shards: `{"total":1,"failed":0}`},
		{name: "missing failed", shards: `{"total":1,"successful":1}`},
		{name: "negative total", shards: `{"total":-1,"successful":1,"failed":0}`},
		{name: "negative successful", shards: `{"total":1,"successful":-1,"failed":0}`},
		{name: "no primary acknowledgement", shards: `{"total":1,"successful":0,"failed":0}`},
		{name: "negative failed", shards: `{"total":1,"successful":1,"failed":-1}`},
		{name: "successful exceeds total", shards: `{"total":1,"successful":2,"failed":0}`},
		{name: "failed exceeds remainder", shards: `{"total":2,"successful":1,"failed":2}`},
		{name: "excessive counts", shards: `{"total":1,"successful":9223372036854775807,"failed":9223372036854775807}`},
		{name: "healthy", shards: `{"total":1,"successful":1,"failed":0}`, applied: true},
		{name: "replica failure", shards: `{"total":2,"successful":1,"failed":1}`, applied: true, replicaFailed: true},
	}
	for _, tc := range cases {
		for _, expressionResult := range []string{"updated", "noop"} {
			t.Run(tc.name+"/"+expressionResult, func(t *testing.T) {
				var reads, writes atomic.Int32
				reply := fmt.Sprintf(`{"errors":false,"took":1,"items":[
{"index":{"_index":"records","_id":"put","status":200,"result":"updated","_version":1,"_seq_no":2,"_primary_term":1,"_shards":%s}},
{"update":{"_index":"records","_id":"expression","status":200,"result":%q,"_version":1,"_seq_no":2,"_primary_term":1,"_shards":%s}},
{"index":{"_index":"records","_id":"program","status":200,"result":"updated","_version":1,"_seq_no":2,"_primary_term":1,"_shards":%s}}]}`,
					tc.shards, expressionResult, tc.shards, tc.shards)
				handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/records":
						_, _ = io.WriteString(w, testIndexReply)
					case "/records/_mget":
						reads.Add(1)
						_, _ = io.WriteString(w, `{"docs":[{"_index":"records","_id":"program","found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1}}]}`)
					case "/_bulk":
						writes.Add(1)
						_, _ = io.WriteString(w, reply)
					default:
						t.Errorf("unexpected request: %s", r.URL.Path)
						http.NotFound(w, r)
					}
				})
				server := httptest.NewServer(handler)
				defer server.Close()
				cfg := Config{Store: "search", URL: server.URL}
				a := &Adapter{dialect: ElasticsearchProduct, config: cfg, client: server.Client(), ctx: context.Background()}
				actions := []string{"put", "expression", "program"}
				works := make([]*execution.Plan, len(actions))
				for i, action := range actions {
					works[i] = batchTestPlan(t, a, action, "records/s:"+action)
				}
				results := a.executeRecords(t.Context(), works)
				if len(results) != len(works) {
					t.Fatal("missing acknowledgement results", len(results))
				}
				for i, event := range results {
					want := pb.MutationOutcome_UNKNOWN
					if tc.applied && (i != 1 || expressionResult != "noop" || !tc.replicaFailed) {
						want = pb.MutationOutcome_APPLIED
					}
					mutation := event.GetMutationResult()
					if mutation.GetOutcome() != want {
						t.Fatalf("%s acknowledgement outcome: got %v, want %v", actions[i], mutation, want)
					}
					if want == pb.MutationOutcome_APPLIED && !tc.replicaFailed {
						if mutation.GetFailure() != nil {
							t.Fatal("healthy acknowledgement retained a failure", actions[i], mutation)
						}
					} else if mutation.GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE {
						t.Fatal("incomplete or replica-failed acknowledgement lost failure evidence", actions[i], mutation)
					}
				}
				if reads.Load() != 1 || writes.Load() != 1 {
					t.Fatal("write acknowledgement caused replay or Lua reevaluation", reads.Load(), writes.Load())
				}
			})
		}
	}
}

func TestMgetRequiresCompleteIDCorrespondenceAndIsolatesItemErrors(t *testing.T) {
	for _, mode := range []string{"reordered", "missing", "item_error"} {
		t.Run(mode, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/records" {
					_, _ = io.WriteString(w, testIndexReply)
					return
				}
				first := `{"_index":"records","_id":"first","found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1}}`
				second := `{"_index":"records","_id":"second","found":false}`
				switch mode {
				case "reordered":
					first, second = second, first
				case "item_error":
					second = `{"_index":"records","_id":"second","error":{"type":"index_not_found_exception"}}`
				}
				if mode == "missing" {
					_, _ = fmt.Fprintf(w, `{"docs":[%s]}`, first)
				} else {
					_, _ = fmt.Fprintf(w, `{"docs":[%s,%s]}`, first, second)
				}
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			cfg := Config{Store: "search", URL: server.URL}
			a := &Adapter{dialect: ElasticsearchProduct, config: cfg, client: server.Client(), ctx: context.Background()}
			works := []*execution.Plan{batchTestPlan(t, a, "read", "records/s:first"), batchTestPlan(t, a, "read", "records/s:second")}
			results := a.executeRecords(context.Background(), works)
			if len(results) != 2 || results[1].GetReadResult().GetFailure() == nil {
				t.Fatal("incomplete/error evidence", results)
			}
			if (results[0].GetReadResult().GetDocument() != nil) != (mode == "item_error") {
				t.Fatal("positional evidence or error isolation", results)
			}
		})
	}
}

func TestMixedLuaCreateConflictRetriesOnlyConfirmedItem(t *testing.T) {
	for _, mode := range []string{"conflict", "unknown", "exhausted"} {
		t.Run(mode, func(t *testing.T) {
			var reads, writes atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/records":
					_, _ = io.WriteString(w, testIndexReply)
				case "/records/_mget":
					attempt := reads.Add(1)
					if attempt == 1 {
						_, _ = io.WriteString(w, `{"docs":[{"_index":"records","_id":"program","found":false}]}`)
					} else {
						_, _ = io.WriteString(w, `{"docs":[{"_index":"records","_id":"program","found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":10}}]}`)
					}
				case "/_bulk":
					attempt := writes.Add(1)
					raw, _ := io.ReadAll(r.Body)
					if attempt == 1 {
						if !strings.Contains(string(raw), `"create"`) || !strings.Contains(string(raw), `"_id":"put"`) {
							t.Error("Lua create did not share peer bulk", string(raw))
						}
						if mode == "unknown" {
							_, _ = io.WriteString(w, `{}`)
							return
						}
						_, _ = io.WriteString(w, `{"errors":true,"took":1,"items":[{"create":{"_index":"records","_id":"program","status":409,"error":{"type":"version_conflict_engine_exception"}}},{"index":{"_index":"records","_id":"put","status":201,"result":"created","_seq_no":2,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}]}`)
					} else {
						if strings.Contains(string(raw), `"_id":"put"`) || !strings.Contains(string(raw), `"if_seq_no":1`) {
							t.Error("peer replayed or retry missing new OCC condition", string(raw))
						}
						if mode == "exhausted" {
							_, _ = io.WriteString(w, `{"errors":true,"took":1,"items":[{"index":{"_index":"records","_id":"program","status":409,"error":{"type":"version_conflict_engine_exception"}}}]}`)
						} else {
							_, _ = io.WriteString(w, `{"errors":false,"took":1,"items":[{"index":{"_index":"records","_id":"program","status":200,"result":"updated","_version":2,"_seq_no":3,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}]}`)
						}
					}
				default:
					t.Error("unexpected unbatched path", r.URL.Path)
				}
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			cfg := Config{Store: "search", URL: server.URL}
			a := &Adapter{dialect: ElasticsearchProduct, config: cfg, client: server.Client(), ctx: context.Background()}
			works := []*execution.Plan{batchTestPlan(t, a, "program", "records/s:program"), batchTestPlan(t, a, "put", "records/s:put")}
			results := a.executeRecords(context.Background(), works)
			wantCalls, wantOutcome := int32(2), pb.MutationOutcome_APPLIED
			if mode == "unknown" {
				wantCalls, wantOutcome = 1, pb.MutationOutcome_UNKNOWN
			}
			if mode == "exhausted" {
				wantCalls = programAttempts
			}
			if reads.Load() != wantCalls || writes.Load() != wantCalls {
				t.Fatal("unbounded/ambiguous retry", reads.Load(), writes.Load())
			}
			for i, result := range results {
				expected := wantOutcome
				if mode == "exhausted" && i == 0 {
					expected = pb.MutationOutcome_NOT_APPLIED
					if result.GetMutationResult().GetFailure().GetCode() != pb.FailureCode_CONFLICT {
						t.Fatal("retry limit did not report confirmed conflicts", result)
					}
				}
				if result.GetMutationResult().GetOutcome() != expected {
					t.Fatal(result)
				}
			}
		})
	}
}

func TestCanceledCallerSkippedAfterBatchReadWithoutCancelingPeer(t *testing.T) {
	caller, cancel := context.WithCancel(context.Background())
	defer cancel()
	var writes atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/records":
			_, _ = io.WriteString(w, testIndexReply)
		case "/records/_mget":
			cancel()
			_, _ = io.WriteString(w, `{"docs":[{"_index":"records","_id":"replace","found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1}}]}`)
		case "/_bulk":
			writes.Add(1)
			raw, _ := io.ReadAll(r.Body)
			if strings.Contains(string(raw), `"_id":"replace"`) {
				t.Error("canceled caller dispatched after pre-read")
			}
			_, _ = io.WriteString(w, `{"errors":false,"took":1,"items":[{"index":{"_index":"records","_id":"put","status":201,"result":"created","_seq_no":2,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}]}`)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	cfg := Config{Store: "search", URL: server.URL}
	a := &Adapter{dialect: ElasticsearchProduct, config: cfg, client: server.Client(), ctx: context.Background()}
	replace := batchTestPlan(t, a, "replace", "records/s:replace")
	replace.Context = caller
	put := batchTestPlan(t, a, "put", "records/s:put")
	results := a.executeRecords(context.Background(), []*execution.Plan{replace, put})
	if writes.Load() != 1 || results[0].GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_APPLIED || results[0].GetMutationResult().GetFailure().GetCode() != pb.FailureCode_CANCELLED || results[1].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("caller isolation", results, writes.Load())
	}
}
