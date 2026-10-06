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
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/value"
)

func TestLuaRecordBatchIsolatesActionsAndPreservesBusinessSources(t *testing.T) {
	var reads, writes atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/records":
			_, _ = io.WriteString(w, testIndexReply)
		case "/records/_mget":
			reads.Add(1)
			var request struct{ IDs []string }
			if json.NewDecoder(r.Body).Decode(&request) != nil || strings.Join(request.IDs, ",") != "replace,keep,reject,invalid,delete,create" {
				t.Error("Lua actions did not share their read", request.IDs)
			}
			_, _ = io.WriteString(w, `{"docs":[`)
			for i, id := range request.IDs {
				if i != 0 {
					_, _ = io.WriteString(w, ",")
				}
				if id == "create" {
					_, _ = fmt.Fprintf(w, `{"_index":"records","_id":%q,"found":false}`, id)
				} else {
					_, _ = fmt.Fprintf(w, `{"_index":"records","_id":%q,"found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1,"keep":"source"}}`, id)
				}
			}
			_, _ = io.WriteString(w, "]}")
		case "/_bulk":
			writes.Add(1)
			raw, _ := io.ReadAll(r.Body)
			lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
			if len(lines) != 5 || lines[1] != `{"n":2,"keep":"source"}` || lines[4] != `{"n":3}` || !strings.Contains(lines[0], `"if_seq_no":1`) || !strings.Contains(lines[2], `"if_primary_term":1`) || strings.Contains(lines[3], "if_seq_no") {
				t.Error("Lua write grouping or business source changed", string(raw))
			}
			_, _ = io.WriteString(w, `{"errors":false,"took":1,"items":[{"index":{"_index":"records","_id":"replace","status":200,"result":"updated","_version":2,"_seq_no":2,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}},{"delete":{"_index":"records","_id":"delete","status":200,"result":"deleted","_version":2,"_seq_no":2,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}},{"create":{"_index":"records","_id":"create","status":201,"result":"created","_version":1,"_seq_no":3,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}]}`)
		default:
			t.Error("unexpected unbatched path", r.URL.Path)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	config := Config{Store: "search", URL: server.URL}
	adapter := &Adapter{config: config, dialect: ElasticsearchProduct, client: server.Client(), ctx: t.Context()}
	ids := []string{"replace", "keep", "reject", "invalid", "delete", "create"}
	sources := []string{
		`return weir.replace(weir.set(current, "n", weir.add(weir.to64(weir.get(current, "n")), weir.i64("1"))))`,
		`return weir.keep()`,
		`return weir.reject("business rule")`,
		`return "invalid result"`,
		`return weir.delete()`,
		`return weir.replace(weir.object("n", weir.i64("3")))`,
	}
	works := make([]*execution.Plan, 0, len(ids))
	for i, id := range ids {
		work := batchTestPlan(t, adapter, "program", "records/s:"+id)
		work.Backend.(*plan).program.Source = sources[i]
		// Independent RPCs all begin at ordinal 1; ownership is by plan pointer.
		work.ID = 1
		if work.Command.GetScan() != nil || work.Command.GetNative() != nil {
			t.Fatal("Lua plan excluded from record batching")
		}
		works = append(works, work)
	}
	results := make(map[*execution.Plan]*pb.Event)
	emit := func(work *execution.Plan, event *pb.Event) error {
		results[work] = event
		return nil
	}
	adapter.Execute(t.Context(), works, emit)
	if reads.Load() != 1 || writes.Load() != 1 || len(results) != len(works) {
		t.Fatal("all-Lua native batch or ownership", reads.Load(), writes.Load(), len(results))
	}
	for i, work := range works {
		result := results[work]
		mutation := result.GetMutationResult()
		if work.Backend.(*plan).action != "program" || work.Backend.(*plan).source != nil {
			t.Fatal("original plan or RPC ordinal changed", result)
		}
		switch i {
		case 2:
			if mutation.Outcome != pb.MutationOutcome_NOT_APPLIED || mutation.GetFailure().GetCode() != pb.FailureCode_PRECONDITION_FAILED || mutation.GetFailure().GetMessage() != "business rule" {
				t.Fatal("business rejection lost", result)
			}
		case 3:
			if mutation.Outcome != pb.MutationOutcome_NOT_APPLIED || mutation.GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT {
				t.Fatal("invalid Lua did not remain isolated", result)
			}
		default:
			if mutation.Outcome != pb.MutationOutcome_APPLIED || mutation.Failure != nil {
				t.Fatal("peer did not apply", result)
			}
		}
	}
}

func TestLuaBatchCallerCancellationAndDeadlineKeepPeers(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			caller, cancel := context.WithCancel(t.Context())
			if mode == "deadline" {
				cancel()
				caller, cancel = context.WithTimeout(t.Context(), 250*time.Millisecond)
			}
			defer cancel()
			var reads, writes atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/records":
					_, _ = io.WriteString(w, testIndexReply)
				case "/records/_mget":
					reads.Add(1)
					if mode == "cancel" {
						cancel()
					} else {
						<-caller.Done()
					}
					_, _ = io.WriteString(w, `{"docs":[{"_index":"records","_id":"cancelled","found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1}},{"_index":"records","_id":"peer","found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1}}]}`)
				case "/_bulk":
					writes.Add(1)
					raw, _ := io.ReadAll(r.Body)
					if strings.Contains(string(raw), `"_id":"cancelled"`) || !strings.Contains(string(raw), `"_id":"peer"`) {
						t.Error("caller cancellation leaked into peer write", string(raw))
					}
					_, _ = io.WriteString(w, `{"errors":false,"took":1,"items":[{"index":{"_index":"records","_id":"peer","status":200,"result":"updated","_version":2,"_seq_no":2,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}]}`)
				}
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			config := Config{Store: "search", URL: server.URL}
			adapter := &Adapter{config: config, dialect: ElasticsearchProduct, client: server.Client(), ctx: t.Context()}
			cancelled := batchTestPlan(t, adapter, "program", "records/s:cancelled")
			cancelled.Context = caller
			peer := batchTestPlan(t, adapter, "program", "records/s:peer")
			works := []*execution.Plan{cancelled, peer}
			results := adapter.executeRecords(t.Context(), works)
			code := pb.FailureCode_CANCELLED
			if mode == "deadline" {
				code = pb.FailureCode_DEADLINE_EXCEEDED
			}
			if reads.Load() != 1 || writes.Load() != 1 || results[0].GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_APPLIED || results[0].GetMutationResult().GetFailure().GetCode() != code || results[1].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal("per-caller lifetime or peer isolation", results, reads.Load(), writes.Load())
			}
		})
	}
}

func TestLuaBatchChunksLargeLegalSourcesBeforeEvaluation(t *testing.T) {
	const records = 130
	source := `{"pad":"` + strings.Repeat("x", value.MaxBytes-len(`{"pad":""}`)) + `"}`
	var reads, writes, maxReadItems atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/records":
			_, _ = io.WriteString(w, testIndexReply)
		case "/records/_mget":
			reads.Add(1)
			var request struct{ IDs []string }
			if json.NewDecoder(r.Body).Decode(&request) != nil {
				t.Error("invalid mget body")
				return
			}
			for current := maxReadItems.Load(); int32(len(request.IDs)) > current; current = maxReadItems.Load() {
				if maxReadItems.CompareAndSwap(current, int32(len(request.IDs))) {
					break
				}
			}
			_, _ = io.WriteString(w, `{"docs":[`)
			for i, id := range request.IDs {
				if i != 0 {
					_, _ = io.WriteString(w, ",")
				}
				_, _ = fmt.Fprintf(w, `{"_index":"records","_id":%q,"found":true,"_seq_no":1,"_primary_term":1,"_source":%s}`, id, source)
			}
			_, _ = io.WriteString(w, "]}")
		case "/_bulk":
			writes.Add(1)
			raw, _ := io.ReadAll(r.Body)
			lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
			if len(lines) != 2*records {
				t.Error("small generated replacements failed to collect", len(lines))
			}
			_, _ = io.WriteString(w, `{"errors":false,"took":1,"items":[`)
			for i := range records {
				if i != 0 {
					_, _ = io.WriteString(w, ",")
				}
				_, _ = fmt.Fprintf(w, `{"index":{"_index":"records","_id":"%d","status":200,"result":"updated","_version":2,"_seq_no":2,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}`, i)
			}
			_, _ = io.WriteString(w, "]}")
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	config := Config{Store: "search", URL: server.URL}
	adapter := &Adapter{config: config, dialect: ElasticsearchProduct, client: server.Client(), ctx: t.Context()}
	works := make([]*execution.Plan, 0, records)
	for i := range records {
		work := batchTestPlan(t, adapter, "program", fmt.Sprintf("records/s:%d", i))
		works = append(works, work)
	}
	results := adapter.executeRecords(t.Context(), works)
	if reads.Load() != 9 || maxReadItems.Load() != 15 || writes.Load() != 1 {
		t.Fatal("large document pre-read escaped scratch bounds", reads.Load(), maxReadItems.Load(), writes.Load())
	}
	for _, result := range results {
		if result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || result.GetMutationResult().GetFailure() != nil {
			t.Fatal("legal large source failed to transform", result)
		}
	}
}

func TestLuaBatchUsesAbsoluteBackendDeadlineBeyondFiveSeconds(t *testing.T) {
	var reads, writes atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/records":
			timer := time.NewTimer(5100 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-r.Context().Done():
				return
			}
			_, _ = io.WriteString(w, testIndexReply)
		case "/records/_mget":
			reads.Add(1)
			_, _ = io.WriteString(w, `{"docs":[{"_index":"records","_id":"program","found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1}}]}`)
		case "/_bulk":
			writes.Add(1)
			_, _ = io.WriteString(w, `{"errors":false,"took":1,"items":[{"index":{"_index":"records","_id":"program","status":200,"result":"updated","_version":2,"_seq_no":2,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}]}`)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	config := Config{Store: "search", URL: server.URL}
	adapter := &Adapter{config: config, dialect: ElasticsearchProduct, client: server.Client(), ctx: t.Context()}
	work := batchTestPlan(t, adapter, "program", "records/s:program")
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	works := []*execution.Plan{work}
	results := adapter.executeRecords(ctx, works)
	if reads.Load() != 1 || writes.Load() != 1 || results[0].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || results[0].GetMutationResult().GetFailure() != nil {
		t.Fatal("hidden Lua RMW lifetime overrode legal backend deadline", results, reads.Load(), writes.Load())
	}
}
