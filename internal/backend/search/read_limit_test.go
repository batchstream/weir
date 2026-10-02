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

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
)

func TestSearchReadSizeConfigurationAndBudgets(t *testing.T) {
	for _, limit := range []int{0, 1023, 1024, protocol.MaxDocument, protocol.MaxDocument + 1} {
		config := Config{Store: "search", URL: "http://127.0.0.1:9200", Pool: 4, MaxReadSize: limit}
		valid := limit == 0 || limit >= 1024 && limit <= protocol.MaxDocument
		if (ValidateConfig(config) == nil) != valid {
			t.Fatal("invalid maximum read size accepted", limit)
		}
		if !valid {
			continue
		}
		adapter := &Adapter{config: config}
		request := &pb.ReadRequest{Resource: "records/s:id"}
		variant := &pb.Call_Read{Read: request}
		call := &pb.Call{Version: 1, Operation: variant}
		work, failure := adapter.PrepareCall(1, call)
		if failure != nil {
			t.Fatal(failure)
		}
		if limit == 0 {
			limit = protocol.MaxDocument
		}
		if work.ResultBytes != limit+protocol.ResultOverhead || work.WorkingBytes > 24<<20 || work.WorkingBytes < 3*metadataLimit {
			t.Fatal("read declaration was not reflected in resource bounds", limit, work.ResultBytes, work.WorkingBytes)
		}
	}
}

func TestSearchSmallReadProfileBatches128Records(t *testing.T) {
	var requests atomic.Int32
	var idsPerRequest atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/records":
			_, _ = io.WriteString(w, testIndexReply)
		case "/records/_mget":
			requests.Add(1)
			var body struct{ IDs []string }
			if json.NewDecoder(r.Body).Decode(&body) != nil {
				t.Error("invalid mget body")
				return
			}
			idsPerRequest.Store(int32(len(body.IDs)))
			_, _ = io.WriteString(w, `{"docs":[`)
			for i, id := range body.IDs {
				if i != 0 {
					_, _ = io.WriteString(w, ",")
				}
				_, _ = fmt.Fprintf(w, `{"_index":"records","_id":%q,"found":true,"_seq_no":1,"_primary_term":1,"_source":{"id":%q}}`, id, id)
			}
			_, _ = io.WriteString(w, "]}")
		default:
			t.Error("unexpected endpoint", r.URL.Path)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	config := Config{Store: "search", URL: server.URL, MaxReadSize: 1024}
	adapter := &Adapter{config: config, dialect: ElasticsearchProduct, client: server.Client(), ctx: context.Background()}
	plans := make([]*execution.Plan, 0, 128)
	for i := range 128 {
		work := batchTestPlan(t, adapter, "read", fmt.Sprintf("weir://search/records/s:%d", i))
		plans = append(plans, work)
	}
	results, signal := adapter.executeRecords(context.Background(), plans)
	for _, result := range results {
		if result.GetRead().GetDocument() == nil {
			t.Fatal("small record failed", result)
		}
	}
	if requests.Load() != 1 || idsPerRequest.Load() != 128 || signal != execution.Healthy {
		t.Fatal("small read profile did not collect the complete batch", requests.Load(), idsPerRequest.Load(), signal)
	}
}

func TestSearchReadSizeLimitDoesNotConstrainOrMisreportWrites(t *testing.T) {
	for _, size := range []int{1024, 1025, 70 << 10} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			source := `{"pad":"` + strings.Repeat("x", size-len(`{"pad":""}`)) + `"}`
			var writes atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/records":
					_, _ = io.WriteString(w, testIndexReply)
				case "/records/_mget":
					_, _ = fmt.Fprintf(w, `{"docs":[{"_index":"records","_id":"read","found":true,"_seq_no":1,"_primary_term":1,"_source":%s}]}`, source)
				case "/_bulk":
					writes.Add(1)
					_, _ = io.WriteString(w, `{"errors":false,"took":1,"items":[{"index":{"_index":"records","_id":"write","status":201,"result":"created","_version":1,"_seq_no":1,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}]}`)
				default:
					t.Error("unexpected endpoint", r.URL.Path)
				}
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			config := Config{Store: "search", URL: server.URL, MaxReadSize: 1024}
			adapter := &Adapter{config: config, dialect: ElasticsearchProduct, client: server.Client(), ctx: context.Background()}
			read := batchTestPlan(t, adapter, "read", "weir://search/records/s:read")
			writeSource := []byte(`{"pad":"` + strings.Repeat("x", 2048) + `"}`)
			document := &pb.Document{MediaType: "application/json", Data: writeSource}
			mutation := &pb.MutateRequest{Resource: "weir://search/records/s:write", Action: &pb.MutateRequest_Put{Put: document}}
			operation := &pb.Operation{Operation: &pb.Operation_Mutate{Mutate: mutation}}
			write, failure := adapter.prepareRecord(operation)
			if failure != nil {
				t.Fatal("read profile affected mutation admission", failure)
			}
			works := []*execution.Plan{read, write}
			results, _ := adapter.executeRecords(context.Background(), works)
			result := results[0].GetRead()
			if size == 1024 && len(result.GetDocument().Data) != size || size > 1024 && result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
				t.Fatal("read size boundary was not enforced", size, result)
			}
			if writes.Load() != 1 || results[1].GetMutation().Outcome != pb.MutationOutcome_APPLIED || results[1].GetMutation().Failure != nil {
				t.Fatal("acknowledged write was affected by a read limit", results[1], writes.Load())
			}
		})
	}
}
