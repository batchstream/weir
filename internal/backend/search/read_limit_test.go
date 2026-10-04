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

func TestSearchReadSizeConfigurationAndBudgets(t *testing.T) {
	for _, limit := range []int{0, 1023, 1024, 16 << 10, protocol.MaxDocument, protocol.MaxDocument + 1} {
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
		callOperation := &pb.Command_Read{Read: request}
		callCommand := &pb.Command{Operation: callOperation}
		call := &pb.ExecuteRequest{Index: 1, Command: callCommand}
		work, failure := prepareTestRecord(adapter, call)
		if failure != nil {
			t.Fatal(failure)
		}
		if limit == 0 {
			limit = 16 << 10
		}
		if work.ResultBytes != limit+execution.ResultOverheadBytes || work.WorkingBytes != 3*batchBodyLimit || work.WorkingBytes < 3*metadataLimit {
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
		work := batchTestPlan(t, adapter, "read", fmt.Sprintf("records/s:%d", i))
		plans = append(plans, work)
	}
	results, signal := adapter.executeRecords(context.Background(), plans)
	for _, result := range results {
		if result.GetReadResult().GetDocument() == nil {
			t.Fatal("small record failed", result)
		}
	}
	if requests.Load() != 1 || idsPerRequest.Load() != 128 || signal != execution.Healthy {
		t.Fatal("small read profile did not collect the complete batch", requests.Load(), idsPerRequest.Load(), signal)
	}
}

func TestSearchReadSizeLimitDoesNotConstrainOrMisreportWrites(t *testing.T) {
	cases := []struct {
		limit int
		size  int
	}{
		{1024, 1024}, {1024, 1025}, {1024, 70 << 10},
		{0, 16 << 10}, {0, (16 << 10) + 1},
		{protocol.MaxDocument, (16 << 10) + 1},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("limit=%d/size=%d", tc.limit, tc.size), func(t *testing.T) {
			size, limit := tc.size, tc.limit
			if limit == 0 {
				limit = 16 << 10
			}
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
			config := Config{Store: "search", URL: server.URL, MaxReadSize: tc.limit}
			adapter := &Adapter{config: config, dialect: ElasticsearchProduct, client: server.Client(), ctx: context.Background()}
			read := batchTestPlan(t, adapter, "read", "records/s:read")
			writeSource := []byte(`{"pad":"` + strings.Repeat("x", 2048) + `"}`)
			document := &pb.Document{ContentType: "application/json", Data: writeSource}
			mutation := &pb.MutateRequest{Resource: "records/s:write", Action: &pb.MutateRequest_Put{Put: document}}
			operationOperation := &pb.Command_Mutate{Mutate: mutation}
			operationCommand := &pb.Command{Operation: operationOperation}
			operation := &pb.ExecuteRequest{Index: 1, Command: operationCommand}
			write, failure := prepareTestRecord(adapter, operation)
			if failure != nil {
				t.Fatal("read profile affected mutation admission", failure)
			}
			works := []*execution.Plan{read, write}
			results, _ := adapter.executeRecords(context.Background(), works)
			result := results[0].GetReadResult()
			if size <= limit && len(result.GetDocument().Data) != size || size > limit && result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
				t.Fatal("read size boundary was not enforced", size, result)
			}
			if writes.Load() != 1 || results[1].GetMutationResult().Outcome != pb.MutationOutcome_APPLIED || results[1].GetMutationResult().Failure != nil {
				t.Fatal("acknowledged write was affected by a read limit", results[1], writes.Load())
			}
		})
	}
}
