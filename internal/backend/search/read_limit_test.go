package search

import (
	"context"
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

func TestSearchReadUsesOnlyProtocolDocumentBound(t *testing.T) {
	sizes := []int{1024, (16 << 10) + 1, 70 << 10, protocol.MaxDocument, protocol.MaxDocument + 1}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
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
			config := Config{Store: "search", URL: server.URL}
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
				t.Fatal("read boundary affected mutation admission", failure)
			}
			works := []*execution.Plan{read, write}
			results := adapter.executeRecords(context.Background(), works)
			result := results[0].GetReadResult()
			if size <= protocol.MaxDocument && len(result.GetDocument().Data) != size || size > protocol.MaxDocument && result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
				t.Fatal("read size boundary was not enforced", size, result)
			}
			if writes.Load() != 1 || results[1].GetMutationResult().Outcome != pb.MutationOutcome_APPLIED || results[1].GetMutationResult().Failure != nil {
				t.Fatal("acknowledged write was affected by a read limit", results[1], writes.Load())
			}
		})
	}
}
