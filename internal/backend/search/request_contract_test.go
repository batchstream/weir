package search

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
)

func TestSearchLuaBulkRequiresPositivePrimaryEvidence(t *testing.T) {
	for _, action := range []string{"created", "updated", "deleted"} {
		for _, evidence := range []string{"zero", "replica_failure", "confirmed"} {
			t.Run(action+"/"+evidence, func(t *testing.T) {
				adapter := &Adapter{dialect: ElasticsearchProduct}
				operation := "index"
				if action == "created" {
					operation = "create"
				} else if action == "deleted" {
					operation = "delete"
				}
				status := 200
				if action == "created" {
					status = 201
				}
				successful, failed, total := 1, 0, 1
				if evidence == "zero" {
					successful = 0
				} else if evidence == "replica_failure" {
					total, failed = 2, 1
				}
				program := &luaengine.Program{}
				native := &plan{index: "records", id: "item", action: operation, program: program}
				empty := &pb.Empty{}
				mutationAction := &pb.MutateRequest_Delete{Delete: empty}
				mutation := &pb.MutateRequest{Resource: "records/s:item", Action: mutationAction}
				variant := &pb.Command_Mutate{Mutate: mutation}
				command := &pb.Command{Operation: variant}
				work := &execution.Plan{Command: command, Backend: native}
				raw := []byte(fmt.Sprintf(`{"errors":false,"took":1,"items":[{%q:{"_index":"records","_id":"item","status":%d,"_version":1,"_seq_no":0,"_primary_term":1,"result":%q,"_shards":{"total":%d,"successful":%d,"failed":%d}}}]}`, operation, status, action, total, successful, failed))
				replyInput48 := bulkResponse{works: []*execution.Plan{work}, status: 200, raw: raw, err: nil}
				results := adapter.bulkResults(context.Background(), replyInput48)
				result := results[0].GetMutationResult()
				if evidence == "zero" {
					if result.Outcome != pb.MutationOutcome_UNKNOWN || result.Failure == nil {
						t.Fatal("zero primary acknowledgement accepted", result)
					}
				} else if result.Outcome != pb.MutationOutcome_APPLIED || (result.Failure != nil) != (evidence == "replica_failure") {
					t.Fatal("confirmed application evidence lost", result)
				}
				replyInput58 := bulkResponse{works: []*execution.Plan{work}, status: 200, raw: raw, err: errTimeout}
				lost := adapter.bulkResults(context.Background(), replyInput58)
				if lost[0].GetMutationResult().Outcome != pb.MutationOutcome_UNKNOWN {
					t.Fatal("transport failure trusted body", lost[0])
				}
			})
		}
	}
}

func TestSearchConcreteIndexRemainsOneHTTPPathSegment(t *testing.T) {
	for _, index := range []string{"percent%2f", "logs.2026", "1数字", strings.Repeat("a", 255)} {
		for _, dialect := range []string{ElasticsearchProduct, OpenSearchProduct} {
			t.Run(dialect+"/"+index, func(t *testing.T) {
				paths := map[string]int{}
				escaped := "/" + url.PathEscape(index)
				handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if !strings.HasPrefix(r.URL.EscapedPath(), escaped) || strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")[0] != index {
						t.Errorf("index was decoded twice or split: %s", r.RequestURI)
						w.WriteHeader(500)
						return
					}
					suffix := strings.TrimPrefix(r.URL.EscapedPath(), escaped)
					paths[suffix]++
					switch suffix {
					case "":
						fmt.Fprintf(w, `{%q:{"settings":{"index.uuid":"test","index.number_of_shards":"1"},"mappings":{"_source":{"enabled":true}}}}`, index)
					case "/_mget":
						fmt.Fprintf(w, `{"docs":[{"_index":%q,"_id":"item","found":false}]}`, index)
					case "/_doc/item":
						fmt.Fprint(w, `{"found":false}`)
					default:
						t.Errorf("unexpected path: %s", r.RequestURI)
						w.WriteHeader(500)
					}
				})
				server := httptest.NewServer(handler)
				defer server.Close()
				config := Config{Store: "search", URL: server.URL}
				adapter := &Adapter{dialect: dialect, config: config, client: server.Client(), nativeClient: server.Client(), ctx: context.Background()}
				resource := url.PathEscape(index)
				read := &pb.ReadRequest{Resource: resource + "/s:item"}
				variant := &pb.Command_Read{Read: read}
				command := &pb.Command{Operation: variant}
				record, err := execution.NewRecord("search", 1, command)
				if err != nil {
					t.Fatal(err)
				}
				work, failure := adapter.PrepareRecord(context.Background(), record)
				if failure != nil {
					t.Fatal(failure)
				}
				observations := adapter.mgetUnique(context.Background(), []*execution.Plan{work})
				if observations[0].failure != nil || *observations[0].reply.Found {
					t.Fatal("decoded index/read mismatch", observations[0].failure)
				}
				native := nativeRequest(t, resource, "GET", "/_doc/item")
				end, _ := runNative(t, adapter, native, nil)
				if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || paths["/_mget"] != 1 || paths["/_doc/item"] != 1 {
					t.Fatal("read and Native did not share concrete index", end, paths)
				}
			})
		}
	}
}

func TestSearchWarmReadClassifiesNativeTargetAndPermissionFailures(t *testing.T) {
	for _, item := range []struct {
		status int
		kind   string
		code   pb.FailureCode
	}{
		{status: 401, kind: "security_exception", code: pb.FailureCode_UNAUTHENTICATED},
		{status: 403, kind: "security_exception", code: pb.FailureCode_PERMISSION_DENIED},
		{status: 404, kind: "index_not_found_exception", code: pb.FailureCode_TARGET_NOT_FOUND},
	} {
		t.Run(fmt.Sprint(item.status), func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(item.status)
				fmt.Fprintf(w, `{"error":{"type":%q},"status":%d}`, item.kind, item.status)
			})
			adapter := testHTTPAdapter(t, handler)

			read := &pb.ReadRequest{Resource: "records/s:item"}
			variant := &pb.Command_Read{Read: read}
			command := &pb.Command{Operation: variant}
			record, err := execution.NewRecord("search", 1, command)
			if err != nil {
				t.Fatal(err)
			}
			work, failure := adapter.PrepareRecord(context.Background(), record)
			if failure != nil {
				t.Fatal(failure)
			}
			observations := adapter.mgetUnique(context.Background(), []*execution.Plan{work})
			if observations[0].failure.GetCode() != item.code {
				t.Fatal("warm read classification", observations[0].failure)
			}
			_, failure = adapter.inspect(context.Background(), "records", false)
			if failure.GetCode() != item.code {
				t.Fatal("initial classification differed", failure)
			}
		})
	}
}
