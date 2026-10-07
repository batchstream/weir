package search

import (
	"context"
	"encoding/json"
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
				results := adapter.bulkResults([]*execution.Plan{work}, 200, raw, nil)
				result := results[0].GetMutationResult()
				if evidence == "zero" {
					if result.Outcome != pb.MutationOutcome_UNKNOWN || result.Failure == nil {
						t.Fatal("zero primary acknowledgement accepted", result)
					}
				} else if result.Outcome != pb.MutationOutcome_APPLIED || (result.Failure != nil) != (evidence == "replica_failure") {
					t.Fatal("confirmed application evidence lost", result)
				}
				lost := adapter.bulkResults([]*execution.Plan{work}, 200, raw, errTimeout)
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
					if r.URL.Path == "/_search" {
						var request scanSearchRequest
						if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.PIT.ID != "snapshot" {
							t.Error("Scan fetch lost its qualified index PIT", request.PIT.ID, err)
							w.WriteHeader(500)
							return
						}
						paths["/_search"]++
						_, _ = w.Write(scanBatchReply(nil))
						return
					}
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
					case "/_pit":
						fmt.Fprint(w, `{"id":"snapshot","_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`)
					case "/_search/point_in_time":
						fmt.Fprint(w, `{"pit_id":"snapshot","creation_time":1,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`)
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
				work, failure := adapter.PrepareRecord(record)
				if failure != nil {
					t.Fatal(failure)
				}
				observations := adapter.mgetIndex(context.Background(), index, []*execution.Plan{work})
				if observations[0].failure != nil || *observations[0].reply.Found {
					t.Fatal("decoded index/read mismatch", observations[0].failure)
				}
				request := &pb.ScanRequest{Resource: resource}
				scan, failure := adapter.prepareScan(request)
				if failure != nil {
					t.Fatal(failure)
				}
				page := adapter.fetchScan(context.Background(), scan)
				if page.Failure != nil {
					t.Fatal(page.Failure)
				}
				native := nativeRequest(t, resource, "GET", "/_doc/item")
				end, _ := runNative(t, adapter, native, nil)
				if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || paths["/_mget"] != 1 || paths["/_doc/item"] != 1 || paths[""] != 1 || paths["/_search"] != 1 {
					t.Fatal("typed operations did not share concrete index", end, paths)
				}
			})
		}
	}
}

func TestSearchScanProjectionPublishesSourceAndBindsCheckpoint(t *testing.T) {
	for _, mode := range []pb.ProjectionMode{pb.ProjectionMode_INCLUDE, pb.ProjectionMode_EXCLUDE} {
		t.Run(mode.String(), func(t *testing.T) {
			calls := 0
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var request struct {
					Query  json.RawMessage
					Source map[string][]string `json:"_source"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				key := "includes"
				if mode == pb.ProjectionMode_EXCLUDE {
					key = "excludes"
				}
				if len(request.Source) != 1 || len(request.Source[key]) != 1 || request.Source[key][0] != "private" || string(request.Query) != `{"match_all":{}}` {
					t.Error("native projection/filter envelope", request)
				}
				fields := scanTestReply()
				raw, _ := json.Marshal(fields)
				_, _ = w.Write(raw)
			})
			adapter := scanBatchAdapter(t, handler)
			filter := &pb.Document{ContentType: "application/json", Data: []byte(`{"match_all":{}}`)}
			projection := &pb.Projection{Mode: mode, Fields: []string{"private"}}
			request := &pb.ScanRequest{Resource: "records", Filter: filter, Projection: projection, PageSize: 1}
			work, failure := adapter.prepareScan(request)
			if failure != nil {
				t.Fatal(failure)
			}
			state := work.Backend.(*scanPlan)
			state.opened, state.pit = true, "snapshot"
			page := adapter.fetchScan(context.Background(), work)
			if page.Failure != nil || len(page.Documents) != 1 || string(page.Documents[0].Data) != `{"n":9223372036854775807}` || !page.Complete || calls != 1 {
				t.Fatal("hit metadata leaked or private cursor lost", page.Failure)
			}
			request.ContinuationToken = page.NextContinuationToken
			projection.Fields = []string{"changed"}
			if _, failure := adapter.prepareScan(request); failure.GetCode() != pb.FailureCode_INVALID_ARGUMENT {
				t.Fatal("projection changed without invalidating token", failure)
			}
		})
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
			adapter := scanBatchAdapter(t, handler)
			adapter.config.MaxReadSize = execution.DefaultMaxReadSize
			read := &pb.ReadRequest{Resource: "records/s:item"}
			variant := &pb.Command_Read{Read: read}
			command := &pb.Command{Operation: variant}
			record, err := execution.NewRecord("search", 1, command)
			if err != nil {
				t.Fatal(err)
			}
			work, failure := adapter.PrepareRecord(record)
			if failure != nil {
				t.Fatal(failure)
			}
			observations := adapter.mgetIndex(context.Background(), "records", []*execution.Plan{work})
			if observations[0].failure.GetCode() != item.code {
				t.Fatal("warm read classification", observations[0].failure)
			}
			_, failure = adapter.inspectTarget(context.Background(), "records", false)
			if failure.GetCode() != item.code {
				t.Fatal("initial classification differed", failure)
			}
		})
	}
}
