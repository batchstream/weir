package search

import (
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

	pb "github.com/batchstream/weir/api/weir/v1"
)

func TestSearchScanAndNativeRequestTargets(t *testing.T) {
	for _, dialect := range []string{ElasticsearchProduct, OpenSearchProduct} {
		t.Run(dialect, func(t *testing.T) {
			var nativeWrites, cleanups atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/":
					if dialect == OpenSearchProduct {
						fmt.Fprint(w, `{"version":{"distribution":"opensearch"}}`)
					} else {
						fmt.Fprint(w, `{"version":{"build_flavor":"default"}}`)
					}
					return
				case "/_cluster/settings":
					fmt.Fprint(w, `{"persistent":{"action.auto_create_index":"false"}}`)
					return
				case "/_search":
					var body struct {
						PIT   struct{ ID string }
						After []int64 `json:"search_after"`
						Sort  []string
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
						return
					}
					index := strings.TrimSuffix(body.PIT.ID, "-pit")
					if index != "left" && index != "right" {
						t.Error("PIT lost index identity", body.PIT.ID)
					}
					wantSort := "_shard_doc"
					if dialect == OpenSearchProduct {
						wantSort = "_doc"
					}
					if len(body.Sort) != 1 || body.Sort[0] != wantSort {
						t.Error("detected product selected wrong PIT sort", body.Sort)
					}
					fmt.Fprintf(w, `{"pit_id":%q,"took":1,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"max_score":null,"hits":[`, body.PIT.ID)
					if len(body.After) == 0 {
						fmt.Fprintf(w, `{"_index":%q,"_id":"same","_score":null,"_source":{"target":%q},"sort":[0]}`, index, index)
					}
					fmt.Fprint(w, "]}}")
					return
				case "/_pit", "/_search/point_in_time":
					if r.Method != http.MethodDelete {
						t.Error("unscoped PIT open", r.URL.Path)
						return
					}
					cleanups.Add(1)
					var body struct {
						ID  string
						IDs []string `json:"pit_id"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if dialect == OpenSearchProduct {
						if len(body.IDs) != 1 {
							t.Error("wrong OpenSearch PIT cleanup identity", body.IDs)
							return
						}
						fmt.Fprintf(w, `{"pits":[{"pit_id":%q,"successful":true}]}`, body.IDs[0])
					} else {
						if body.ID != "left-pit" && body.ID != "right-pit" {
							t.Error("wrong Elasticsearch PIT cleanup identity", body.ID)
						}
						fmt.Fprint(w, `{"succeeded":true,"num_freed":1}`)
					}
					return
				}
				parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
				index := parts[0]
				if index != "left" && index != "right" {
					t.Error("unexpected request target", r.URL.Path)
					return
				}
				if len(parts) == 1 {
					_, _ = io.WriteString(w, strings.ReplaceAll(testIndexReply, "records", index))
					return
				}
				switch parts[1] {
				case "_pit", "_search":
					if dialect == OpenSearchProduct {
						if r.URL.Path != "/"+index+"/_search/point_in_time" {
							t.Error("wrong OpenSearch PIT path", r.URL.Path)
						}
						fmt.Fprintf(w, `{"pit_id":%q,"creation_time":1,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`, index+"-pit")
					} else {
						if r.URL.Path != "/"+index+"/_pit" {
							t.Error("wrong Elasticsearch PIT path", r.URL.Path)
						}
						fmt.Fprintf(w, `{"id":%q,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`, index+"-pit")
					}
				case "_doc":
					fmt.Fprintf(w, `{"_index":%q,"_id":"same","_source":{"target":%q}}`, index, index)
				case "_bulk":
					nativeWrites.Add(1)
					var metadata map[string]map[string]string
					if err := json.NewDecoder(r.Body).Decode(&metadata); err != nil || metadata["index"]["_index"] != index {
						t.Error("Native bulk crossed target", metadata, err)
					}
					fmt.Fprint(w, `{"errors":false,"items":[]}`)
				default:
					t.Error("unexpected endpoint", r.URL.Path)
				}
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			cfg := Config{Store: "search", URL: server.URL, Pool: 2}
			a, err := Open(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			var scans sync.WaitGroup
			for _, index := range []string{"left", "right"} {
				request := &pb.ScanRequest{Resource: "weir://search/" + index, FetchItemsHint: 1}
				work, failure := a.PrepareScan(request)
				if failure != nil {
					t.Fatal(failure)
				}
				scans.Go(func() {
					defer func() {
						if failure := a.CloseScan(context.Background(), work); failure != nil {
							t.Error(failure)
						}
					}()
					for step := range 3 {
						page, _ := a.FetchScan(context.Background(), work)
						if page.Failure != nil || page.Exhausted != (step == 2) {
							t.Error("Scan target/PIT failure", index, step, page)
							return
						}
						if step == 1 && (len(page.Documents) != 1 || !strings.Contains(string(page.Documents[0].Data), `"_index":"`+index+`"`)) {
							t.Error("Scan returned another index's hit", index, page)
						}
					}
				})
				get := nativeOpen(t, index, "GET", "/_doc/same")
				end, capture := runNative(t, a, get, io.NopCloser(strings.NewReader("")))
				if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || !strings.Contains(capture.body.String(), `"_index":"`+index+`"`) {
					t.Fatal("Native GET used another target", end, capture.body.String())
				}
				bulk := nativeOpen(t, index, "POST", "/_bulk")
				body := fmt.Sprintf("{\"index\":{\"_index\":%q,\"_id\":\"same\"}}\n{}\n", index)
				end, _ = runNative(t, a, bulk, io.NopCloser(strings.NewReader(body)))
				if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE {
					t.Fatal("Native bulk target failed", end)
				}
				wrong := "{\"index\":{\"_index\":\"other\",\"_id\":\"same\"}}\n{}\n"
				end, _ = runNative(t, a, bulk, io.NopCloser(strings.NewReader(wrong)))
				if end.Completion != pb.NativeCompletion_NATIVE_NOT_STARTED || end.GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT {
					t.Fatal("Native item escaped request target", end)
				}
			}
			scans.Wait()
			if nativeWrites.Load() != 2 || cleanups.Load() != 2 {
				t.Fatal("request target operation/cleanup counts", nativeWrites.Load(), cleanups.Load())
			}
		})
	}
}
