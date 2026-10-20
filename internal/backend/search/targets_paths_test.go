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
)

func TestSearchNativeRequestTargets(t *testing.T) {
	for _, dialect := range []string{ElasticsearchProduct, OpenSearchProduct} {
		t.Run(dialect, func(t *testing.T) {
			var nativeWrites atomic.Int32
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
				case "_doc":
					fmt.Fprintf(w, `{"_index":%q,"_id":"same","_source":{"target":%q}}`, index, index)
				case "_bulk":
					decoder := json.NewDecoder(r.Body)
					var metadata map[string]map[string]string
					if err := decoder.Decode(&metadata); err != nil || metadata["index"]["_id"] != "same" {
						t.Error("Native bulk metadata changed", metadata, err)
						return
					}
					var source map[string]json.RawMessage
					if err := decoder.Decode(&source); err != nil || source == nil || len(source) != 0 {
						t.Error("Native bulk source changed", source, err)
						return
					}
					var extra json.RawMessage
					if err := decoder.Decode(&extra); err != io.EOF {
						t.Error("Native bulk upload did not end after one item", err)
						return
					}
					nativeWrites.Add(1)
					fmt.Fprint(w, `{"errors":false,"items":[]}`)
				default:
					t.Error("unexpected endpoint", r.URL.Path)
				}
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			cfg := Config{Store: "search", URL: server.URL}
			a, err := Open(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close(context.Background())
			for _, index := range []string{"left", "right"} {
				get := nativeRequest(t, index, "GET", "/_doc/same")
				end, capture := runNative(t, a, get, nil)
				if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || !strings.Contains(capture.body.String(), `"target":"`+index+`"`) {
					t.Fatal("Native GET used another target", end, capture.body.String())
				}
				bulk := nativeRequest(t, index, "POST", "/_bulk")
				body := fmt.Sprintf("{\"index\":{\"_index\":%q,\"_id\":\"same\"}}\n{}\n", index)
				end, _ = runNative(t, a, bulk, []byte(body))
				if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE {
					t.Fatal("Native bulk target failed", end)
				}
				wrong := "{\"index\":{\"_index\":\"other\",\"_id\":\"same\"}}\n{}\n"
				end, _ = runNative(t, a, bulk, []byte(wrong))
				if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE {
					t.Fatal("Native bulk target was restricted", end)
				}
			}
			if nativeWrites.Load() != 4 {
				t.Fatal("request target operation/cleanup counts", nativeWrites.Load())
			}
		})
	}
}
