//go:build integration

package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testsearch"
)

func TestSearchNativeRealMixedAndReplyLoss(t *testing.T) {
	for _, mode := range []string{"mixed", "multichunk", "drop", "cross_index", "pipeline"} {
		t.Run(mode, func(t *testing.T) {
			backend := testsearch.Open(t)
			otherIndex := backend.Index + "_other"
			backend.Create(t, otherIndex, `{"settings":{"number_of_shards":1,"number_of_replicas":0}}`)
			var calls atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				request, err := http.NewRequestWithContext(r.Context(), r.Method, backend.URL+r.URL.RequestURI(), r.Body)
				if err != nil {
					return
				}
				request.Header = r.Header.Clone()
				request.GetBody = nil
				response, err := backend.Client.Do(request)
				if err != nil {
					w.WriteHeader(502)
					return
				}
				defer response.Body.Close()
				if strings.HasSuffix(r.URL.Path, "/_bulk") {
					calls.Add(1)
					if mode == "drop" {
						io.Copy(io.Discard, response.Body)
						conn, _, _ := w.(http.Hijacker).Hijack()
						conn.Close()
						return
					}
				}
				for key, values := range response.Header {
					for _, v := range values {
						w.Header().Add(key, v)
					}
				}
				w.WriteHeader(response.StatusCode)
				io.Copy(w, response.Body)
			})
			proxy := httptest.NewServer(handler)
			defer proxy.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cfg := Config{Store: "search", URL: proxy.URL}
			a, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			body := "{\"create\":{\"_id\":\"x\"}}\n{\"n\":1}\n"
			if mode == "mixed" {
				body += body + "{\"index\":{\"_id\":\"bad\"}}\n{\"n\":{\"bad\":1}}\n"
			}
			if mode == "multichunk" {
				body = "{\"index\":{\"_id\":\"x\"}}\n{\"pad\":\"" + strings.Repeat("x", 512<<10) + "\"}\n"
			}
			if mode == "cross_index" {
				body = strings.Repeat("{\"index\":{\"_id\":\"x\"}}\n{\"n\":1}\n", 100) + fmt.Sprintf("{\"delete\":{\"_id\":\"x\",\"_index\":%q}}\n", otherIndex)
			}
			if mode == "pipeline" {
				status, _ := backend.Do(t, "PUT", "/"+backend.Index+"/_settings", `{"index.default_pipeline":"missing"}`)
				if status != 200 {
					t.Fatal(status)
				}
			}
			open := nativeRequest(t, backend.Index, "POST", "/_bulk")
			end, capture := runNative(t, a, open, []byte(body))
			expected := pb.NativeCompletion_RESPONSE_COMPLETE
			if mode == "drop" {
				expected = pb.NativeCompletion_RESPONSE_INCOMPLETE
			}
			if end.Completion != expected {
				t.Fatal(end, capture.body.String())
			}
			if calls.Load() > 1 || mode == "drop" && calls.Load() != 1 {
				t.Fatal("replay", calls.Load())
			}
			if mode == "mixed" && (!strings.Contains(capture.body.String(), `"errors":true`) || !strings.Contains(capture.body.String(), `version_conflict_engine_exception`)) {
				t.Fatal(capture.body.String())
			}
			if mode == "drop" || mode == "multichunk" || mode == "cross_index" {
				status, raw := backend.Do(t, "GET", "/"+backend.Index+"/_doc/x", "")
				if status != 200 {
					t.Fatal(status, string(raw))
				}
				t.Logf("%s native bulk calls=%d independent GET status=%d", mode, calls.Load(), status)
			}
			if mode == "multichunk" {
				open = nativeRequest(t, backend.Index, "GET", "/_doc/x")
				end, capture = runNative(t, a, open, nil)
				if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || capture.chunks < 2 {
					t.Fatal(end, capture.chunks)
				}
			}
		})
	}
}

func TestSearchNativeCreatesIndexesSearchesUpdatesAndUsesRootPaths(t *testing.T) {
	backend := testsearch.Open(t)
	config := Config{Store: "search", URL: backend.URL}
	adapter, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	index := backend.Index + "_native"
	t.Cleanup(func() {
		status, raw := backend.Do(t, http.MethodDelete, "/"+index, "")
		if status != 200 && status != 404 {
			t.Error("Native test index cleanup failed", status, string(raw))
		}
	})
	tests := []struct {
		resource string
		method   string
		path     string
		body     string
		status   int
		contains string
	}{
		{resource: index, method: "PUT", path: "/", body: `{"settings":{"number_of_shards":1,"number_of_replicas":0}}`, status: 200, contains: `"acknowledged":true`},
		{resource: index, method: "PUT", path: "/_doc/" + url.PathEscape("part/id ?中文") + "?refresh=wait_for", body: `{"n":1}`, status: 201, contains: `"result":"created"`},
		{resource: index, method: "POST", path: "/_update/" + url.PathEscape("part/id ?中文") + "?refresh=wait_for", body: `{"doc":{"n":2}}`, status: 200, contains: `"result":"updated"`},
		{method: "POST", path: "/_refresh", status: 200, contains: `"failed":0`},
		{resource: index, method: "GET", path: "/_search?size=1&from=0&track_total_hits=true&preference=tenant%2Fone", body: `{"query":{"term":{"n":2}}}`, status: 200, contains: `"n":2`},
		{method: "POST", path: "/_msearch", body: fmt.Sprintf("{\"index\":%q}\n{\"query\":{\"match_all\":{}}}\n", index), status: 200, contains: `"responses"`},
		{resource: index, method: "POST", path: "/_search", body: `{"query":{"unknown_native_query":{}}}`, status: 400, contains: `"error"`},
	}
	for _, test := range tests {
		t.Run(test.method+test.path, func(t *testing.T) {
			httpRequest, err := http.NewRequest(test.method, "http://ignored.invalid"+test.path, bytes.NewBufferString(test.body))
			if err != nil {
				t.Fatal(err)
			}
			contentType := "application/json"
			if test.path == "/_msearch" {
				contentType = "application/x-ndjson"
			}
			httpRequest.Header.Set("Content-Type", contentType)
			httpRequest.Header.Set("X-Opaque-Id", "native-open-integration")
			request, err := weirclient.NewHTTPNativeRequest(test.resource, httpRequest)
			if err != nil {
				t.Fatal(err)
			}
			end, capture := runNative(t, adapter, request, []byte(test.body))
			metadata := nativeMetadata(t, capture.head)
			if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil || metadata.StatusCode != test.status || metadata.Headers.Get("X-Opaque-Id") != "native-open-integration" || !strings.Contains(capture.body.String(), test.contains) {
				t.Fatal("Native request or response changed", end, metadata, capture.body.String())
			}
			if test.resource == index && strings.HasPrefix(test.path, "/_search?") {
				var result struct {
					Hits struct {
						Total struct{ Value int }
						Hits  []json.RawMessage
					}
				}
				if err := json.Unmarshal(capture.body.Bytes(), &result); err != nil || result.Hits.Total.Value != 1 || len(result.Hits.Hits) != 1 {
					t.Fatal("Native search query or pagination changed", err, capture.body.String())
				}
			}
		})
	}
}
