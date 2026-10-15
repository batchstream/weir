package search

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	backendcfg "github.com/batchstream/weir/internal/backend"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestNativeHTTPFramingRejectsAmbiguousRequests(t *testing.T) {
	requests := map[string]string{
		"empty":                      "",
		"header terminator":          "GET /_doc/x HTTP/1.1\r\n",
		"old HTTP":                   "GET /_doc/x HTTP/1.0\r\n\r\n",
		"absolute URL":               "GET http://evil/_bulk HTTP/1.1\r\n\r\n",
		"network path":               "GET //evil/_bulk HTTP/1.1\r\n\r\n",
		"HTTP2":                      "GET /_doc/x HTTP/2.0\r\n\r\n",
		"bare newline":               "GET /_doc/x HTTP/1.1\nX-Opaque-Id: x\r\n\r\n",
		"folded header":              "GET /_doc/x HTTP/1.1\r\nX-Opaque-Id: x\r\n y\r\n\r\n",
		"short body":                 "POST /_bulk HTTP/1.1\r\nContent-Type: application/x-ndjson\r\nContent-Length: 2\r\n\r\nx",
		"long body":                  "POST /_bulk HTTP/1.1\r\nContent-Type: application/x-ndjson\r\nContent-Length: 1\r\n\r\nxx",
		"no body length":             "POST /_bulk HTTP/1.1\r\nContent-Type: application/x-ndjson\r\n\r\nx",
		"equal duplicate length":     "GET /_doc/x HTTP/1.1\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
		"different duplicate length": "GET /_doc/x HTTP/1.1\r\nContent-Length: 0\r\nContent-Length: 1\r\n\r\n",
		"chunked":                    "POST /_bulk HTTP/1.1\r\nContent-Type: application/x-ndjson\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
		"chunked deletes length":     "GET /_doc/x HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 0\r\n\r\n0\r\n\r\n",
		"identity encoding":          "GET /_doc/x HTTP/1.1\r\nTransfer-Encoding: identity\r\n\r\n",
		"trailer":                    "GET /_doc/x HTTP/1.1\r\nTrailer: X-End\r\n\r\n",
		"second request":             "GET /_doc/x HTTP/1.1\r\nContent-Length: 0\r\n\r\nGET /_doc/y HTTP/1.1\r\n\r\n",
		"negative length":            "GET /_doc/x HTTP/1.1\r\nContent-Length: -1\r\n\r\n",
	}
	for name, raw := range requests {
		t.Run(name, func(t *testing.T) {
			if _, _, failure := parseNativeHTTPRequest([]byte(raw)); failure == nil {
				t.Fatal("ambiguous HTTP request accepted")
			}
		})
	}
}

func TestNativeHTTPBodyBorrowsLargeInput(t *testing.T) {
	body := []byte("{\"delete\":{\"_id\":\"x\"}}\n")
	head := []byte(fmt.Sprintf("POST /_bulk?refresh=true HTTP/1.1\r\nHost: ignored.invalid\r\nContent-Type: application/x-ndjson\r\nContent-Length: %d\r\n\r\n", len(body)))
	raw := append(head, body...)
	request, parsed, failure := parseNativeHTTPRequest(raw)
	if failure != nil || request.Method != http.MethodPost || !bytes.Equal(parsed, body) || &parsed[0] != &raw[len(head)] || request.Body != http.NoBody {
		t.Fatal("native body copied or retained by HTTP parser", failure)
	}
	for _, length := range []int{(8 << 20), (8 << 20) + 1} {
		// Use a fixed-width length so the whole input is exactly the tested bound.
		header := "POST /_bulk HTTP/1.1\r\nContent-Type: application/x-ndjson\r\nContent-Length: 00000000\r\n\r\n"
		bodyLength := length - len(header)
		header = strings.Replace(header, "00000000", fmt.Sprintf("%08d", bodyLength), 1)
		input := append([]byte(header), bytes.Repeat([]byte("x"), bodyLength)...)
		_, body, failure := parseNativeHTTPRequest(input)
		if failure != nil || len(body) != bodyLength {
			t.Fatal("whole-message byte bound", length, failure)
		}
	}
}

func TestNativeHTTPHeadersBeyondFormerLimit(t *testing.T) {
	header := strings.Repeat("x", 128<<10)
	raw := []byte("GET /_doc/x HTTP/1.1\r\nX-Opaque-Id: " + header + "\r\n\r\n")
	request, _, failure := parseNativeHTTPRequest(raw)
	if failure != nil || request.Header.Get("X-Opaque-Id") != header {
		t.Fatal("large header changed", failure)
	}
	many := []byte("GET /_doc/x HTTP/1.1\r\n" + strings.Repeat("X-Native: arbitrary\r\n", 1000) + "\r\n")
	request, _, failure = parseNativeHTTPRequest(many)
	if failure != nil || len(request.Header.Values("X-Native")) != 1000 {
		t.Fatal("header count changed", failure)
	}
}

func TestNativeHTTPHostCannotSelectBackend(t *testing.T) {
	var called atomic.Bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/records" {
			_, _ = io.WriteString(w, testIndexReply)
			return
		}
		called.Store(true)
		if r.URL.Path != "/records/_doc/x" || r.Host == "evil.invalid" || r.Header.Get("X-Opaque-Id") != "evidence" {
			t.Error("unscoped target", r.Host, r.URL)
		}
		_, _ = io.WriteString(w, "native response")
	})
	backend := httptest.NewServer(handler)
	defer backend.Close()
	config := Config{Store: "search", URL: backend.URL}
	adapter := &Adapter{config: config, dialect: ElasticsearchProduct, nativeClient: backend.Client(), ctx: context.Background()}
	document := &pb.Document{ContentType: "application/http", Data: []byte("GET /_doc/x HTTP/1.1\r\nHost: evil.invalid\r\nX-Opaque-Id: evidence\r\n\r\n")}
	request := &pb.NativeRequest{Resource: "records", Request: document}
	end, capture := runNative(t, adapter, request, nil)
	if !called.Load() || end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || capture.body.String() != "native response" {
		t.Fatal("scoped Native did not complete", end)
	}
	if capture.head.Metadata.GetContentType() != "application/http" || !bytes.HasSuffix(capture.head.Metadata.Data, []byte("\r\n\r\n")) || bytes.Contains(capture.head.Metadata.Data, capture.body.Bytes()) {
		t.Fatal("body entered metadata", capture.head)
	}
}

func TestSearchScanProjectionAcceptsLiteralDollarField(t *testing.T) {
	projection := &pb.Projection{Mode: pb.ProjectionMode_INCLUDE, Fields: []string{"$price"}}
	request := &pb.ScanRequest{Resource: "records", Projection: projection}
	config := Config{Store: "search"}
	adapter := &Adapter{config: config}
	work, failure := adapter.prepareScan(context.Background(), request)
	if failure != nil || work == nil {
		t.Fatal("adapter-independent business field rejected", failure)
	}
}

func TestSearchNativeAdapterOwnsRequestFormat(t *testing.T) {
	adapter := &Adapter{}
	for _, contentType := range []string{"application/bson", "application/vnd.future.store"} {
		document := &pb.Document{ContentType: contentType, Data: []byte{5, 0, 0, 0, 0}}
		request := &pb.NativeRequest{Resource: "records", Request: document}
		if _, failure := adapter.prepareNative(request); failure.GetCode() != pb.FailureCode_UNSUPPORTED {
			t.Fatal("shared envelope interpreted adapter payload", contentType, failure)
		}
	}
}

func TestNativeHTTPPreservesEscapedPathsAndRootRequests(t *testing.T) {
	tests := []struct {
		resource string
		path     string
		want     string
	}{
		{path: "/_msearch?preference=a%2Fb&preference=a+b", want: "/_msearch?preference=a%2Fb&preference=a+b"},
		{path: "/_refresh", want: "/_refresh"},
		{resource: "records", path: "/_update/id%2Fpart%20%3F%252F?refresh=wait_for", want: "/records/_update/id%2Fpart%20%3F%252F?refresh=wait_for"},
		{resource: "records/_doc", path: "/id%2Fpart?", want: "/records/_doc/id%2Fpart?"},
		{resource: "a%2Cb", path: "/_search", want: "/a%2Cb/_search"},
	}
	for _, test := range tests {
		t.Run(test.want, func(t *testing.T) {
			var calls atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				if request.Method != "PATCH" || request.RequestURI != test.want {
					t.Error("Native URI changed", request.Method, request.RequestURI)
				}
				actual, err := io.ReadAll(request.Body)
				if err != nil || string(actual) != `{"doc":{"n":2}}` || request.Header.Get("Content-Type") != "application/json" {
					t.Error("JSON native body changed", err, string(actual), request.Header)
				}
				_, _ = io.WriteString(w, "ok")
			})
			backend := httptest.NewServer(handler)
			defer backend.Close()
			config := Config{Store: "search", URL: backend.URL}
			adapter := &Adapter{config: config, nativeClient: backend.Client(), ctx: context.Background()}
			body := []byte(`{"doc":{"n":2}}`)
			header := fmt.Sprintf("PATCH %s HTTP/1.1\r\nHost: ignored.invalid\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", test.path, len(body))
			document := &pb.Document{ContentType: "application/http", Data: append([]byte(header), body...)}
			request := &pb.NativeRequest{Resource: test.resource, Request: document}
			end, capture := runNative(t, adapter, request, body)
			if calls.Load() != 1 || end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || capture.body.String() != "ok" {
				t.Fatal(calls.Load(), end, capture.body.String())
			}
		})
	}
}

func TestNativeHTTPPreservesCompressedResponse(t *testing.T) {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write([]byte("native compressed response")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Accept-Encoding") != "gzip" {
			t.Error("Native request encoding header changed")
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(compressed.Bytes())
	})
	backend := httptest.NewServer(handler)
	defer backend.Close()
	transport := newTransport(connectionTimeout, backendcfg.DefaultOptions().IdleTimeout)
	transport.DisableKeepAlives = true
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
	config := Config{Store: "search", URL: backend.URL}
	adapter := &Adapter{config: config, nativeClient: client, ctx: context.Background()}
	document := &pb.Document{ContentType: "application/http", Data: []byte("GET /_search HTTP/1.1\r\nAccept-Encoding: gzip\r\n\r\n")}
	request := &pb.NativeRequest{Request: document}
	end, capture := runNative(t, adapter, request, nil)
	if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || !bytes.Equal(capture.body.Bytes(), compressed.Bytes()) || nativeMetadata(t, capture.head).Headers.Get("Content-Encoding") != "gzip" {
		t.Fatal("compressed Native response changed", end)
	}
}

func TestNativeHTTPRedirectCannotSelectAnotherBackend(t *testing.T) {
	var foreignCalls atomic.Int32
	foreignHandler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		foreignCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	foreign := httptest.NewServer(foreignHandler)
	defer foreign.Close()
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Location", foreign.URL+"/foreign")
		w.WriteHeader(http.StatusTemporaryRedirect)
		_, _ = io.WriteString(w, "redirect response")
	})
	backend := httptest.NewServer(handler)
	defer backend.Close()
	transport := newTransport(connectionTimeout, backendcfg.DefaultOptions().IdleTimeout)
	transport.DisableKeepAlives = true
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
	config := Config{Store: "search", URL: backend.URL}
	adapter := &Adapter{config: config, nativeClient: client, ctx: context.Background()}
	request := nativeRequest(t, "", "GET", "/_search")
	end, capture := runNative(t, adapter, request, nil)
	metadata := nativeMetadata(t, capture.head)
	if foreignCalls.Load() != 0 || end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || metadata.StatusCode != 307 || metadata.Headers.Get("Location") != foreign.URL+"/foreign" || capture.body.String() != "redirect response" {
		t.Fatal("Native redirect changed backend or response", foreignCalls.Load(), end, metadata)
	}
}

func TestNativeHTTPConfiguredTransportAcceptsFullMetadataBudget(t *testing.T) {
	value := strings.Repeat("x", 128<<10)
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if qualification(w, request) {
			return
		}
		w.Header().Set("X-Native-Large", value)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"native":"response"}`)
	})
	backend := httptest.NewServer(handler)
	defer backend.Close()
	config := Config{Store: "search", URL: backend.URL}
	adapter, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	request := nativeRequest(t, "records", "GET", "/_search")
	end, capture := runNative(t, adapter, request, nil)
	if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil || capture.head == nil || len(capture.head.Metadata.GetData()) <= 32<<10 || capture.body.String() != `{"native":"response"}` {
		t.Fatal("Native transport imposed a smaller header bound", end)
	}
	metadata := nativeMetadata(t, capture.head)
	if metadata.Headers.Get("X-Native-Large") != value {
		t.Fatal("Native transport truncated a valid response header")
	}
}
