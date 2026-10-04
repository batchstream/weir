package search

import (
	"bytes"
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
)

func TestNativeHTTPFramingRejectsAmbiguousRequests(t *testing.T) {
	requests := map[string]string{
		"empty":                      "",
		"header terminator":          "GET /_doc/x HTTP/1.1\r\n",
		"old HTTP":                   "GET /_doc/x HTTP/1.0\r\n\r\n",
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
		"expect":                     "GET /_doc/x HTTP/1.1\r\nExpect: 100-continue\r\n\r\n",
		"compression":                "GET /_doc/x HTTP/1.1\r\nContent-Encoding: gzip\r\n\r\n",
		"second request":             "GET /_doc/x HTTP/1.1\r\nContent-Length: 0\r\n\r\nGET /_doc/y HTTP/1.1\r\n\r\n",
		"negative length":            "GET /_doc/x HTTP/1.1\r\nContent-Length: -1\r\n\r\n",
		"GET body":                   "GET /_doc/x HTTP/1.1\r\nContent-Length: 1\r\n\r\nx",
	}
	for name, raw := range requests {
		t.Run(name, func(t *testing.T) {
			if _, _, failure := parseNativeHTTPRequest([]byte(raw)); failure == nil {
				t.Fatal("ambiguous HTTP request accepted")
			}
		})
	}
}

func TestNativeHTTPBodyBorrowsInputWithinWholeMessageLimit(t *testing.T) {
	body := []byte("{\"delete\":{\"_id\":\"x\"}}\n")
	head := []byte(fmt.Sprintf("POST /_bulk?refresh=true HTTP/1.1\r\nHost: ignored.invalid\r\nContent-Type: application/x-ndjson\r\nContent-Length: %d\r\n\r\n", len(body)))
	raw := append(head, body...)
	request, parsed, failure := parseNativeHTTPRequest(raw)
	if failure != nil || request.Method != http.MethodPost || !bytes.Equal(parsed, body) || &parsed[0] != &raw[len(head)] || request.Body != http.NoBody {
		t.Fatal("native body copied or retained by HTTP parser", failure)
	}
	for _, length := range []int{protocol.MaxNativeRequestBytes, protocol.MaxNativeRequestBytes + 1} {
		// Use a fixed-width length so the whole input is exactly the tested bound.
		header := "POST /_bulk HTTP/1.1\r\nContent-Type: application/x-ndjson\r\nContent-Length: 00000000\r\n\r\n"
		bodyLength := length - len(header)
		header = strings.Replace(header, "00000000", fmt.Sprintf("%08d", bodyLength), 1)
		input := append([]byte(header), bytes.Repeat([]byte("x"), bodyLength)...)
		_, body, failure := parseNativeHTTPRequest(input)
		if length == protocol.MaxNativeRequestBytes && (failure != nil || len(body) != bodyLength) || length > protocol.MaxNativeRequestBytes && failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
			t.Fatal("whole-message byte bound", length, failure)
		}
	}
}

func TestNativeHTTPHeaderBoundPrecedesParserAllocation(t *testing.T) {
	raw := []byte("GET /_doc/x HTTP/1.1\r\nX-Opaque-Id: " + strings.Repeat("x", protocol.MaxNativeMetadataBytes) + "\r\n\r\n")
	allocations := testing.AllocsPerRun(3, func() {
		if _, _, failure := parseNativeHTTPRequest(raw); failure == nil {
			t.Fatal("oversized header parsed")
		}
	})
	if allocations > 8 {
		t.Fatal("oversized HTTP header allocated before rejection", allocations)
	}
	many := []byte("GET /_doc/x HTTP/1.1\r\n" + strings.Repeat("Accept: application/json\r\n", 1000) + "\r\n")
	allocations = testing.AllocsPerRun(3, func() {
		if _, _, failure := parseNativeHTTPRequest(many); failure == nil {
			t.Fatal("excessive header entries parsed")
		}
	})
	if allocations > 8 {
		t.Fatal("header expansion allocated before rejection", allocations)
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
	work, failure := adapter.prepareScan(request)
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
