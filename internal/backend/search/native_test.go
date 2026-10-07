package search

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil"
)

type nativeCapture struct {
	head              *pb.NativeHead
	body              bytes.Buffer
	chunks            int
	headErr, chunkErr error
	cancelAfterHead   context.CancelFunc
}

func (c *nativeCapture) Head(h *pb.NativeHead) error {
	if c.headErr != nil {
		return c.headErr
	}
	c.head = h
	if c.cancelAfterHead != nil {
		c.cancelAfterHead()
	}
	return nil
}
func (c *nativeCapture) Chunk(b []byte) error {
	if c.chunkErr != nil {
		return c.chunkErr
	}
	c.chunks++
	_, err := c.body.Write(b)
	return err
}
func (c *nativeCapture) Emit(_ *execution.Plan, event *pb.Event) error {
	if head := event.GetHead(); head != nil {
		return c.Head(head)
	}
	return c.Chunk(event.GetChunk())
}
func nativeRequest(t *testing.T, index, method, path string) *pb.NativeRequest {
	t.Helper()
	httpRequest, err := http.NewRequest(method, "http://ignored.invalid"+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if method == http.MethodPost {
		httpRequest.Header.Set("Content-Type", "application/x-ndjson")
	}
	request, err := weirclient.NewHTTPNativeRequest(index, httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func setNativeBody(t *testing.T, request *pb.NativeRequest, body []byte) {
	t.Helper()
	parsed, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(request.Request.Data)))
	if err != nil {
		t.Fatal(err)
	}
	parsed.Body = io.NopCloser(bytes.NewReader(body))
	parsed.ContentLength = int64(len(body))
	updated, err := weirclient.NewHTTPNativeRequest(request.Resource, parsed)
	if err != nil {
		t.Fatal(err)
	}
	request.Request = updated.Request
}

func nativeMetadata(t *testing.T, head *pb.NativeHead) *weirclient.HTTPNativeResponse {
	t.Helper()
	metadata, err := weirclient.ParseHTTPNativeResponse(head)
	if err != nil {
		t.Fatal(err)
	}
	return metadata
}

func runNative(t *testing.T, a *Adapter, request *pb.NativeRequest, body []byte) (*pb.NativeEnd, *nativeCapture) {
	t.Helper()
	setNativeBody(t, request, body)
	p, f := a.prepareNative(request)
	if f != nil {
		t.Fatal(f)
	}
	capture := &nativeCapture{}
	p.Command = testutil.NativeCommand(request)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	end := a.executeNative(ctx, p, capture.Emit)
	return end, capture
}

func TestNativeHTTPRequestScope(t *testing.T) {
	for _, path := range []string{"http://evil/_bulk", "//evil/_bulk", "/../_bulk", "/%2e%2e/_bulk", "/_doc/%2Fetc", "/_doc/%252fetc", "/_doc/..", "/_reindex", "/_search", "/_doc/x?x=y"} {
		raw := []byte("GET " + path + " HTTP/1.1\r\n\r\n")
		if _, _, failure := parseNativeHTTPRequest(raw); failure == nil {
			t.Fatal(path)
		}
	}
	for _, query := range []string{"pipeline=p", "refresh=true&refresh=false", "refresh=%74rue", "routing=x", "filter_path=items", "realtime=true%0D%0Ax:y"} {
		raw := []byte("POST /_bulk?" + query + " HTTP/1.1\r\nContent-Type: application/x-ndjson\r\n\r\n")
		if _, _, failure := parseNativeHTTPRequest(raw); failure == nil {
			t.Fatal(query)
		}
	}
	for _, name := range []string{"authorization", "proxy-authorization", "connection", "transfer-encoding", "idempotency-key", "x-idempotency-key"} {
		raw := []byte("GET /_doc/x HTTP/1.1\r\n" + name + ": x\r\n\r\n")
		if _, _, failure := parseNativeHTTPRequest(raw); failure == nil {
			t.Fatal(name)
		}
	}
}
func TestNativeBulkItemAndBodyBounds(t *testing.T) {
	valid := "{\"create\":{\"_id\":\"x\",\"_index\":\"records\"}}\n{\"n\":1}\n"
	for _, suffix := range []string{"{\"delete\":{\"_id\":\"x\",\"_index\":\"other\"}}\n", "{\"index\":{\"_id\":\"x\",\"pipeline\":\"p\"}}\n{}\n", "{\"update\":{\"_id\":\"x\"}}\n{}\n", "{\"delete\":{\"_id\":\"x\",\"routing\":\"a\"}}\n", "{\"delete\":{\"_id\":\"x\"}}", "\n"} {
		reader := &nativeBulkReader{reader: bufio.NewReaderSize(strings.NewReader(valid+suffix), 4096), index: "records"}
		first, err := reader.item()
		if err != nil || string(first) != valid {
			t.Fatal(err)
		}
		if _, err := reader.item(); err == nil || err == io.EOF {
			t.Fatal("invalid suffix forwarded", suffix)
		}
	}
	prefix := "{\"index\":{\"_id\":\"x\"}}\n{\"pad\":\""
	suffix := "\"}\n"
	for _, extra := range []int{0, 1} {
		body := prefix + strings.Repeat("x", NativeItemLimit-len(prefix)-len(suffix)+extra) + suffix
		reader := &nativeBulkReader{reader: bufio.NewReaderSize(strings.NewReader(body), 4096), index: "records"}
		raw, err := reader.item()
		if extra == 0 && (err != nil || len(raw) != NativeItemLimit) || extra == 1 && err == nil {
			t.Fatal(extra, err, len(raw))
		}
	}
	repeated := strings.Repeat(prefix+strings.Repeat("x", NativeItemLimit-len(prefix)-len(suffix))+suffix, 33)
	reader := &nativeBulkReader{reader: bufio.NewReaderSize(strings.NewReader(repeated), 4096), index: "records"}
	for range 32 {
		if _, err := reader.item(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := reader.item(); err == nil {
		t.Fatal("cumulative body limit")
	}
}
func TestNativeHTTPSyntheticFraming(t *testing.T) {
	for _, mode := range []string{"empty", "headers", "redirect", "multichunk", "large", "truncated", "length", "drop", "trailers", "upgrade", "boundary", "large_chunked", "000", "099", "600", "999"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/records" {
					_, _ = io.WriteString(w, testIndexReply)
					return
				}
				calls.Add(1)
				if mode == "000" || mode == "099" || mode == "600" || mode == "999" {
					conn, buffer, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					fmt.Fprintf(buffer, "HTTP/1.1 %s Invalid\r\nContent-Length: 0\r\n\r\n", mode)
					_ = buffer.Flush()
					_ = conn.Close()
					return
				}
				if mode == "drop" {
					conn, _, _ := w.(http.Hijacker).Hijack()
					conn.Close()
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch mode {
				case "headers":
					w.Header().Add("Warning", "one")
					w.Header().Add("Warning", "two")
					w.WriteHeader(400)
					io.WriteString(w, `{"error":"native"}`)
				case "trailers":
					w.Header().Set("Trailer", "X-Native-Trailer")
					io.WriteString(w, "body")
					w.Header().Set("X-Native-Trailer", "value")
				case "upgrade":
					w.WriteHeader(101)
				case "redirect":
					w.Header().Set("Location", "http://127.0.0.1:1/evil")
					w.WriteHeader(307)
				case "multichunk":
					io.WriteString(w, strings.Repeat("x", 150<<10))
				case "boundary", "large_chunked":
					w.(http.Flusher).Flush()
					size := NativeResponseLimit
					if mode == "large_chunked" {
						size++
					}
					io.WriteString(w, strings.Repeat("x", size))
				case "large":
					w.Header().Set("Content-Length", fmt.Sprint(NativeResponseLimit+1))
				case "length":
					w.Header().Set("Content-Length", "100")
					io.WriteString(w, "short")
				case "truncated":
					conn, b, _ := w.(http.Hijacker).Hijack()
					fmt.Fprint(b, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nabc")
					b.Flush()
					conn.Close()
				}
			})
			backend := httptest.NewServer(handler)
			defer backend.Close()
			transport := newTransport(1)
			transport.DisableKeepAlives = true
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
			cfg := Config{Store: "search", URL: backend.URL}
			a := &Adapter{dialect: ElasticsearchProduct, config: cfg, nativeClient: client, ctx: context.Background()}
			open := nativeRequest(t, "records", "GET", "/_doc/x")
			end, capture := runNative(t, a, open, nil)
			complete := mode == "empty" || mode == "headers" || mode == "redirect" || mode == "multichunk" || mode == "boundary"
			if (end.Completion == pb.NativeCompletion_RESPONSE_COMPLETE) != complete || calls.Load() != 1 {
				t.Fatal(end, calls.Load())
			}
			if complete && end.Failure != nil {
				t.Fatal(end)
			}
			if mode == "000" || mode == "099" || mode == "600" || mode == "999" {
				if end.Completion != pb.NativeCompletion_RESPONSE_INCOMPLETE || end.GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE || capture.head != nil || capture.body.Len() != 0 {
					t.Fatal("invalid backend status published as complete", end, capture.head)
				}
			}
			if mode == "boundary" && capture.body.Len() != NativeResponseLimit {
				t.Fatal("response boundary", capture.body.Len())
			}
			if mode == "multichunk" && (capture.chunks < 2 || capture.body.Len() != 150<<10) {
				t.Fatal(capture.chunks, capture.body.Len())
			}
			if mode == "headers" {
				meta := nativeMetadata(t, capture.head)
				if meta.StatusCode != 400 {
					t.Fatal(meta)
				}
				if len(meta.Headers.Values("Warning")) != 2 {
					t.Fatal(meta)
				}
			}
		})
	}
}

func TestNativeHTTPExplicitCongestion(t *testing.T) {
	for _, test := range []struct {
		name            string
		method          string
		status          int
		body            string
		truncated       bool
		oversized       bool
		failHead        bool
		failChunk       bool
		cancelAfterHead bool
		completion      pb.NativeCompletion
	}{
		{name: "complete_get", status: 200, body: `{"_index":"records","_id":"x","found":true}`, completion: pb.NativeCompletion_RESPONSE_COMPLETE},
		{name: "empty_get", status: 204, completion: pb.NativeCompletion_RESPONSE_COMPLETE},
		{name: "complete_bulk", method: "POST", status: 200, body: `{"errors":false,"items":[]}`, completion: pb.NativeCompletion_RESPONSE_COMPLETE},
		{name: "capacity", status: 429, body: "opaque capacity response\n", completion: pb.NativeCompletion_RESPONSE_COMPLETE},
		{name: "unavailable", status: 503, body: "opaque unavailable response\n", completion: pb.NativeCompletion_RESPONSE_COMPLETE},
		{name: "native_bad_request", status: 400, body: `{"error":"native request"}`, completion: pb.NativeCompletion_RESPONSE_COMPLETE},
		{name: "mixed_bulk", method: "POST", status: 200, body: `{"errors":true,"items":[{"index":{"status":429}}]}`, completion: pb.NativeCompletion_RESPONSE_COMPLETE},
		{name: "unknown_http_error", status: 500, body: `{"error":"unknown"}`, completion: pb.NativeCompletion_RESPONSE_COMPLETE},
		{name: "redirect", status: 307, completion: pb.NativeCompletion_RESPONSE_COMPLETE},
		{name: "canceled_get", status: 200, body: "reply", cancelAfterHead: true, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE},
		{name: "truncated_capacity", status: 429, body: "short", truncated: true, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE},
		{name: "oversized_capacity", status: 429, oversized: true, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE},
		{name: "head_failure", status: 503, body: "unavailable", failHead: true, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE},
		{name: "chunk_failure", status: 429, body: "capacity", failChunk: true, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/records" {
					_, _ = io.WriteString(w, testIndexReply)
					return
				}
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				if test.truncated {
					w.Header().Set("Content-Length", "100")
				}
				if test.oversized {
					w.Header().Set("Content-Length", fmt.Sprint(NativeResponseLimit+1))
				}
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			})
			backend := httptest.NewServer(handler)
			defer backend.Close()
			transport := newTransport(1)
			transport.DisableKeepAlives = true
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
			cfg := Config{Store: "search", URL: backend.URL}
			a := &Adapter{dialect: ElasticsearchProduct, config: cfg, nativeClient: client, ctx: context.Background()}
			method, path, input := http.MethodGet, "/_doc/x", ""
			if test.method == http.MethodPost {
				method, path = http.MethodPost, "/_bulk"
				input = "{\"index\":{\"_index\":\"records\",\"_id\":\"x\"}}\n{}\n"
			}
			open := nativeRequest(t, "records", method, path)
			setNativeBody(t, open, []byte(input))
			plan, failure := a.prepareNative(open)
			if failure != nil {
				t.Fatal(failure)
			}
			capture := &nativeCapture{}
			if test.failHead {
				capture.headErr = errors.New("output unavailable")
			}
			if test.failChunk {
				capture.chunkErr = errors.New("output unavailable")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if test.cancelAfterHead {
				capture.cancelAfterHead = cancel
			}
			plan.Command = testutil.NativeCommand(open)
			end := a.executeNative(ctx, plan, capture.Emit)
			if end.Completion != test.completion || calls.Load() != 1 {
				t.Fatal(end, calls.Load())
			}
			if test.completion == pb.NativeCompletion_RESPONSE_COMPLETE {
				metadata := nativeMetadata(t, capture.head)
				if int(metadata.StatusCode) != test.status || capture.body.String() != test.body || end.Failure != nil {
					t.Fatal("native reply changed", metadata, capture.body.String(), end)
				}
			}
		})
	}
}

func TestNativeHTTPQualificationCongestion(t *testing.T) {
	for _, status := range []int{429, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/records" || r.Method != http.MethodGet {
					t.Error("Native command dispatched after qualification rejection", r.Method, r.URL.Path)
				}
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"error":{"type":"capacity"}}`)
			})
			backend := httptest.NewServer(handler)
			defer backend.Close()
			transport := newTransport(1)
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
			cfg := Config{Store: "search", URL: backend.URL}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			a := &Adapter{dialect: ElasticsearchProduct, config: cfg, client: client, nativeClient: client, ctx: ctx}
			open := nativeRequest(t, "records", "POST", "/_bulk")
			setNativeBody(t, open, []byte("{\"index\":{\"_id\":\"x\"}}\n{}\n"))
			plan, failure := a.prepareNative(open)
			if failure != nil {
				t.Fatal(failure)
			}
			capture := &nativeCapture{}
			plan.Command = testutil.NativeCommand(open)
			end := a.executeNative(ctx, plan, capture.Emit)
			if end.Completion != pb.NativeCompletion_NATIVE_NOT_STARTED || calls.Load() != 1 || capture.head != nil || capture.body.Len() != 0 {
				t.Fatal(end, calls.Load(), capture)
			}
		})
	}
}

func TestNativeHTTPBodyCloseReleasesInput(t *testing.T) {
	body := &nativeHTTPBody{Reader: strings.NewReader("bounded input")}
	var buffer [4]byte
	if n, err := body.Read(buffer[:]); n != 4 || err != nil {
		t.Fatal(n, err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if body.Reader != nil {
		t.Fatal("closed HTTP request still retains Native input")
	}
	if n, err := body.Read(buffer[:]); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("closed upload still reads borrowed input", n, err)
	}
	if err := body.Close(); err != nil {
		t.Fatal("repeated close", err)
	}
}
