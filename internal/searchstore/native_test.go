package searchstore

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	spb "github.com/batchstream/weir/api/weir/search/v1"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"google.golang.org/protobuf/proto"
)

type nativeCapture struct {
	head   *pb.NativeHead
	body   bytes.Buffer
	chunks int
}

func (c *nativeCapture) Head(h *pb.NativeHead) error { c.head = h; return nil }
func (c *nativeCapture) Chunk(b []byte) error        { c.chunks++; _, err := c.body.Write(b); return err }
func (c *nativeCapture) Interrupt()                  {}
func nativeOpen(t *testing.T, index, method, path string) *pb.NativeOpen {
	t.Helper()
	descriptor := &spb.Request{Method: method, Path: path}
	raw, err := proto.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{MediaType: NativeDescriptor, Data: raw}
	media := ""
	if method == "POST" {
		media = "application/x-ndjson"
	}
	open := &pb.NativeOpen{Resource: "weir://search/" + index, Descriptor_: document, BodyMediaType: media}
	return open
}
func runNative(t *testing.T, a *Adapter, open *pb.NativeOpen, body io.ReadCloser) (*pb.NativeEnd, *nativeCapture) {
	t.Helper()
	p, f := a.PrepareNative(open)
	if f != nil {
		t.Fatal(f)
	}
	capture := &nativeCapture{}
	exchange := &execution.NativeExchange{Source: body, Sink: capture}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	end, _ := a.ExecuteNative(ctx, p, exchange)
	return end, capture
}
func TestNativeHTTPDescriptorScope(t *testing.T) {
	for _, path := range []string{"http://evil/_bulk", "//evil/_bulk", "/../_bulk", "/%2e%2e/_bulk", "/_doc/%2Fetc", "/_doc/%252fetc", "/_doc/..", "/_reindex", "/_search", "/_doc/x?x=y"} {
		d := &spb.Request{Method: "GET", Path: path}
		if nativeDescriptor(d, "") == nil {
			t.Fatal(path)
		}
	}
	for _, query := range []string{"pipeline=p", "refresh=true&refresh=false", "refresh=%74rue", "routing=x", "filter_path=items", "realtime=true%0D%0Ax:y"} {
		d := &spb.Request{Method: "POST", Path: "/_bulk", Query: query}
		if nativeDescriptor(d, "application/x-ndjson") == nil {
			t.Fatal(query)
		}
	}
	for _, name := range []string{"host", "authorization", "proxy-authorization", "connection", "transfer-encoding", "content-length", "idempotency-key", "x-idempotency-key"} {
		h := &spb.Header{Name: name, Values: []string{"x"}}
		d := &spb.Request{Method: "POST", Path: "/_bulk", Headers: []*spb.Header{h}}
		if nativeDescriptor(d, "application/x-ndjson") == nil {
			t.Fatal(name)
		}
	}
	h := &spb.Header{Name: "x-opaque-id", Values: []string{"x\r\nHost: evil"}}
	d := &spb.Request{Method: "GET", Path: "/_doc/x", Headers: []*spb.Header{h}}
	if nativeDescriptor(d, "") == nil {
		t.Fatal("header injection")
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
	for _, mode := range []string{"empty", "headers", "redirect", "multiframe", "large", "truncated", "length", "drop", "trailers", "upgrade", "boundary", "large_chunked"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
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
				case "multiframe":
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
			cfg := Config{Store: "search", Index: "records", URL: backend.URL}
			a := &Adapter{config: cfg, nativeClient: client}
			open := nativeOpen(t, "records", "GET", "/_doc/x")
			end, capture := runNative(t, a, open, io.NopCloser(strings.NewReader("")))
			complete := mode == "empty" || mode == "headers" || mode == "redirect" || mode == "multiframe" || mode == "boundary"
			if (end.Completion == pb.NativeCompletion_RESPONSE_COMPLETE) != complete || calls.Load() != 1 {
				t.Fatal(end, calls.Load())
			}
			if complete && end.Failure != nil {
				t.Fatal(end)
			}
			if mode == "boundary" && capture.body.Len() != NativeResponseLimit {
				t.Fatal("response boundary", capture.body.Len())
			}
			if mode == "multiframe" && (capture.chunks < 2 || capture.body.Len() != 150<<10) {
				t.Fatal(capture.chunks, capture.body.Len())
			}
			if mode == "headers" {
				meta := &spb.Response{}
				proto.Unmarshal(capture.head.Metadata.Data, meta)
				if meta.StatusCode != 400 {
					t.Fatal(meta)
				}
				found := false
				for _, h := range meta.Headers {
					if h.Name == "warning" {
						found = len(h.Values) == 2
					}
				}
				if !found {
					t.Fatal(meta)
				}
			}
		})
	}
}
