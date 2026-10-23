package search

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type nativePlan struct {
	resource string
	raw      []byte
}

func (a *Adapter) prepareNative(request *pb.NativeRequest) (*execution.Plan, *pb.Failure) {
	if f := protocol.ValidateNative(request); f != nil {
		return nil, f
	}
	if request.Request.ContentType != "application/http" {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Search Native requires application/http")
	}
	if _, failure := scanNativeHTTPRequest(request.Request.Data); failure != nil {
		return nil, failure
	}
	native := &nativePlan{resource: request.Resource, raw: request.Request.Data}
	p := &execution.Plan{Key: request.Resource, Backend: native}
	return p, nil
}

// scanNativeHTTPRequest checks the request framing without allocating and
// returns the length of the request head.
func scanNativeHTTPRequest(raw []byte) (int, *pb.Failure) {
	invalid := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid Native HTTP framing")
	end := bytes.Index(raw, []byte("\r\n\r\n"))
	if end < 0 {
		return 0, invalid
	}
	head := raw[:end+4]
	remaining := head[:len(head)-2]
	lengths, lines := 0, 0
	for len(remaining) != 0 {
		line, rest, _ := bytes.Cut(remaining, []byte("\r\n"))
		remaining = rest
		lines++
		if bytes.ContainsAny(line, "\r\n") || lines > 1 && len(line) > 0 && (line[0] == ' ' || line[0] == '\t') {
			return 0, invalid
		}
		if lines == 1 || len(line) == 0 {
			continue
		}
		colon := bytes.IndexByte(line, ':')
		if colon < 1 {
			return 0, invalid
		}
		name := line[:colon]
		if bytes.EqualFold(name, []byte("Transfer-Encoding")) || bytes.EqualFold(name, []byte("Trailer")) {
			return 0, invalid
		}
		if bytes.EqualFold(name, []byte("Content-Length")) {
			lengths++
			if lengths > 1 {
				return 0, invalid
			}
		}
	}
	return len(head), nil
}

// parseNativeHTTPRequest parses the request head; the body stays a slice of
// the Native input.
func parseNativeHTTPRequest(raw []byte) (*http.Request, []byte, *pb.Failure) {
	headBytes, failure := scanNativeHTTPRequest(raw)
	if failure != nil {
		return nil, nil, failure
	}
	invalid := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid Native HTTP framing")
	head := raw[:headBytes]
	request, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(head)))
	if err != nil {
		return nil, nil, invalid
	}
	_ = request.Body.Close()
	request.Body = http.NoBody
	body := raw[headBytes:]
	if request.Proto != "HTTP/1.1" || request.URL.IsAbs() || request.URL.Host != "" || request.URL.Opaque != "" ||
		!strings.HasPrefix(request.RequestURI, "/") || strings.HasPrefix(request.RequestURI, "//") ||
		len(request.TransferEncoding) != 0 || len(request.Trailer) != 0 || request.ContentLength != int64(len(body)) ||
		request.Header.Get("Content-Length") == "" && len(body) != 0 {
		return nil, nil, invalid
	}
	return request, body, nil
}

func (a *Adapter) executeNative(ctx context.Context, p *execution.Plan, emit execution.Emit) *pb.NativeEnd {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if a.ctx != nil {
		if a.ctx.Err() != nil {
			cancel()
		}
		stop := context.AfterFunc(a.ctx, cancel)
		defer stop()
	}
	native := p.Backend.(*nativePlan)
	if ctx.Err() != nil {
		return protocol.NativeFailure(false, protocol.ContextFailure(ctx))
	}
	d, nativeBody, failure := parseNativeHTTPRequest(native.raw)
	if failure != nil {
		return protocol.NativeFailure(false, failure)
	}
	path := d.URL.EscapedPath()
	if native.resource != "" {
		parts, _ := protocol.ParseRelativeResource(native.resource)
		for i := range parts {
			parts[i] = url.PathEscape(parts[i])
		}
		path = "/" + strings.Join(parts, "/") + path
	}
	endpoint := a.config.URL + path
	if d.URL.RawQuery != "" || d.URL.ForceQuery {
		endpoint += "?" + d.URL.RawQuery
	}
	var body io.Reader
	if len(nativeBody) != 0 {
		body = bytes.NewReader(nativeBody)
	}
	request, err := http.NewRequestWithContext(ctx, d.Method, endpoint, body)
	if err != nil {
		return protocol.NativeFailure(false, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid HTTP request"))
	}
	a.configureRequest(request)

	for name, values := range d.Header {
		if strings.EqualFold(name, "content-length") {
			continue
		}
		request.Header[name] = append([]string(nil), values...)
	}
	request.ContentLength = int64(len(nativeBody))
	// Closing the body terminates this exchange on cancellation. net/http
	// has at most one writer/read loop for this one non-reused HTTP/1 connection.
	if body != nil {
		request.Body = &nativeHTTPBody{Reader: body}
		defer request.Body.Close()
	}
	response, err := a.nativeClient.Do(request)
	if err != nil {
		return protocol.NativeFailure(true, nativeExchangeFailure(ctx, "Native HTTP exchange incomplete"))
	}
	defer response.Body.Close()
	if response.StatusCode < 100 || response.StatusCode > 599 {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "invalid Native HTTP response status"))
	}
	if len(response.Trailer) != 0 ||
		response.StatusCode == http.StatusSwitchingProtocols {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNSUPPORTED, "unsupported Native HTTP response framing"))
	}
	var metadata bytes.Buffer
	fmt.Fprintf(&metadata, "%s %s\r\n", response.Proto, response.Status)
	names := make([]string, 0, len(response.Header))
	for name := range response.Header {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		for _, value := range response.Header.Values(name) {
			fmt.Fprintf(&metadata, "%s: %s\r\n", name, value)
		}
	}
	metadata.WriteString("\r\n")
	contentType, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
	document := &pb.Document{ContentType: "application/http", Data: metadata.Bytes()}
	head := &pb.NativeHead{Metadata: document, BodyContentType: contentType}
	value := &pb.Event_Head{Head: head}
	event := &pb.Event{Value: value}
	if err := emit(event); err != nil {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "Native output unavailable"))
	}
	buffer := make([]byte, protocol.NativeChunk)
	for {
		if ctx.Err() != nil {
			return protocol.NativeFailure(true, protocol.ContextFailure(ctx))
		}
		n, err := response.Body.Read(buffer)
		if n > 0 {
			value := &pb.Event_Chunk{Chunk: append([]byte(nil), buffer[:n]...)}
			event := &pb.Event{Value: value}
			if err := emit(event); err != nil {
				return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "Native output unavailable"))
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return protocol.NativeFailure(true, nativeExchangeFailure(ctx, "Native HTTP response truncated"))
		}
	}
	if len(response.Trailer) != 0 {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Native HTTP trailers unsupported"))
	}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	return end
}

// nativeHTTPBody joins finite in-memory reads when the HTTP transport
// closes its upload. Its input already belongs to the Native request.
type nativeHTTPBody struct {
	io.Reader
	mu     sync.Mutex
	closed bool
}

func (b *nativeHTTPBody) Read(dst []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, io.ErrClosedPipe
	}
	return b.Reader.Read(dst)
}

func (b *nativeHTTPBody) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.Reader = nil
	return nil
}

func nativeExchangeFailure(ctx context.Context, message string) *pb.Failure {
	if ctx.Err() != nil {
		return protocol.ContextFailure(ctx)
	}
	return protocol.Fail(pb.FailureCode_UNAVAILABLE, message)
}
