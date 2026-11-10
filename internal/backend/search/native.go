package search

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

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

func (a *Adapter) ExecuteNative(ctx context.Context, command *pb.Command, emit execution.Emit) *pb.NativeEnd {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(a.ctx, cancel)
	defer stop()
	if a.ctx.Err() != nil {
		cancel()
	}
	if ctx.Err() != nil {
		return protocol.NativeFailure(false, protocol.ContextFailure(ctx))
	}
	native := command.GetNative()
	if native.Request.ContentType != "application/http" {
		return protocol.NativeFailure(false, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Search Native requires application/http"))
	}
	d, nativeBody, failure := parseNativeHTTPRequest(native.Request.Data)
	if failure != nil {
		return protocol.NativeFailure(false, failure)
	}
	path := d.URL.EscapedPath()
	if native.Resource != "" {
		parts, _ := protocol.ParseRelativeResource(native.Resource)
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
	var phase exchangePhase
	traced := httptrace.WithClientTrace(ctx, phase.trace())
	request, err := http.NewRequestWithContext(traced, d.Method, endpoint, body)
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
		return protocol.NativeFailure(true, nativeExchangeFailure(ctx, "Native HTTP exchange incomplete "+phase.describe(err)))
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

// Phases of a Native HTTP exchange before its response head arrives.
const (
	phaseConnecting int32 = iota
	phaseHandshake
	phaseSending
	phaseWaiting
	phaseReading
)

var exchangePhases = [...]string{
	phaseConnecting: "while connecting",
	phaseHandshake:  "during the TLS handshake",
	phaseSending:    "while sending the request",
	phaseWaiting:    "while waiting for the response head",
	phaseReading:    "while reading the response head",
}

// exchangePhase records how far a Native HTTP exchange got, so that a failure
// before the response head can say where it happened. Only hooks that run in
// exchange order are used: a losing parallel dial can report its connect
// after the winner's TLS handshake has begun, and net/http reports a failed
// request write too.
type exchangePhase struct {
	reached atomic.Int32
}

func (p *exchangePhase) trace() *httptrace.ClientTrace {
	trace := &httptrace.ClientTrace{
		TLSHandshakeStart: func() { p.reached.Store(phaseHandshake) },
		GotConn:           func(httptrace.GotConnInfo) { p.reached.Store(phaseSending) },
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				p.reached.Store(phaseWaiting)
			}
		},
		GotFirstResponseByte: func() { p.reached.Store(phaseReading) },
	}
	return trace
}

// describe names where and how the exchange failed from a fixed vocabulary.
// It never quotes err, which carries the backend URL.
func (p *exchangePhase) describe(err error) string {
	where := exchangePhases[p.reached.Load()]
	var request *url.Error
	if errors.As(err, &request) {
		err = request.Err
	}
	var network net.Error
	switch {
	case errors.Is(err, syscall.ECONNREFUSED):
		return where + " (refused)"
	case errors.Is(err, syscall.ECONNRESET):
		return where + " (reset)"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, syscall.EPIPE):
		return where + " (closed)"
	case errors.As(err, &network) && network.Timeout():
		return where + " (timeout)"
	default:
		return where
	}
}

func nativeExchangeFailure(ctx context.Context, message string) *pb.Failure {
	if ctx.Err() != nil {
		return protocol.ContextFailure(ctx)
	}
	return protocol.Fail(pb.FailureCode_UNAVAILABLE, message)
}
