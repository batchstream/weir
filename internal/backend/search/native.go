package search

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"google.golang.org/protobuf/proto"
)

const NativeBodyLimit = 8 << 20

const NativeResponseLimit = 8 << 20
const NativeItemLimit = 256 << 10

const nativeMetadataLine = 4 << 10
const nativeBudget = 4 << 20

type nativePlan struct {
	index   string
	request *http.Request
	body    []byte
}

func (a *Adapter) prepareNative(request *pb.NativeRequest) (*execution.Plan, *pb.Failure) {
	if f := protocol.ValidateNative(request); f != nil {
		return nil, f
	}
	parts, _ := protocol.ParseRelativeResource(request.Resource)
	if len(parts) != 1 || !validIndex(parts[0]) {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "Native requires one concrete Search index")
	}
	if request.Request.ContentType != "application/http" {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Search Native requires application/http")
	}
	httpRequest, body, failure := parseNativeHTTPRequest(request.Request.Data)
	if failure != nil {
		return nil, failure
	}

	native := &nativePlan{index: parts[0], request: httpRequest, body: body}
	p := &execution.Plan{
		Key:          request.Resource,
		Bytes:        proto.Size(request) + execution.EntryOverheadBytes,
		ResultBytes:  protocol.NativeChunk + protocol.MaxNativeMetadataBytes + execution.ResultOverheadBytes,
		WorkingBytes: nativeBudget,
		Backend:      native,
	}
	return p, nil
}

// Inspect the bounded header span before net/http can allocate header entries.
// The body remains a borrowed slice of the admitted Native request.
func parseNativeHTTPRequest(raw []byte) (*http.Request, []byte, *pb.Failure) {
	invalid := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid Native HTTP framing")
	if len(raw) > protocol.MaxNativeRequestBytes {
		return nil, nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Native HTTP request bound")
	}
	end := bytes.Index(raw[:min(len(raw), protocol.MaxNativeMetadataBytes)], []byte("\r\n\r\n"))
	if end < 0 || end+4 > protocol.MaxNativeMetadataBytes {
		return nil, nil, invalid
	}
	head := raw[:end+4]
	remaining := head[:len(head)-2]
	lengths, lines := 0, 0
	for len(remaining) != 0 {
		line, rest, _ := bytes.Cut(remaining, []byte("\r\n"))
		remaining = rest
		lines++
		// Three option headers with up to eight values each, plus Host and length.
		if lines > 27 || bytes.ContainsAny(line, "\r\n") || lines > 1 && len(line) > 0 && (line[0] == ' ' || line[0] == '\t') {
			return nil, nil, invalid
		}
		if lines == 1 || len(line) == 0 {
			continue
		}
		colon := bytes.IndexByte(line, ':')
		if colon < 1 {
			return nil, nil, invalid
		}
		name := string(line[:colon])
		if strings.EqualFold(name, "Transfer-Encoding") || strings.EqualFold(name, "Trailer") || strings.EqualFold(name, "Expect") {
			return nil, nil, invalid
		}
		if strings.EqualFold(name, "Content-Length") {
			lengths++
			if lengths > 1 {
				return nil, nil, invalid
			}
		}
	}
	request, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(head)))
	if err != nil {
		return nil, nil, invalid
	}
	_ = request.Body.Close()
	request.Body = http.NoBody
	body := raw[end+4:]
	if request.Proto != "HTTP/1.1" || request.URL.IsAbs() || request.URL.Host != "" || request.URL.Opaque != "" ||
		!strings.HasPrefix(request.RequestURI, "/") || strings.HasPrefix(request.RequestURI, "//") ||
		len(request.TransferEncoding) != 0 || len(request.Trailer) != 0 || request.ContentLength != int64(len(body)) ||
		lengths == 0 && len(body) != 0 {
		return nil, nil, invalid
	}
	if failure := validateNativeHTTPRequest(request); failure != nil {
		return nil, nil, failure
	}
	return request, body, nil
}

func validateNativeHTTPRequest(d *http.Request) *pb.Failure {
	unsupported := protocol.Fail(pb.FailureCode_UNSUPPORTED, "unsupported Native HTTP operation or option")
	if d.URL == nil || d.URL.EscapedPath() != d.URL.Path {
		return unsupported
	}
	path := d.URL.Path
	switch {
	case d.Method == "POST" && path == "/_bulk":
		if d.Header.Get("Content-Type") != "application/x-ndjson" {
			return unsupported
		}
	case d.Method == "GET" && strings.HasPrefix(path, "/_doc/"):
		id := strings.TrimPrefix(path, "/_doc/")
		if len(id) == 0 || len(id) > 512 || id == "." || id == ".." || d.Header.Get("Content-Type") != "" || d.ContentLength != 0 {
			return unsupported
		}
		for _, b := range []byte(id) {
			if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || strings.ContainsRune("-._~", rune(b))) {
				return unsupported
			}
		}
	default:
		return unsupported
	}
	query, err := url.ParseQuery(d.URL.RawQuery)
	if err != nil || query.Encode() != d.URL.RawQuery {
		return unsupported
	}
	for key, values := range query {
		if len(values) != 1 {
			return unsupported
		}
		switch key {
		case "refresh":
			if path != "/_bulk" || values[0] != "true" && values[0] != "false" && values[0] != "wait_for" {
				return unsupported
			}
		case "realtime":
			if d.Method != "GET" || values[0] != "true" && values[0] != "false" {
				return unsupported
			}
		default:
			return unsupported
		}
	}
	for name, values := range d.Header {
		if len(values) == 0 || len(values) > 8 {
			return unsupported
		}
		switch strings.ToLower(name) {
		case "content-length":
			continue
		case "accept", "content-type", "x-opaque-id":
		default:
			return unsupported
		}
		for _, value := range values {
			if len(value) > 128 || !utf8.ValidString(value) {
				return unsupported
			}
			for _, c := range value {
				if c < 32 || c == 127 {
					return unsupported
				}
			}
			if strings.EqualFold(name, "accept") && value != "application/json" || strings.EqualFold(name, "content-type") && value != "application/x-ndjson" {
				return unsupported
			}
		}
	}
	return nil
}

// Materialize only one bounded item. Validation happens before any byte of that
// item can reach the backend; the original metadata/source bytes are unchanged.
type nativeBulkReader struct {
	reader       *bufio.Reader
	index        string
	current      []byte
	total, items int
}

var errNativeInput = errors.New("invalid or excessive Native bulk input")

func (r *nativeBulkReader) line(limit int) ([]byte, error) {
	line := make([]byte, 0, min(limit, 4096))
	for {
		fragment, err := r.reader.ReadSlice('\n')
		if len(line)+len(fragment) > limit || r.total+len(fragment) > NativeBodyLimit {
			return nil, errNativeInput
		}
		r.total += len(fragment)
		line = append(line, fragment...)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil {
			if err == io.EOF && len(line) > 0 {
				return nil, errNativeInput
			}
			return nil, err
		}
		return line, nil
	}
}

func (r *nativeBulkReader) item() ([]byte, error) {
	metadata, err := r.line(nativeMetadataLine)
	if err != nil {
		return nil, err
	}
	r.items++
	if r.items > 4096 || validateJSON(metadata, 64) != nil {
		return nil, errNativeInput
	}
	var action map[string]map[string]json.RawMessage
	if json.Unmarshal(metadata, &action) != nil || len(action) != 1 {
		return nil, errNativeInput
	}
	var operation string
	for key, fields := range action {
		operation = key
		if key != "index" && key != "create" && key != "delete" {
			return nil, errNativeInput
		}
		var id string
		if json.Unmarshal(fields["_id"], &id) != nil || len(id) == 0 || len(id) > 512 {
			return nil, errNativeInput
		}
		for key, raw := range fields {
			if key == "_id" {
				continue
			}
			var index string
			if key != "_index" || json.Unmarshal(raw, &index) != nil || index != r.index {
				return nil, errNativeInput
			}
		}
	}
	if operation == "delete" {
		return metadata, nil
	}
	source, err := r.line(NativeItemLimit - len(metadata))
	if err != nil {
		return nil, errNativeInput
	}
	if !object(source) || validateJSON(source, 4096) != nil {
		return nil, errNativeInput
	}
	return append(metadata, source...), nil
}

func (r *nativeBulkReader) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if len(r.current) == 0 {
		item, err := r.item()
		if err != nil {
			return 0, err
		}
		r.current = item
	}
	n := copy(dst, r.current)
	r.current = r.current[n:]
	return n, nil
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
	d := native.request
	input := native.body
	var body io.Reader
	caps, failure := a.inspect(ctx, native.index, true)
	if failure != nil {
		if ctx.Err() != nil {
			failure = protocol.ContextFailure(ctx)
		}
		return protocol.NativeFailure(false, failure)
	}
	if d.Method != "GET" {
		if !caps.nativeWrite {
			return protocol.NativeFailure(false, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Native bulk requires no default/final ingest pipeline"))
		}
		reader := &nativeBulkReader{reader: bufio.NewReaderSize(bytes.NewReader(input), 4096), index: native.index}
		first, err := reader.item()
		if err != nil {
			return protocol.NativeFailure(false, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid first Native bulk item"))
		}
		reader.current = first
		body = reader
	}
	if ctx.Err() != nil {
		return protocol.NativeFailure(false, protocol.ContextFailure(ctx))
	}
	endpoint := a.config.URL + "/" + url.PathEscape(native.index) + d.URL.Path
	if d.URL.RawQuery != "" {
		endpoint += "?" + d.URL.RawQuery
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
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	if body != nil && request.Header.Get("Content-Type") == "" {
		request.Header.Set("Content-Type", "application/x-ndjson")
	}
	// Closing the body terminates this bounded exchange on cancellation. net/http
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
	if response.ContentLength > NativeResponseLimit ||
		response.Header.Get("Content-Encoding") != "" ||
		len(response.Trailer) != 0 ||
		response.StatusCode == http.StatusSwitchingProtocols {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Native HTTP response bounds"))
	}
	var metadata bytes.Buffer
	fmt.Fprintf(&metadata, "HTTP/1.1 %d %s\r\n", response.StatusCode, http.StatusText(response.StatusCode))
	names := make([]string, 0, len(response.Header))
	for name := range response.Header {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		switch strings.ToLower(name) {
		case "content-type", "content-length", "warning", "x-opaque-id", "x-elastic-product", "location", "retry-after", "etag":
			for _, value := range response.Header.Values(name) {
				if metadata.Len()+len(name)+len(value)+6 > protocol.MaxNativeMetadataBytes {
					return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Native metadata bound"))
				}
				fmt.Fprintf(&metadata, "%s: %s\r\n", name, value)
			}
		}
	}
	metadata.WriteString("\r\n")
	contentType, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
	document := &pb.Document{ContentType: "application/http", Data: metadata.Bytes()}
	head := &pb.NativeHead{Metadata: document, BodyContentType: contentType}
	value := &pb.Event_Head{Head: head}
	event := &pb.Event{Value: value}
	if err := emit(p, event); err != nil {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "Native output unavailable"))
	}
	buffer := make([]byte, protocol.NativeChunk)
	total := 0
	for {
		if ctx.Err() != nil {
			return protocol.NativeFailure(true, protocol.ContextFailure(ctx))
		}
		n, err := response.Body.Read(buffer)
		total += n
		if total > NativeResponseLimit {
			return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Native response limit"))
		}
		if n > 0 {
			value := &pb.Event_Chunk{Chunk: append([]byte(nil), buffer[:n]...)}
			event := &pb.Event{Value: value}
			if err := emit(p, event); err != nil {
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

// nativeHTTPBody joins finite in-memory validation reads when the HTTP transport
// closes its upload. Its input already belongs to the bounded Native request.
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
