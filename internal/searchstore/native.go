package searchstore

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	spb "github.com/batchstream/weir/api/weir/search/v1"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"google.golang.org/protobuf/proto"
)

const NativeDescriptor = "application/vnd.weir.search-http.v1+protobuf"
const NativeBodyLimit = 8 << 20
const NativeResponseLimit = 8 << 20
const NativeItemLimit = 256 << 10
const nativeMetadataLine = 4 << 10
const nativeBudget = 4 << 20

func (a *Adapter) PrepareNative(open *pb.NativeOpen) (*execution.Plan, *pb.Failure) {
	if f := protocol.ValidateNative(open, a.config.Store); f != nil {
		return nil, f
	}
	_, parts, _ := protocol.ParseResource(open.Resource)
	if len(parts) != 1 || parts[0] != a.config.Index {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Native requires the configured concrete index")
	}
	if open.Descriptor_.MediaType != NativeDescriptor {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "unsupported Native descriptor")
	}
	descriptor := &spb.Request{}
	if err := proto.Unmarshal(open.Descriptor_.Data, descriptor); err != nil || len(descriptor.ProtoReflect().GetUnknown()) != 0 {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid HTTP descriptor")
	}
	if f := nativeDescriptor(descriptor, open.BodyMediaType); f != nil {
		return nil, f
	}
	p := &execution.Plan{Native: true, Key: open.Resource, Token: "native", Bytes: proto.Size(open) + protocol.EntryOverhead, ResultBytes: protocol.NativeChunk + protocol.NativeDescriptor + protocol.ResultOverhead, PageBytes: nativeBudget, Backend: descriptor}
	return p, nil
}

func nativeDescriptor(d *spb.Request, media string) *pb.Failure {
	unsupported := protocol.Fail(pb.FailureCode_UNSUPPORTED, "unsupported Native HTTP operation or option")
	switch {
	case d.Method == "POST" && d.Path == "/_bulk":
		if media != "application/x-ndjson" {
			return unsupported
		}
	case d.Method == "GET" && strings.HasPrefix(d.Path, "/_doc/"):
		id := strings.TrimPrefix(d.Path, "/_doc/")
		// Deliberately allow only unreserved IDs. No double decoding, encoded slash,
		// dot traversal or aliases; callers use another supported profile for more.
		if len(id) == 0 || len(id) > 512 || id == "." || id == ".." || media != "" {
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
	query, err := url.ParseQuery(d.Query)
	if err != nil || query.Encode() != d.Query {
		return unsupported
	}
	for key, values := range query {
		if len(values) != 1 {
			return unsupported
		}
		switch key {
		case "refresh":
			if d.Path != "/_bulk" || values[0] != "true" && values[0] != "false" && values[0] != "wait_for" {
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
	for _, h := range d.Headers {
		if h == nil || len(h.ProtoReflect().GetUnknown()) != 0 || len(h.Values) == 0 || len(h.Values) > 8 {
			return unsupported
		}
		switch h.Name {
		case "accept", "content-type", "x-opaque-id":
		default:
			return unsupported
		}
		for _, value := range h.Values {
			if len(value) > 128 || !utf8.ValidString(value) {
				return unsupported
			}
			for _, c := range value {
				if c < 32 || c == 127 {
					return unsupported
				}
			}
			if h.Name == "accept" && value != "application/json" || h.Name == "content-type" && value != media {
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

func (a *Adapter) ExecuteNative(ctx context.Context, p *execution.Plan, exchange *execution.NativeExchange) (*pb.NativeEnd, execution.Feedback) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if a.ctx != nil {
		stop := context.AfterFunc(a.ctx, cancel)
		defer stop()
	}
	d := p.Backend.(*spb.Request)
	var body io.Reader
	if d.Method == "GET" {
		var byte [1]byte
		n, err := exchange.Source.Read(byte[:])
		if n != 0 || err != io.EOF {
			return protocol.NativeFailure(false, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "GET requires an empty half-closed upload")), execution.Neutral
		}
	} else {
		caps, failure, _ := a.inspect(ctx, true)
		if failure != nil {
			return protocol.NativeFailure(false, failure), execution.Neutral
		}
		if !caps.nativeWrite {
			return protocol.NativeFailure(false, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Native bulk requires no default/final ingest pipeline")), execution.Neutral
		}
		reader := &nativeBulkReader{reader: bufio.NewReaderSize(exchange.Source, 4096), index: a.config.Index}
		first, err := reader.item()
		if err != nil {
			return protocol.NativeFailure(false, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid first Native bulk item")), execution.Neutral
		}
		reader.current = first
		body = reader
	}
	if ctx.Err() != nil {
		return protocol.NativeFailure(false, protocol.ContextFailure(ctx)), execution.Neutral
	}
	endpoint := a.config.URL + "/" + a.config.Index + d.Path
	if d.Query != "" {
		endpoint += "?" + d.Query
	}
	request, err := http.NewRequestWithContext(ctx, d.Method, endpoint, body)
	if err != nil {
		return protocol.NativeFailure(false, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid HTTP request")), execution.Neutral
	}
	request.GetBody = nil

	for _, header := range d.Headers {
		for _, value := range header.Values {
			request.Header.Add(header.Name, value)
		}
	}
	if body != nil && request.Header.Get("Content-Type") == "" {
		request.Header.Set("Content-Type", "application/x-ndjson")
	}
	// Closing this body must interrupt an upload blocked in gRPC Recv. net/http
	// has at most one writer/read loop for this one non-reused HTTP/1 connection.
	if body != nil {
		request.Body = &nativeHTTPBody{Reader: body, source: exchange.Source}
		defer request.Body.Close()
	}
	response, err := a.nativeClient.Do(request)
	if err != nil {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "Native HTTP exchange incomplete")), execution.Neutral
	}
	defer response.Body.Close()
	// Preserve a complete early backend reply before stopping input. A local
	// upload cancellation is not allowed to replace a known complete native error.
	defer exchange.Source.Close()
	if response.ContentLength > NativeResponseLimit || response.Header.Get("Content-Encoding") != "" || len(response.Trailer) != 0 || response.StatusCode == http.StatusSwitchingProtocols {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Native HTTP response bounds")), execution.Neutral
	}
	metadata := &spb.Response{StatusCode: uint32(response.StatusCode)}
	names := make([]string, 0, len(response.Header))
	for name := range response.Header {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		switch strings.ToLower(name) {
		case "content-type", "content-length", "warning", "x-opaque-id", "x-elastic-product", "location", "retry-after", "etag":
			h := &spb.Header{Name: strings.ToLower(name), Values: response.Header.Values(name)}
			metadata.Headers = append(metadata.Headers, h)
		}
	}
	encoded, err := proto.Marshal(metadata)
	if err != nil || len(encoded) > protocol.NativeDescriptor {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Native metadata bound")), execution.Neutral
	}
	document := &pb.Document{MediaType: NativeDescriptor, Data: encoded}
	media, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
	head := &pb.NativeHead{Metadata: document, BodyMediaType: media}
	if err := exchange.Sink.Head(head); err != nil {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "Native output unavailable")), execution.Neutral
	}
	buffer := make([]byte, protocol.NativeChunk)
	total := 0
	for {
		n, err := response.Body.Read(buffer)
		total += n
		if total > NativeResponseLimit {
			return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Native response limit")), execution.Neutral
		}
		if n > 0 {
			if err := exchange.Sink.Chunk(buffer[:n]); err != nil {
				return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "Native output unavailable")), execution.Neutral
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "Native HTTP response truncated")), execution.Neutral
		}
	}
	if len(response.Trailer) != 0 {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Native HTTP trailers unsupported")), execution.Neutral
	}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	return end, execution.Neutral
}

// Close joins any in-progress source read before the exchange releases its
// execution permit. It can also be called by net/http after normal upload EOF.
type nativeHTTPBody struct {
	io.Reader
	source  io.Closer
	mu      sync.Mutex
	closed  bool
	reading chan struct{}
}

func (b *nativeHTTPBody) Read(dst []byte) (int, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	done := make(chan struct{})
	b.reading = done
	b.mu.Unlock()
	n, err := b.Reader.Read(dst)
	b.mu.Lock()
	b.reading = nil
	close(done)
	b.mu.Unlock()
	return n, err
}
func (b *nativeHTTPBody) Close() error {
	b.mu.Lock()
	b.closed = true
	reading := b.reading
	b.mu.Unlock()
	err := b.source.Close()
	if reading != nil {
		<-reading
	}
	return err
}
