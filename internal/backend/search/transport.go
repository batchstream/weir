package search

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"

	"github.com/batchstream/weir/internal/execution"
)

const connectionTimeout = 2 * time.Second
const responseReadBuffer = 4096

var errTransport = errors.New("backend transport failed")
var errResponse = errors.New("invalid or excessive backend response")
var errTimeout = errors.New("backend call deadline")
var errCanceled = errors.New("backend call canceled")
var errWriteNotSent = errors.New("backend write request not sent")
var errWriteMemoryNotSent = errors.New("backend write memory capacity exhausted before send")
var errResponseWindow = errors.New("read response exceeds exchange window")

type exchange struct {
	path          string
	body          []byte
	method        string
	contentType   string
	native        bool
	mutation      bool
	operation     string
	records       int
	responseLimit int
}

type requestContextKey struct{}

func (a *Adapter) request(ctx context.Context, call exchange) (status int, raw []byte, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if a.ctx.Err() != nil {
		if call.mutation {
			return 0, nil, errWriteNotSent
		}
		return 0, nil, errResponse
	}
	stop := context.AfterFunc(a.ctx, cancel)
	defer stop()
	method := http.MethodGet
	var body io.Reader
	if call.body != nil {
		method = http.MethodPost
		body = bytes.NewReader(call.body)
	}
	if call.method != "" {
		method = call.method
	}
	request, err := http.NewRequestWithContext(ctx, method, a.config.URL+call.path, body)
	if err != nil {
		if call.mutation {
			return 0, nil, errWriteNotSent
		}
		return 0, nil, errResponse
	}
	client := a.client
	if call.native {
		client = a.nativeClient
	}
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	_, standardTransport := transport.(*http.Transport)
	var acquiring, connected atomic.Bool
	header := &nativeHeaderMemory{}
	defer header.finish()
	if memory := searchMemory(ctx); memory != nil {
		header.memory = memory.owner
		header.records = memory
		header.base = max(memory.base, memory.frame) + memory.sources
		header.minimum = header.base + memory.reply
	}
	trace := &httptrace.ClientTrace{
		GetConn: func(string) { acquiring.Store(true) },
		GotConn: func(info httptrace.GotConnInfo) {
			connected.Store(true)
			if connection, ok := info.Conn.(*searchConn); ok {
				connection.setHeader(header)
			}
		},
	}
	*request = *request.WithContext(httptrace.WithClientTrace(request.Context(), trace))
	headerKey := nativeHeaderKey{}
	*request = *request.WithContext(context.WithValue(request.Context(), headerKey, header))
	// No replay even on a reused connection: nonempty command bodies, no GetBody
	// or idempotency headers. Record Delete is deliberately a bulk POST too.
	a.configureRequest(request)
	if body != nil {
		request.Body = &nativeHTTPBody{Reader: body}
		defer request.Body.Close()
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/x-ndjson")
		if call.contentType != "" {
			request.Header.Set("Content-Type", call.contentType)
		}
	}
	if call.mutation && ctx.Err() != nil {
		return 0, nil, errWriteNotSent
	}
	a.metrics.initialize()
	started := time.Now()
	defer func() {
		observation := httpObservation{call: call, request: request, duration: time.Since(started), status: status, err: err}
		a.metrics.observeHTTP(observation)
	}()
	if err := reserveSearchScratch(ctx, 64<<10); err != nil {
		if call.mutation {
			return 0, nil, errWriteMemoryNotSent
		}
		return 0, nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		if errors.Is(err, execution.ErrExecutionMemory) {
			return 0, nil, execution.ErrExecutionMemory
		}
		// A positive GetConn without GotConn proves that this HTTP exchange
		// never acquired a connection. Once GotConn runs, any failure remains
		// uncertain, even if no complete request body was observed. Missing
		// trace callbacks or a custom transport are also insufficient evidence.
		if call.mutation && standardTransport && acquiring.Load() && !connected.Load() {
			return 0, nil, errWriteNotSent
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return 0, nil, errTimeout
		}
		if errors.Is(err, context.Canceled) {
			return 0, nil, errCanceled
		}
		var network *net.OpError
		if errors.As(err, &network) {
			return 0, nil, errTransport
		}
		// Protocol/header limits and cancellation are not affirmative congestion.
		return 0, nil, errResponse
	}
	defer func() {
		if response != nil {
			_ = response.Body.Close()
		}
	}()
	if response.Header.Get("Content-Encoding") != "" {
		return response.StatusCode, nil, errResponse
	}
	var reader io.Reader = response.Body
	if call.responseLimit > 0 {
		reader = io.LimitReader(reader, int64(call.responseLimit)+1)
	}
	headerBytes, err := headerWorking(response.Header)
	if err != nil {
		return response.StatusCode, nil, err
	}
	trailerBytes, err := headerWorking(response.Trailer)
	if err != nil {
		return response.StatusCode, nil, err
	}
	if trailerBytes > math.MaxInt-8192 || headerBytes > math.MaxInt-8192-trailerBytes {
		return response.StatusCode, nil, execution.ErrExecutionMemory
	}
	headerBytes += 8192 + trailerBytes
	bodyContext := ctx
	if len(response.TransferEncoding) != 0 {
		// Keep the same guard and its prefetched wire window through EOF.
		// Body capacities and trailer parser work share one absolute budget.
		header.mu.Lock()
		header.trailer = trailerBytes
		header.mu.Unlock()
		bodyContext = request.Context()
	} else {
		header.finish()
	}
	length := response.ContentLength
	if call.responseLimit > 0 && length > int64(call.responseLimit)+1 {
		length = int64(call.responseLimit) + 1
	}
	raw, err = readResponseBody(bodyContext, reader, length, headerBytes)
	if err == nil && call.responseLimit > 0 && len(raw) > call.responseLimit {
		return response.StatusCode, nil, errResponseWindow
	}
	if errors.Is(err, execution.ErrExecutionMemory) {
		return response.StatusCode, nil, execution.ErrExecutionMemory
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return response.StatusCode, nil, errTimeout
	}
	if errors.Is(err, context.Canceled) {
		return response.StatusCode, nil, errCanceled
	}
	if err != nil {
		return response.StatusCode, nil, errResponse
	}
	// The complete body has reached EOF. Release response metadata before its
	// JSON validation allocates; the raw bytes remain covered by this exchange.
	status = response.StatusCode
	_ = response.Body.Close()
	response.Header, response.Trailer = nil, nil
	response, reader = nil, nil
	header.finish()
	if err := reserveSearchScratch(ctx, cap(raw)+jsonDecodeWorking(raw)); err != nil {
		return status, nil, err
	}
	if err := validateJSON(ctx, raw); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return status, nil, errTimeout
		}
		if errors.Is(err, context.Canceled) {
			return status, nil, errCanceled
		}
		return status, nil, errResponse
	}
	return status, raw, nil
}

func headerWorking(header http.Header) (int, error) {
	working := 0
	for name, values := range header {
		if len(name) > math.MaxInt-256-working || cap(values) > (math.MaxInt-256-working-len(name))/16 {
			return 0, execution.ErrExecutionMemory
		}
		working += len(name) + 256 + cap(values)*16
		for _, value := range values {
			if len(value) > math.MaxInt-working {
				return 0, execution.ErrExecutionMemory
			}
			working += len(value)
		}
	}
	return working, nil
}

// Backend authentication supplies defaults; Native may override end-to-end headers.
func (a *Adapter) configureRequest(request *http.Request) {
	// Go 1.27 Transport detaches dial cancellation using WithoutCancel, retaining
	// values. Keep the original request lifetime for our owned DNS/TLS I/O too.
	ctx := request.Context()
	key := requestContextKey{}
	*request = *request.WithContext(context.WithValue(ctx, key, ctx))
	request.GetBody = nil
	if connection := a.config.Connection; connection != nil && connection.Username != "" {
		request.SetBasicAuth(connection.Username, connection.Password)
	}
}
func newTransport(timeout, idleTimeout time.Duration) *http.Transport {
	dialer := &net.Dialer{Timeout: timeout}
	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	transport := &http.Transport{
		Proxy: nil, DialContext: dialer.DialContext, Protocols: protocols,
		DisableCompression: true, MaxConnsPerHost: 0,
		MaxIdleConnsPerHost: math.MaxInt, IdleConnTimeout: idleTimeout,
		ReadBufferSize:         responseReadBuffer,
		MaxResponseHeaderBytes: math.MaxInt64,
		TLSHandshakeTimeout:    timeout,
	}
	return transport
}
func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// Approve both old and new capacities before growing. No bytes are truncated:
// an exhausted exchange fails and closes its response body at the caller.
func readResponseBody(ctx context.Context, reader io.Reader, length int64, headers int) ([]byte, error) {
	capacity := 4096
	if length > 0 {
		if length > int64(math.MaxInt-headers) {
			return nil, execution.ErrExecutionMemory
		}
		capacity = int(length)
	}
	if err := reserveSearchScratch(ctx, headers+capacity); err != nil {
		return nil, err
	}
	raw := make([]byte, 0, capacity)
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if len(raw) == cap(raw) {
			// An exact Content-Length can finish without allocating another body.
			var one [1]byte
			n, err := reader.Read(one[:])
			if err != nil && err != io.EOF {
				return nil, err
			}
			if n == 0 {
				if err == io.EOF {
					return raw, nil
				}
				continue
			}
			if cap(raw) > (math.MaxInt-headers)/3 {
				return nil, execution.ErrExecutionMemory
			}
			nextCapacity := max(4096, 2*cap(raw))
			if err := reserveSearchScratch(ctx, headers+cap(raw)+nextCapacity); err != nil {
				return nil, err
			}
			next := make([]byte, len(raw), nextCapacity)
			copy(next, raw)
			raw = next
			raw = append(raw, one[0])
			if err := reserveSearchScratch(ctx, headers+cap(raw)); err != nil {
				return nil, err
			}
			if err == io.EOF {
				return raw, nil
			}
		}
		n, err := reader.Read(raw[len(raw):cap(raw)])
		raw = raw[:len(raw)+n]
		if err == io.EOF {
			return raw, nil
		}
		if err != nil {
			return nil, err
		}
	}
}
