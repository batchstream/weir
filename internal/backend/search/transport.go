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
)

const connectionTimeout = 2 * time.Second
const responseReadBuffer = 4096
const maxResponseHeaderBytes = 1 << 20
const maxInitialBody = 16 << 20

var errTransport = errors.New("backend transport failed")
var errResponse = errors.New("invalid or excessive backend response")
var errTimeout = errors.New("backend call deadline")
var errCanceled = errors.New("backend call canceled")
var errWriteNotSent = errors.New("backend write request not sent")
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
	trace := &httptrace.ClientTrace{
		GetConn: func(string) { acquiring.Store(true) },
		GotConn: func(httptrace.GotConnInfo) { connected.Store(true) },
	}
	*request = *request.WithContext(httptrace.WithClientTrace(request.Context(), trace))
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
	response, err := client.Do(request)
	if err != nil {
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
	defer response.Body.Close()
	if response.Header.Get("Content-Encoding") != "" {
		return response.StatusCode, nil, errResponse
	}
	var reader io.Reader = response.Body
	if call.responseLimit > 0 {
		reader = io.LimitReader(reader, int64(call.responseLimit)+1)
	}
	length := response.ContentLength
	if call.responseLimit > 0 && length > int64(call.responseLimit)+1 {
		length = int64(call.responseLimit) + 1
	}
	raw, err = readResponseBody(reader, length)
	if err == nil && call.responseLimit > 0 && len(raw) > call.responseLimit {
		return response.StatusCode, nil, errResponseWindow
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
	status = response.StatusCode
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
		MaxResponseHeaderBytes: maxResponseHeaderBytes,
		TLSHandshakeTimeout:    timeout,
	}
	return transport
}
func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// readResponseBody reads a complete body, sizing the buffer from a declared
// length without trusting it for more than one initial allocation.
func readResponseBody(reader io.Reader, length int64) ([]byte, error) {
	capacity := 4096
	if length > 0 {
		capacity = int(min(length, maxInitialBody))
	}
	raw := make([]byte, 0, capacity)
	for {
		n, err := reader.Read(raw[len(raw):cap(raw)])
		raw = raw[:len(raw)+n]
		if err == io.EOF {
			return raw, nil
		}
		if err != nil {
			return nil, err
		}
		if len(raw) == cap(raw) {
			raw = append(raw, 0)[:len(raw)]
		}
	}
}
