package searchstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"time"
)

const metadataLimit = 256 << 10
const responseLimit = 1 << 20
const callLimit = 2 * time.Second

var errTransport = errors.New("backend transport failed")
var errResponse = errors.New("invalid or excessive backend response")
var errTimeout = errors.New("backend call deadline")
var errResponseLimit = errors.New("backend response byte limit")

type exchange struct {
	path   string
	body   []byte
	limit  int
	method string
	native bool
}

func (a *Adapter) request(ctx context.Context, call exchange) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, callLimit)
	defer cancel()
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
		return 0, nil, errResponse
	}
	// No replay even on a reused connection: nonempty command bodies, no GetBody
	// or idempotency headers. Record Delete is deliberately a bulk POST too.
	request.GetBody = nil
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/x-ndjson")
	}
	client := a.client
	if call.native {
		client = a.nativeClient
	}
	response, err := client.Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return 0, nil, errTimeout
		}
		var network *net.OpError
		if errors.As(err, &network) {
			return 0, nil, errTransport
		}
		// Protocol/header limits and cancellation are not affirmative congestion.
		return 0, nil, errResponse
	}
	defer response.Body.Close()
	if response.ContentLength > int64(call.limit) {
		return response.StatusCode, nil, errResponseLimit
	}
	if response.Header.Get("Content-Encoding") != "" {
		return response.StatusCode, nil, errResponse
	}
	// Limit before ReadAll/JSON parsing, not after an unbounded materialization.
	raw, err := io.ReadAll(io.LimitReader(response.Body, int64(call.limit)+1))
	if len(raw) > call.limit {
		return response.StatusCode, nil, errResponseLimit
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return response.StatusCode, nil, errTimeout
	}
	if err != nil {
		return response.StatusCode, nil, errResponse
	}
	if err := validateJSON(raw, 16384); err != nil {
		return response.StatusCode, nil, errResponse
	}
	return response.StatusCode, raw, nil
}
func newTransport(pool int) *http.Transport {
	dialer := &net.Dialer{Timeout: callLimit, KeepAlive: 30 * time.Second}
	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	transport := &http.Transport{
		Proxy: nil, DialContext: dialer.DialContext, Protocols: protocols,
		DisableCompression: true, MaxConnsPerHost: pool, MaxIdleConns: pool,
		MaxIdleConnsPerHost: pool, IdleConnTimeout: 30 * time.Second,
		ResponseHeaderTimeout: callLimit, MaxResponseHeaderBytes: 32 << 10,
		TLSHandshakeTimeout: callLimit,
	}
	return transport
}
func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
