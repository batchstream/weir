//go:build integration

package app

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil/testsearch"
)

// One-to-one TLS socket relay preserves actual per-adapter connection counts.
// No proxy-side HTTP pool merges the independent executors' connections.
type searchBudgetProxy struct {
	listener      net.Listener
	mu            sync.Mutex
	conns         map[net.Conn]bool
	current, peak int
	closed        bool
	workers       sync.WaitGroup
	observation   *budgetObservation
	upstream      string
	clientTLS     *tls.Config
	drop          chan struct{}
	dropNext      atomic.Bool
	dropped       atomic.Int32
	mutations     atomic.Int32
	applied       atomic.Int32
	reject        atomic.Bool
}

func startSearchBudgetProxy(t *testing.T, f *testsearch.SecureFixture, o *budgetObservation) *searchBudgetProxy {
	t.Helper()
	// These are exclusively this running fixture's generated materials, consumed
	// by the test program; no existing developer certificates are read.
	certificate, err := tls.LoadX509KeyPair(filepath.Join(f.Root, "materials", "server.pem"), filepath.Join(f.Root, "materials", "server.key"))
	if err != nil {
		t.Fatal("owned proxy certificate unavailable")
	}
	config := &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", config)
	if err != nil {
		t.Fatal(err)
	}
	upstream, err := url.Parse(f.Backend.URL)
	if err != nil {
		t.Fatal("fixture address")
	}
	clientTLS := f.Backend.Client.Transport.(*http.Transport).TLSClientConfig.Clone()
	clientTLS.ServerName = upstream.Hostname()
	p := &searchBudgetProxy{listener: listener, conns: make(map[net.Conn]bool), upstream: upstream.Host, clientTLS: clientTLS, observation: o, drop: make(chan struct{})}
	p.workers.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				_ = conn.Close()
				return
			}
			p.conns[conn] = true
			p.mu.Unlock()
			p.workers.Go(func() { p.relay(conn) })
		}
	})
	t.Cleanup(func() {
		_ = listener.Close()
		p.mu.Lock()
		p.closed = true
		for conn := range p.conns {
			_ = conn.Close()
		}
		p.mu.Unlock()
		p.workers.Wait()
	})
	return p
}
func (p *searchBudgetProxy) sockets() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current, p.peak
}
func (p *searchBudgetProxy) relay(client net.Conn) {
	defer func() { _ = client.Close(); p.mu.Lock(); delete(p.conns, client); p.mu.Unlock() }()
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	if err := client.(*tls.Conn).Handshake(); err != nil {
		return
	}
	raw, err := net.DialTimeout("tcp", p.upstream, time.Second)
	if err != nil {
		return
	}
	backend := tls.Client(raw, p.clientTLS)
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = raw.Close()
		return
	}
	p.conns[backend] = true
	p.current++
	p.peak = max(p.peak, p.current)
	p.mu.Unlock()
	defer func() { _ = raw.Close(); p.mu.Lock(); delete(p.conns, backend); p.current--; p.mu.Unlock() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err = backend.HandshakeContext(ctx)
	cancel()
	if err != nil {
		return
	}
	clientReader, backendReader := bufio.NewReader(client), bufio.NewReader(backend)
	for {
		_ = client.SetDeadline(time.Now().Add(5 * time.Second))
		_ = backend.SetDeadline(time.Now().Add(5 * time.Second))
		request, err := http.ReadRequest(clientReader)
		if err != nil {
			return
		}
		body, err := io.ReadAll(io.LimitReader(request.Body, 8<<20))
		_ = request.Body.Close()
		if err != nil {
			return
		}
		request.Body = io.NopCloser(bytes.NewReader(body))
		request.GetBody = nil
		mutation := strings.Contains(request.URL.Path, "_bulk") || strings.Contains(request.URL.Path, "/_update/")
		if mutation {
			p.mutations.Add(1)
		}
		if p.reject.Load() && strings.Contains(request.URL.Path, "/_doc/") {
			_, _ = io.WriteString(client, "HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}")
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		p.observation.begin(ctx)
		cancel()
		if err := request.Write(backend); err != nil {
			p.observation.end()
			return
		}
		response, err := http.ReadResponse(backendReader, request)
		if err != nil {
			p.observation.end()
			return
		}
		responseBody, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
		_ = response.Body.Close()
		p.observation.end()
		if err != nil {
			return
		}
		if mutation && response.StatusCode == 200 && !bytes.Contains(responseBody, []byte(`"errors":true`)) {
			p.applied.Add(1)
		}
		if mutation && p.dropNext.CompareAndSwap(true, false) {
			p.dropped.Add(1)
			select {
			case <-p.drop:
			case <-time.After(time.Second):
			}
			return
		}
		response.Body = io.NopCloser(bytes.NewReader(responseBody))
		if err := response.Write(client); err != nil {
			return
		}
		if request.Close || response.Close {
			return
		}
	}
}
