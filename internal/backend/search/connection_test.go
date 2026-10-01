package search

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	spb "github.com/batchstream/weir/api/weir/search/v1"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testdns"
)

func TestSearchConnectionValidation(t *testing.T) {
	good := []string{"http://10.0.0.1:9200", "https://Search.Example:443", "https://[::1]:9200", "http://127.0.0.1:9200", "https://192.168.1.2:443"}
	for _, endpoint := range good {
		cfg := Config{Store: "search", Pool: 4, URL: endpoint}
		if err := ValidateConfig(cfg); err != nil {
			t.Fatal("valid endpoint rejected", err)
		}
	}
	bad := []string{"http://host", "http://host:", "http://host:0", "http://host:65536", "http://host:+80", "https://a:b@host:443", "http://host:80/", "http://host:80?", "http://host:80#", "http://host:80/path", "http://host:80?q=v", "http://host:80#f", "http://%68ost:80", "http://[host]:80", "http://[fe80::1%25en0]:80", "http://127.1:80", "http://0127.0.0.1:80", "http://0.0.0.0:80", "http://[::]:80", "http://224.1.2.3:80", "http://host.:80", "http://.host:80", "http://bad_host:80", "http://-host:80", "http://host-:80", "http://a..b:80", "http://é.example:80", "http://host:80\\evil", "ftp://host:80", "https:host:443", strings.Repeat("a", 1025)}
	for _, endpoint := range bad {
		cfg := Config{Store: "search", Pool: 4, URL: endpoint}
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("invalid endpoint accepted")
		}
	}
	for _, mode := range []string{"http-auth", "missing-user", "missing-password", "long-user", "long-pass", "colon", "control", "ca-http", "long-ca", "empty"} {
		t.Run(mode, func(t *testing.T) {
			c := &Connection{Username: "user", Password: "password-sentinel", CAFile: "/missing/ca-sentinel.pem"}
			cfg := Config{Store: "search", Pool: 4, URL: "https://unresolved.invalid:443", Connection: c}
			if err := ValidateConfig(cfg); err != nil {
				t.Fatal("preflight accessed CA or DNS", err)
			}
			switch mode {
			case "http-auth":
				cfg.URL = "http://host:80"
			case "missing-user":
				c.Username = ""
			case "missing-password":
				c.Password = ""
			case "long-user":
				c.Username = strings.Repeat("u", 129)
			case "long-pass":
				c.Password = strings.Repeat("p", 257)
			case "colon":
				c.Username = "bad:name"
			case "control":
				c.Password = "bad\r\nvalue"
			case "ca-http":
				cfg.URL = "http://host:80"
				c.Username = ""
				c.Password = ""
			case "long-ca":
				c.CAFile = strings.Repeat("c", 2049)
			case "empty":
				c.Username = ""
				c.Password = ""
				c.CAFile = ""
			}
			a, err := Open(context.Background(), cfg)
			if a != nil || err == nil || strings.Contains(err.Error(), "sentinel") {
				t.Fatal("validation or redaction failed")
			}
		})
	}
}

// All key material is generated and consumed by the test. Agent tools never
// inspect certificate files; TempDir removes this test's own material.
func tlsEndpoint(t *testing.T, handler http.Handler, expired bool) (*httptest.Server, *Connection) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal("test key generation")
	}
	subject := pkix.Name{CommonName: "Search local test"}
	expiry := time.Now().Add(time.Hour)
	if expired {
		expiry = time.Now().Add(-time.Minute)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: subject, NotBefore: time.Now().Add(-time.Hour), NotAfter: expiry, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, DNSNames: []string{"search.test"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal("test certificate generation")
	}
	pair := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{pair}, NextProtos: []string{"h2", "http/1.1"}}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = tlsConfig
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	t.Cleanup(server.Close)
	block := &pem.Block{Type: "CERTIFICATE", Bytes: der}
	name := filepath.Join(t.TempDir(), "generated-ca.pem")
	if err := os.WriteFile(name, pem.EncodeToMemory(block), 0600); err != nil {
		t.Fatal("test CA creation")
	}
	c := &Connection{CAFile: name, Username: "test-app", Password: "test-password"}
	return server, c
}
func qualification(w http.ResponseWriter, r *http.Request) bool {
	switch r.URL.Path {
	case "/":
		io.WriteString(w, `{"version":{"number":"8.19.22","build_flavor":"default"}}`)
	case "/_cluster/settings":
		io.WriteString(w, `{"defaults":{"action.auto_create_index":"false"}}`)
	case "/records":
		io.WriteString(w, `{"records":{"settings":{"index.uuid":"test","index.number_of_shards":"1"},"mappings":{}}}`)
	default:
		return false
	}
	return true
}
func openTestTLS(t *testing.T, endpoint string, connection *Connection) *Adapter {
	t.Helper()
	cfg := Config{Store: "search", Pool: 2, URL: endpoint, Connection: connection}
	a, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}
func TestSearchTLSBothPathsAndTransportPolicy(t *testing.T) {
	var calls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "test-app" || pass != "test-password" || r.ProtoMajor != 1 || r.TLS == nil || r.Header.Get("Accept-Encoding") != "" {
			t.Error("connection policy mismatch")
		}
		if !qualification(w, r) {
			calls.Add(1)
			io.WriteString(w, `{}`)
		}
	})
	endpoint, c := tlsEndpoint(t, handler, false)
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	a := openTestTLS(t, endpoint.URL, c)
	call := exchange{path: "/probe", body: []byte(`{}`), limit: 256}
	if _, _, err := a.request(context.Background(), call); err != nil {
		t.Fatal(err)
	}
	open := nativeOpen(t, "records", "GET", "/_doc/x")
	end, _ := runNative(t, a, open, io.NopCloser(strings.NewReader("")))
	if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || calls.Load() != 2 {
		t.Fatal("native connection config missing")
	}
	for _, transport := range []*http.Transport{a.transport, a.nativeTransport} {
		if transport.Proxy != nil || !transport.DisableCompression || transport.Protocols.HTTP2() || !transport.Protocols.HTTP1() {
			t.Fatal("unsafe transport defaults")
		}
	}
	if !a.nativeTransport.DisableKeepAlives || a.nativeTransport.MaxConnsPerHost != 1 || a.transport.MaxConnsPerHost != 2 {
		t.Fatal("pool policy")
	}
	for _, name := range []string{"authorization", "proxy-authorization", "host", "idempotency-key", "x-idempotency-key"} {
		header := &spb.Header{Name: name, Values: []string{"forbidden"}}
		d := &spb.Request{Method: "GET", Path: "/_doc/x", Headers: []*spb.Header{header}}
		if nativeDescriptor(d, "") == nil {
			t.Fatal("caller overrode connection/replay headers")
		}
	}
}
func TestSearchTLSValidationAndCAErrors(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { qualification(w, r) })
	endpoint, c := tlsEndpoint(t, handler, false)
	_, other := tlsEndpoint(t, handler, false)
	expired, expiredC := tlsEndpoint(t, handler, true)
	for _, mode := range []string{"wrong-ca", "expired", "missing-ca", "oversize-ca", "malformed-ca", "directory-ca", "system-roots"} {
		t.Run(mode, func(t *testing.T) {
			connection := *c
			cfg := Config{Store: "search", Pool: 1, URL: endpoint.URL, Connection: &connection}
			switch mode {
			case "wrong-ca":
				connection.CAFile = other.CAFile
			case "expired":
				cfg.URL = expired.URL
				cfg.Connection = expiredC
			case "missing-ca":
				connection.CAFile = "/missing/secret-sentinel.pem"
			case "directory-ca":
				connection.CAFile = t.TempDir()
			case "system-roots":
				connection.CAFile = ""
			default:
				connection.CAFile = filepath.Join(t.TempDir(), "invalid-generated-ca")
				data := []byte("invalid-sentinel")
				if mode == "oversize-ca" {
					data = make([]byte, (256<<10)+1)
				}
				if err := os.WriteFile(connection.CAFile, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			a, err := Open(context.Background(), cfg)
			if a != nil || err == nil || strings.Contains(err.Error(), "sentinel") {
				t.Fatal("TLS/CA failure missing or exposed value")
			}
		})
	}
}
func TestSearchDNSNameSNIAddressChangeAndBounds(t *testing.T) {
	dns := testdns.Start(t)
	answer := testdns.Answer{Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}
	dns.Set("search.test", answer)
	var names atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS.ServerName == "search.test" {
			names.Add(1)
		} else {
			t.Error("DNS name lost in SNI")
		}
		if !qualification(w, r) {
			io.WriteString(w, `{}`)
		}
	})
	endpoint, c := tlsEndpoint(t, handler, false)
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(endpoint.URL, "https://"))
	cfg := Config{Store: "search", Pool: 1, URL: "https://SEARCH.test:" + port, Connection: c, Resolver: dns.Resolver()}
	a, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if names.Load() != 2 {
		t.Fatal("qualification SNI")
	}
	unreachable := testdns.Answer{Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.2")}}
	dns.Set("search.test", unreachable)
	call := exchange{path: "/probe", limit: 128}
	if _, _, err := a.request(context.Background(), call); err != nil {
		t.Fatal("existing connection migrated on DNS change")
	}
	a.transport.CloseIdleConnections()
	if _, _, err := a.request(context.Background(), call); err == nil {
		t.Fatal("new connection ignored changed address")
	}
	dns.Set("search.test", answer)
	if _, _, err := a.request(context.Background(), call); err != nil {
		t.Fatal("new independent request did not recover")
	}
	a.transport.CloseIdleConnections()
	wrong := cfg
	wrong.URL = "https://wrong.test:" + port
	dns.Set("wrong.test", answer)
	rejected, err := Open(context.Background(), wrong)
	if err == nil || rejected != nil {
		t.Fatal("hostname mismatch accepted")
	}
	for _, mode := range []string{"too-many", "excessive-message", "timeout", "nxdomain"} {
		t.Run(mode, func(t *testing.T) {
			bad := testdns.Answer{}
			switch mode {
			case "too-many", "excessive-message":
				n := 9
				if mode == "excessive-message" {
					n = 1000
					bad.TCP = true
				}
				for i := 0; i < n; i++ {
					bad.Addresses = append(bad.Addresses, netip.MustParseAddr("127.0.0.1"))
				}
			case "timeout":
				bad.Drop = true
			}
			dns.Set("search.test", bad)
			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
			defer cancel()
			before := time.Now()
			if _, _, err := a.request(ctx, call); err == nil {
				t.Fatal("DNS bound accepted")
			}
			if time.Since(before) > time.Second {
				t.Fatal("DNS cancellation delayed")
			}
			// net/http detaches dial cancellation; Close must synchronously join it.
			before = time.Now()
			_ = a.Close()
			if time.Since(before) > time.Second || len(a.dialer.slots) != 0 {
				t.Fatal("DNS Close leaked lookup")
			}
			dns.Set("search.test", answer)
			a, err = Open(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			a.transport.CloseIdleConnections()
		})
	}
	_ = a.Close()
	t.Logf("DNS ordinary A/AAAA=%d/%d TCP=%d; hostname/SNI retained; new calls re-resolve; Close joins I/O", dns.A.Load(), dns.AAAA.Load(), dns.TCP.Load())
}

func TestSearchTLSHandshakePoolCloseBound(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var mu sync.Mutex
	var sockets []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			sockets = append(sockets, c)
			mu.Unlock()
		}
	}()
	defer func() {
		_ = listener.Close()
		<-done
		mu.Lock()
		defer mu.Unlock()
		for _, c := range sockets {
			_ = c.Close()
		}
	}()
	before := runtime.NumGoroutine()
	for round := 0; round < 8; round++ {
		ctx, cancel := context.WithCancel(context.Background())
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		d := &connectionDialer{ctx: ctx, tlsConfig: tlsConfig, slots: make(chan struct{}, 3), conns: make(map[*searchConn]struct{})}
		var workers sync.WaitGroup
		for i := 0; i < 16; i++ {
			workers.Go(func() {
				conn, err := d.dial(context.Background(), "tcp", listener.Addr().String())
				if conn != nil {
					_ = conn.Close()
				}
				if err == nil {
					t.Error("silent peer completed handshake")
				}
			})
		}
		until := time.Now().Add(time.Second)
		for len(d.slots) < 3 && time.Now().Before(until) {
			time.Sleep(time.Millisecond)
		}
		if len(d.slots) > 3 {
			t.Fatal("handshake capacity exceeded")
		}
		start := time.Now()
		cancel()
		d.close()
		workers.Wait()
		if time.Since(start) > time.Second || len(d.slots) != 0 {
			t.Fatal("Close did not join handshake and release slots")
		}
		d.close()
	}
	ctx, cancel := context.WithCancel(context.Background())
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	d := &connectionDialer{ctx: ctx, tlsConfig: config, slots: make(chan struct{}, 1), conns: make(map[*searchConn]struct{})}
	start := time.Now()
	conn, dialErr := d.dial(context.Background(), "tcp", listener.Addr().String())
	elapsed := time.Since(start)
	cancel()
	d.close()
	if conn != nil || dialErr == nil || elapsed < 1500*time.Millisecond || elapsed > 3*time.Second || len(d.slots) != 0 {
		t.Fatal("hard handshake deadline", elapsed)
	}
	t.Logf("silent TLS peer with no caller deadline: handshake terminated in %s", elapsed)
	time.Sleep(50 * time.Millisecond)
	if runtime.NumGoroutine() > before+6 {
		t.Fatal("repeated canceled handshake goroutine growth")
	}
	t.Logf("8 rounds x 16 attempts: max 3 concurrent handshakes; Close <1s; goroutines before=%d after=%d", before, runtime.NumGoroutine())
}

func TestSearchTLSNoRedirectCredentialLeak(t *testing.T) {
	var leaked atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked.Add(1) }))
	defer target.Close()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if qualification(w, r) {
			return
		}
		w.Header().Set("Location", target.URL)
		w.WriteHeader(307)
	})
	endpoint, c := tlsEndpoint(t, handler, false)
	a := openTestTLS(t, endpoint.URL, c)
	call := exchange{path: "/redirect", body: []byte(`{}`), limit: 256}
	_, _, _ = a.request(context.Background(), call)
	open := nativeOpen(t, "records", "GET", "/_doc/x")
	end, capture := runNative(t, a, open, io.NopCloser(strings.NewReader("")))
	if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || capture.head == nil || leaked.Load() != 0 {
		t.Fatal("redirect followed")
	}
}

func TestSearchTLSWithoutCredentialsAndCertificateCount(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("implicit credentials")
		}
		qualification(w, r)
	})
	endpoint, c := tlsEndpoint(t, handler, false)
	c.Username = ""
	c.Password = ""
	a := openTestTLS(t, endpoint.URL, c)
	_ = a.Close()
	// The same valid trusted leaf repeated nine times verifies in Go, then must
	// fail our retained-chain bound. Material stays inside this test process.
	pair := endpoint.TLS.Certificates[0]
	for len(pair.Certificate) < 9 {
		pair.Certificate = append(pair.Certificate, pair.Certificate[0])
	}
	crowded := httptest.NewUnstartedServer(handler)
	crowded.Config.ErrorLog = log.New(io.Discard, "", 0)
	crowded.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
	crowded.StartTLS()
	defer crowded.Close()
	cfg := Config{Store: "search", Pool: 1, URL: crowded.URL, Connection: c}
	rejected, err := Open(context.Background(), cfg)
	if rejected != nil || err == nil {
		t.Fatal("excessive certificate chain accepted")
	}
}
