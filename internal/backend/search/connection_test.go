package search

import (
	"bytes"
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
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestSearchConnectionValidation(t *testing.T) {
	good := []string{"http://10.0.0.1:9200", "https://Search.Example:443", "https://[::1]:9200", "http://127.0.0.1:9200", "https://192.168.1.2:443"}
	for _, endpoint := range good {
		cfg := Config{URL: endpoint}
		if err := ValidateConfig(cfg); err != nil {
			t.Fatal("valid endpoint rejected", err)
		}
	}
	bad := []string{"", "http://host", "http://host:", "http://host:+80", "https://a:b@host:443", "http://host:80/", "http://host:80?", "http://host:80/path", "http://host:80?q=v", "http://host:80#f", "http://%68ost:80", "http://host:80\\evil", "ftp://host:80", "https:host:443", " http://host:80", strings.Repeat("a", 1025)}
	for _, endpoint := range bad {
		cfg := Config{URL: endpoint}
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("invalid endpoint accepted", endpoint)
		}
	}
	for _, mode := range []string{"http-auth", "missing-user", "missing-password", "colon", "control", "ca-http"} {
		t.Run(mode, func(t *testing.T) {
			cfg := Config{URL: "https://unresolved.invalid:443", Username: "user", Password: "password-sentinel", CAFile: "/missing/ca-sentinel.pem"}
			if err := ValidateConfig(cfg); err != nil {
				t.Fatal("preflight accessed CA or DNS", err)
			}
			switch mode {
			case "http-auth":
				cfg.URL = "http://host:80"
			case "missing-user":
				cfg.Username = ""
			case "missing-password":
				cfg.Password = ""
			case "colon":
				cfg.Username = "bad:name"
			case "control":
				cfg.Password = "bad\r\nvalue"
			case "ca-http":
				cfg.URL = "http://host:80"
				cfg.Username = ""
				cfg.Password = ""
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
func tlsEndpoint(t *testing.T, handler http.Handler, expired bool) (*httptest.Server, Config) {
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
	c := Config{URL: server.URL, CAFile: name, Username: "test-app", Password: "test-password"}
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
func openTestTLS(t *testing.T, cfg Config) *Adapter {
	t.Helper()
	a, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(context.Background()) })
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
	_, c := tlsEndpoint(t, handler, false)
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	a := openTestTLS(t, c)
	call := exchange{path: "/probe", body: []byte(`{}`)}
	if _, _, err := a.request(context.Background(), call); err != nil {
		t.Fatal(err)
	}
	open := nativeRequest(t, "records", "GET", "/_doc/x")
	end, _ := runNative(t, a, open, nil)
	if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || calls.Load() != 2 {
		t.Fatal("native connection config missing")
	}
	ordinary, native := a.client.Transport.(*http.Transport), a.nativeClient.Transport.(*http.Transport)
	for _, transport := range []*http.Transport{ordinary, native} {
		if transport.Proxy != nil || !transport.DisableCompression || transport.Protocols.HTTP2() || !transport.Protocols.HTTP1() {
			t.Fatal("unsafe transport defaults")
		}
	}
	if !native.DisableKeepAlives || native.MaxConnsPerHost != 0 || ordinary.MaxConnsPerHost != 0 {
		t.Fatal("pool policy")
	}

}
func TestSearchTLSValidationAndCAErrors(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { qualification(w, r) })
	endpoint, c := tlsEndpoint(t, handler, false)
	_, other := tlsEndpoint(t, handler, false)
	_, expiredC := tlsEndpoint(t, handler, true)
	for _, mode := range []string{"wrong-ca", "wrong-host", "expired", "missing-ca", "oversize-ca", "malformed-ca", "directory-ca", "system-roots"} {
		t.Run(mode, func(t *testing.T) {
			cfg := c
			switch mode {
			case "wrong-ca":
				cfg.CAFile = other.CAFile
			case "wrong-host":
				// The certificate names search.test and loopback IPs, not localhost.
				_, port, _ := net.SplitHostPort(strings.TrimPrefix(endpoint.URL, "https://"))
				cfg.URL = "https://localhost:" + port
			case "expired":
				cfg = expiredC
			case "missing-ca":
				cfg.CAFile = "/missing/secret-sentinel.pem"
			case "directory-ca":
				cfg.CAFile = t.TempDir()
			case "system-roots":
				cfg.CAFile = ""
			default:
				cfg.CAFile = filepath.Join(t.TempDir(), "invalid-generated-ca")
				data := []byte("invalid-sentinel")
				if mode == "oversize-ca" {
					data = make([]byte, (256<<10)+1)
				}
				if err := os.WriteFile(cfg.CAFile, data, 0600); err != nil {
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
	_, c := tlsEndpoint(t, handler, false)
	a := openTestTLS(t, c)
	call := exchange{path: "/redirect", body: []byte(`{}`)}
	_, _, _ = a.request(context.Background(), call)
	open := nativeRequest(t, "records", "GET", "/_doc/x")
	end, capture := runNative(t, a, open, nil)
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
	a := openTestTLS(t, c)
	_ = a.Close(context.Background())
	// Trust follows Go's TLS/x509 verification without a separate certificate count.
	pair := endpoint.TLS.Certificates[0]
	for len(pair.Certificate) < 9 {
		pair.Certificate = append(pair.Certificate, pair.Certificate[0])
	}
	crowded := httptest.NewUnstartedServer(handler)
	crowded.Config.ErrorLog = log.New(io.Discard, "", 0)
	crowded.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
	crowded.StartTLS()
	defer crowded.Close()
	cfg := c
	cfg.URL = crowded.URL
	accepted, err := Open(context.Background(), cfg)
	if accepted == nil || err != nil {
		t.Fatal("valid TLS chain rejected by certificate count", err)
	}
	defer accepted.Close(context.Background())
}

func TestSearchLargeTrustStoreAndCredentials(t *testing.T) {
	username, password := strings.Repeat("u", 512), strings.Repeat("p", 1024)
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		user, pass, valid := request.BasicAuth()
		if !valid || user != username || pass != password {
			t.Error("long credentials changed")
		}
		qualification(w, request)
	})
	endpoint, cfg := tlsEndpoint(t, handler, false)
	der := endpoint.TLS.Certificates[0].Certificate[0]
	block := &pem.Block{Type: "CERTIFICATE", Bytes: der}
	certificate := pem.EncodeToMemory(block)
	data := bytes.Repeat(certificate, (256<<10)/len(certificate)+1)
	caFile := filepath.Join(t.TempDir(), "large-generated-trust-store")
	if err := os.WriteFile(caFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	cfg.CAFile, cfg.Username, cfg.Password = caFile, username, password
	adapter := openTestTLS(t, cfg)
	defer adapter.Close(context.Background())
}
