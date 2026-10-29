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
		cfg := Config{Store: "search", URL: endpoint}
		if err := ValidateConfig(cfg); err != nil {
			t.Fatal("valid endpoint rejected", err)
		}
	}
	bad := []string{"http://host", "http://host:", "http://host:0", "http://host:65536", "http://host:+80", "https://a:b@host:443", "http://host:80/", "http://host:80?", "http://host:80#", "http://host:80/path", "http://host:80?q=v", "http://host:80#f", "http://%68ost:80", "http://[host]:80", "http://[fe80::1%25en0]:80", "http://127.1:80", "http://0127.0.0.1:80", "http://0.0.0.0:80", "http://[::]:80", "http://224.1.2.3:80", "http://host.:80", "http://.host:80", "http://bad_host:80", "http://-host:80", "http://host-:80", "http://a..b:80", "http://é.example:80", "http://host:80\\evil", "ftp://host:80", "https:host:443", strings.Repeat("a", 1025)}
	for _, endpoint := range bad {
		cfg := Config{Store: "search", URL: endpoint}
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("invalid endpoint accepted")
		}
	}
	for _, mode := range []string{"http-auth", "missing-user", "missing-password", "long-user", "long-pass", "colon", "control", "ca-http", "long-ca", "empty"} {
		t.Run(mode, func(t *testing.T) {
			c := &Connection{Username: "user", Password: "password-sentinel", CAFile: "/missing/ca-sentinel.pem"}
			cfg := Config{Store: "search", URL: "https://unresolved.invalid:443", Connection: c}
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
	cfg := Config{Store: "search", URL: endpoint, Connection: connection}
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
	endpoint, c := tlsEndpoint(t, handler, false)
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	a := openTestTLS(t, endpoint.URL, c)
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
	expired, expiredC := tlsEndpoint(t, handler, true)
	for _, mode := range []string{"wrong-ca", "expired", "missing-ca", "oversize-ca", "malformed-ca", "directory-ca", "system-roots"} {
		t.Run(mode, func(t *testing.T) {
			connection := *c
			cfg := Config{Store: "search", URL: endpoint.URL, Connection: &connection}
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
	a := openTestTLS(t, endpoint.URL, c)
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
	cfg := Config{Store: "search", URL: crowded.URL, Connection: c}
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
	endpoint, connection := tlsEndpoint(t, handler, false)
	der := endpoint.TLS.Certificates[0].Certificate[0]
	block := &pem.Block{Type: "CERTIFICATE", Bytes: der}
	certificate := pem.EncodeToMemory(block)
	data := bytes.Repeat(certificate, (256<<10)/len(certificate)+1)
	caFile := filepath.Join(t.TempDir(), "large-generated-trust-store")
	if err := os.WriteFile(caFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	connection.CAFile, connection.Username, connection.Password = caFile, username, password
	adapter := openTestTLS(t, endpoint.URL, connection)
	defer adapter.Close(context.Background())
}
