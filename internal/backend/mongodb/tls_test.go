package mongodb

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/binary"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil/testdns"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/crypto/ocsp"
)

type certificateOptions struct {
	expired, mustStaple bool
	responders          []string
}
type testCertificate struct {
	config       *tls.Config
	roots        *x509.CertPool
	leaf, issuer *x509.Certificate
	key          *ecdsa.PrivateKey
}

func newTestCertificate(t *testing.T, opts certificateOptions) testCertificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "owned test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, OCSPServer: opts.responders}
	if opts.expired {
		leaf.NotAfter = time.Now().Add(-time.Minute)
	}
	if opts.mustStaple {
		data, err := asn1.Marshal([]int{5})
		if err != nil {
			t.Fatal(err)
		}
		extension := pkix.Extension{Id: asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 7, 1, 24}, Value: data}
		leaf.ExtraExtensions = []pkix.Extension{extension}
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err = x509.ParseCertificate(leafDER)
	if err != nil {
		t.Fatal(err)
	}
	pair := tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: key}
	config := &tls.Config{Certificates: []tls.Certificate{pair}}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	result := testCertificate{config: config, roots: roots, leaf: leaf, issuer: ca, key: key}
	return result
}

func testOCSP(t *testing.T, cert testCertificate, response ocsp.Response) []byte {
	t.Helper()
	response.SerialNumber = cert.leaf.SerialNumber
	data, err := ocsp.CreateResponse(cert.issuer, cert.issuer, response, cert.key)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func tlsWireServer(t *testing.T, config *tls.Config, message []byte) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		raw, err := listener.Accept()
		if err != nil {
			return
		}
		defer raw.Close()
		_ = raw.SetDeadline(time.Now().Add(3 * time.Second))
		conn := tls.Server(raw, config)
		if err := conn.Handshake(); err != nil {
			return
		}
		_, _ = conn.Write(message)
	}()
	t.Cleanup(func() {
		listener.Close()
		select {
		case <-done:
		case <-time.After(4 * time.Second):
			t.Error("TLS fixture did not stop")
		}
	})
	return listener.Addr().String()
}

func TestMongoTLSVerificationAndOCSP(t *testing.T) {
	for _, mode := range []string{"no_extension", "SNI", "expired", "bad_root", "staple_good", "staple_revoked", "staple_invalid", "staple_expired", "must_staple_missing", "staple_oversize", "responder_good", "responder_revoked", "responder_invalid_soft", "responder_oversize", "responder_redirect", "responder_headers", "responder_stall", "multiple_responders", "unsupported_responder"} {
		t.Run(mode, func(t *testing.T) {
			opts := certificateOptions{expired: mode == "expired", mustStaple: mode == "must_staple_missing"}
			var payload []byte
			if strings.HasPrefix(mode, "responder_") || mode == "multiple_responders" {
				handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if mode == "responder_headers" {
						w.Header().Set("Too-Large", strings.Repeat("x", 32<<10))
					}
					if mode == "responder_redirect" {
						w.Header().Set("Location", "http://127.0.0.1:1/")
						w.WriteHeader(302)
						return
					}
					if mode == "responder_stall" {
						select {
						case <-r.Context().Done():
						case <-time.After(time.Second):
						}
						return
					}
					_, _ = w.Write(payload)
				})
				responder := httptest.NewServer(handler)
				defer responder.Close()
				opts.responders = []string{responder.URL}
				if mode == "multiple_responders" {
					opts.responders = append(opts.responders, responder.URL)
				}
			}
			if mode == "unsupported_responder" {
				opts.responders = []string{"https://localhost:1/"}
			}
			cert := newTestCertificate(t, opts)
			response := ocsp.Response{Status: ocsp.Good, ThisUpdate: time.Now().Add(-time.Minute), NextUpdate: time.Now().Add(time.Hour)}
			if strings.Contains(mode, "revoked") {
				response.Status = ocsp.Revoked
				response.RevokedAt = time.Now().Add(-time.Minute)
			}
			if mode == "staple_expired" {
				response.NextUpdate = time.Now().Add(-time.Second)
			}
			payload = testOCSP(t, cert, response)
			if strings.Contains(mode, "invalid") {
				payload = []byte("malformed")
			}
			if strings.Contains(mode, "oversize") {
				payload = make([]byte, maxOCSPBytes+1)
			}
			if strings.HasPrefix(mode, "staple_") {
				cert.config.Certificates[0].OCSPStaple = payload
			}
			sni := make(chan string, 1)
			cert.config.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) { sni <- hello.ServerName; return nil, nil }
			address := tlsWireServer(t, cert.config, nil)
			config := &tls.Config{RootCAs: cert.roots}
			if mode == "SNI" {
				_, port, _ := net.SplitHostPort(address)
				address = net.JoinHostPort("localhost", port)
			}
			if mode == "bad_root" {
				config.RootCAs = x509.NewCertPool()
			}
			d := newConnectionOwner()
			d.tlsConfig = config
			ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer cancel()
			conn, err := d.DialContext(ctx, "tcp", address)
			if conn != nil {
				conn.Close()
			}
			good := mode == "no_extension" || mode == "SNI" || mode == "staple_good" || mode == "responder_good" || mode == "responder_invalid_soft"
			if mode == "SNI" && err == nil && <-sni != "localhost" {
				t.Fatal("missing DNS SNI")
			}
			if (err == nil) != good {
				t.Fatalf("accepted=%t expected=%t error=%v", err == nil, good, err)
			}
		})
	}
}

func TestMongoTLSWireGuardBeforeDriverHeader(t *testing.T) {
	opts := certificateOptions{}
	cert := newTestCertificate(t, opts)
	for _, mode := range []string{"good", "huge_length", "short_length", "errors", "bad_flags", "truncated"} {
		t.Run(mode, func(t *testing.T) {
			doc := bson.D{{Key: "ok", Value: 1.0}}
			if mode == "errors" {
				errors := make(bson.A, 4097)
				for i := range errors {
					errors[i] = "error"
				}
				field := bson.E{Key: "writeErrors", Value: errors}
				doc = append(doc, field)
			}
			message := scanWireMessage(t, doc)
			switch mode {
			case "huge_length":
				message = message[:16]
				binary.LittleEndian.PutUint32(message, scanNativeLimit+1)
			case "short_length":
				binary.LittleEndian.PutUint32(message, 15)
			case "bad_flags":
				message[16] = 2
			case "truncated":
				message = message[:len(message)-1]
			}
			address := tlsWireServer(t, cert.config, message)
			d := newConnectionOwner()
			d.tlsConfig = &tls.Config{RootCAs: cert.roots}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			conn, err := d.DialContext(ctx, "tcp", address)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetReadDeadline(time.Now().Add(time.Second))
			header := make([]byte, 4)
			n, err := conn.Read(header)
			if mode == "good" {
				if n != 4 || err != nil {
					t.Fatal(n, err)
				}
			} else if n != 0 || err == nil {
				t.Fatal("decrypted invalid header exposed to driver", n, err)
			}
		})
	}
}

func TestMongoTLSHandshakeStallCancellation(t *testing.T) {
	baseline := runtime.NumGoroutine()
	for i := 0; i < 13; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, _ = io.Copy(io.Discard, conn)
		}()
		d := newConnectionOwner()
		d.tlsConfig = &tls.Config{}
		duration := 20 * time.Millisecond
		if i == 12 {
			duration = 3 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		start := time.Now()
		conn, err := d.DialContext(ctx, "tcp", listener.Addr().String())
		cancel()
		if conn != nil {
			conn.Close()
		}
		listener.Close()
		if err == nil || i < 12 && time.Since(start) > time.Second || time.Since(start) > 2500*time.Millisecond {
			t.Fatal("handshake exceeded cancellation bound")
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("failed handshake socket retained")
		}
	}
	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > baseline+2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if runtime.NumGoroutine() > baseline+2 {
		t.Fatal("handshake goroutines retained", baseline, runtime.NumGoroutine())
	}
	t.Logf("TLS stalls=13 (12 cancelled, one 2s hard deadline) baseline goroutines=%d after=%d", baseline, runtime.NumGoroutine())
}

func TestMongoDNSCancellation(t *testing.T) {
	fixture := testdns.Start(t)
	answer := testdns.Answer{Drop: true}
	fixture.Set("mongo.weir.test", answer)
	baseline := runtime.NumGoroutine()
	for i := 0; i < 8; i++ {
		d := newConnectionOwner()
		d.resolver = fixture.Resolver()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		start := time.Now()
		conn, err := d.DialContext(ctx, "tcp", "mongo.weir.test:27017")
		cancel()
		if conn != nil {
			conn.Close()
		}
		if err == nil || time.Since(start) > time.Second {
			t.Fatal("DNS did not honor connection deadline")
		}
	}
	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > baseline+2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if runtime.NumGoroutine() > baseline+2 {
		t.Fatal("DNS work retained after cancelled attempts")
	}
	if fixture.Queries.Load() == 0 {
		t.Fatal("DNS fault not exercised")
	}
	t.Logf("DNS cancelled attempts=8 queries=%d goroutines before=%d after=%d", fixture.Queries.Load(), baseline, runtime.NumGoroutine())
}
