package mongodb

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/ocsp"
)

const mongoConnectTimeout = 2 * time.Second
const maxOCSPBytes = 64 << 10

var errTLSBound = errors.New("MongoDB TLS profile exceeds certificate or OCSP bounds")

// ApplyURI normally loads the CA without a size limit. Remove only that option,
// then supply the same exclusive root pool using a bounded regular-file read.
// ValidateURI must precede this function (including all file and DNS access).
func connectionOptions(raw string, dialer *boundedDialer) (*options.ClientOptions, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("invalid MongoDB connection profile")
	}
	query := parsed.Query()
	caFile := ""
	for key, values := range query {
		if strings.EqualFold(key, "tlsCAFile") {
			caFile = values[0]
			query.Del(key)
		}
	}
	parsed.RawQuery = query.Encode()
	opts := options.Client().ApplyURI(parsed.String())
	if err := opts.Validate(); err != nil {
		return nil, errors.New("invalid MongoDB connection profile")
	}
	if caFile != "" {
		pool, err := mongoRoots(caFile)
		if err != nil {
			return nil, err
		}
		opts.TLSConfig.RootCAs = pool
	}
	dialer.tlsConfig = opts.TLSConfig
	// The dialer performs the pinned driver's TLS + OCSP sequence before exposing
	// decrypted Mongo frames. Disable the second TLS wrapping, never verification.
	opts.TLSConfig = nil
	opts.SetDialer(dialer)
	return opts, nil
}

func mongoRoots(name string) (*x509.CertPool, error) {
	// Reject special files before Open, which could otherwise block on a FIFO.
	info, err := os.Stat(name)
	if err != nil {
		return nil, errors.New("MongoDB CA file unavailable")
	}
	if !info.Mode().IsRegular() || info.Size() > 256<<10 {
		return nil, errTLSBound
	}
	file, err := os.Open(name)
	if err != nil {
		return nil, errors.New("MongoDB CA file unavailable")
	}
	defer file.Close()
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 256<<10 {
		return nil, errTLSBound
	}
	data, err := io.ReadAll(io.LimitReader(file, (256<<10)+1))
	if err != nil || len(data) > 256<<10 {
		return nil, errTLSBound
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return nil, errors.New("invalid MongoDB CA file")
	}
	return roots, nil
}

func mongoTLS(ctx context.Context, conn net.Conn, config *tls.Config, address string) (net.Conn, error) {
	config = config.Clone()
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errTLSBound
	}
	if config.ServerName == "" {
		config.ServerName = host
	}
	client := tls.Client(conn, config)
	if err := client.HandshakeContext(ctx); err != nil {
		return nil, err
	}
	state := client.ConnectionState()
	if len(state.PeerCertificates) > 8 || len(state.OCSPResponse) > maxOCSPBytes || len(state.VerifiedChains) == 0 {
		return nil, errTLSBound
	}
	for _, cert := range state.PeerCertificates {
		if len(cert.Raw) > 64<<10 {
			return nil, errTLSBound
		}
	}
	// The fixed verifier fans out once per advertised responder. This finite
	// profile accepts at most one HTTP responder and no redirects or proxies.
	endpoints := state.VerifiedChains[0][0].OCSPServer
	if len(endpoints) > 1 {
		return nil, errTLSBound
	}
	for _, endpoint := range endpoints {
		u, err := url.Parse(endpoint)
		if err != nil || len(endpoint) > 2048 || u.Scheme != "http" || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
			return nil, errTLSBound
		}
	}
	dialer := newBoundedDialer(1, 1)
	defer dialer.close()
	transport := &http.Transport{
		DialContext:            dialer.dialConnection,
		DisableKeepAlives:      true,
		MaxConnsPerHost:        1,
		MaxResponseHeaderBytes: 16 << 10,
		ResponseHeaderTimeout:  mongoConnectTimeout,
	}
	defer transport.CloseIdleConnections()
	bounded := &ocspTransport{transport: transport}
	httpClient := &http.Client{Transport: bounded, Timeout: mongoConnectTimeout, CheckRedirect: rejectOCSPRedirect}
	// A fresh cache is used for exactly one leaf verification: at most one entry,
	// released after this handshake. There is no process-wide certificate map.
	verify := &ocsp.VerifyOptions{Cache: ocsp.NewCache(), HTTPClient: httpClient}
	err = ocsp.Verify(ctx, state, verify)
	if bounded.rejected.Load() {
		return nil, errTLSBound
	}
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return client, nil
}

func rejectOCSPRedirect(_ *http.Request, _ []*http.Request) error {
	return errors.New("MongoDB OCSP redirects are outside the connection profile")
}

// This is the HTTP RoundTripper boundary required by the fixed OCSP verifier,
// whose ReadAll otherwise has no bound. Oversize input is a hard profile failure,
// not a soft failure. I/O errors (including header limits) are also rejected;
// a bounded, complete but inconclusive OCSP response retains driver semantics.
type ocspTransport struct {
	transport *http.Transport
	rejected  atomic.Bool
}

func (t *ocspTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.transport.RoundTrip(request)
	if err != nil {
		t.rejected.Store(true)
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxOCSPBytes+1))
	if len(data) > maxOCSPBytes || response.ContentLength > maxOCSPBytes || response.StatusCode >= 300 && response.StatusCode < 400 {
		t.rejected.Store(true)
		return nil, errTLSBound
	}
	if err != nil {
		t.rejected.Store(true)
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(data))
	return response, nil
}
