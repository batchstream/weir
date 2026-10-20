package mongodb

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/ocsp"
)

const mongoConnectTimeout = 2 * time.Second

var errTLSProfile = errors.New("invalid MongoDB TLS or OCSP profile")

// The profile must be parsed before any trust-file or network access.
func connectionOptions(cfg Config, dialer *connectionOwner, profile connectionProfile) (*options.ClientOptions, error) {
	opts := options.Client().SetHosts(profile.hosts).SetDirect(profile.direct)
	if profile.replicaSet != "" {
		opts.SetReplicaSet(profile.replicaSet)
	}
	if cfg.Username != "" {
		credential := options.Credential{
			AuthMechanism: profile.authMechanism,
			AuthSource:    profile.authSource,
			Username:      cfg.Username,
			Password:      cfg.Password,
			PasswordSet:   true,
		}
		opts.SetAuth(credential)
	}
	if profile.tls {
		config := &tls.Config{}
		if cfg.CAFile != "" {
			pool, err := mongoRoots(cfg.CAFile)
			if err != nil {
				return nil, err
			}
			config.RootCAs = pool
		}
		dialer.tlsConfig = config
	}
	// The owner performs verified TLS + OCSP for each actual member address.
	// Leave driver TLS disabled to avoid wrapping the same socket twice.
	opts.SetDialer(dialer)
	return opts, nil
}

func mongoRoots(name string) (*x509.CertPool, error) {
	// Reject special files before Open, which could otherwise block on a FIFO.
	info, err := os.Stat(name)
	if err != nil {
		return nil, errors.New("MongoDB CA file unavailable")
	}
	if !info.Mode().IsRegular() {
		return nil, errTLSProfile
	}
	file, err := os.Open(name)
	if err != nil {
		return nil, errors.New("MongoDB CA file unavailable")
	}
	defer file.Close()
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, errTLSProfile
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, errTLSProfile
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
		return nil, errTLSProfile
	}
	if config.ServerName == "" {
		config.ServerName = host
	}
	client := tls.Client(conn, config)
	if err := client.HandshakeContext(ctx); err != nil {
		return nil, err
	}
	state := client.ConnectionState()
	if len(state.VerifiedChains) == 0 {
		return nil, errTLSProfile
	}
	// The pinned verifier checks advertised HTTP responders with owned sockets,
	// caller cancellation, and no redirects or proxies.
	endpoints := state.VerifiedChains[0][0].OCSPServer
	for _, endpoint := range endpoints {
		u, err := url.Parse(endpoint)
		if err != nil || u.Scheme != "http" || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
			return nil, errTLSProfile
		}
	}
	dialer := newConnectionOwner()
	if deadline, ok := ctx.Deadline(); ok {
		dialer.timeout = time.Until(deadline)
	}
	defer dialer.close()
	transport := &http.Transport{
		DialContext:            dialer.dialConnection,
		DisableKeepAlives:      true,
		MaxConnsPerHost:        1,
		MaxResponseHeaderBytes: math.MaxInt64,
	}
	defer transport.CloseIdleConnections()
	observed := &ocspTransport{transport: transport}
	httpClient := &http.Client{Transport: observed, CheckRedirect: rejectOCSPRedirect}
	// A fresh cache is used for exactly one leaf verification: at most one entry,
	// released after this handshake. There is no process-wide certificate map.
	verify := &ocsp.VerifyOptions{Cache: ocsp.NewCache(), HTTPClient: httpClient}
	err = ocsp.Verify(ctx, state, verify)
	if observed.rejected.Load() {
		return nil, errTLSProfile
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

// Observe complete HTTP responses so transport and body I/O failures cannot be
// treated as the verifier's soft, inconclusive OCSP result. No payload quota applies.
type ocspTransport struct {
	transport *http.Transport
	rejected  atomic.Bool
}

func (t *ocspTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.transport.RoundTrip(request)
	if err != nil {
		// The verifier cancels other responders after one supplies valid evidence.
		// Caller cancellation is checked by mongoTLS after verification.
		if request.Context().Err() == nil {
			t.rejected.Store(true)
		}
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		t.rejected.Store(true)
		return nil, errTLSProfile
	}
	if err != nil {
		if request.Context().Err() == nil {
			t.rejected.Store(true)
		}
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(data))
	return response, nil
}
