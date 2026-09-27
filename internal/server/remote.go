package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const maxEndpoints = 8
const peerConnectTimeout = 2 * time.Second

// CanonicalEndpoints validates the whole set before any channel or worker exists.
// Identity is canonical host:port, independent of DNS answers and config order.
func CanonicalEndpoints(input []string) ([]string, error) {
	if len(input) < 1 || len(input) > maxEndpoints {
		return nil, errors.New("RemoteWeir requires 1-8 endpoints")
	}
	out := make([]string, 0, len(input))
	for _, value := range input {
		if len(value) > 260 || strings.ContainsAny(value, "/@?#%\\ \t\r\n") {
			return nil, errors.New("invalid peer address")
		}
		host, port, err := net.SplitHostPort(value)
		number, portErr := strconv.Atoi(port)
		if err != nil || portErr != nil || strings.Trim(port, "0123456789") != "" || number < 1 || number > 65535 {
			return nil, errors.New("peer requires host and explicit port 1-65535")
		}
		if ip, err := netip.ParseAddr(host); err == nil {
			host = ip.Unmap().String()
		} else {
			// An optional root dot is spelling, not an alternate identity. Lookups
			// always use an absolute name, never the machine's search suffixes.
			host = strings.TrimSuffix(strings.ToLower(host), ".")
			if len(host) < 1 || len(host) > 253 || strings.Trim(host, "0123456789.") == "" || strings.ContainsAny(value, "[]") {
				return nil, errors.New("invalid DNS peer name")
			}
			for _, label := range strings.Split(host, ".") {
				if len(label) < 1 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
					return nil, errors.New("invalid DNS peer label")
				}
				for _, c := range label {
					if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
						return nil, errors.New("peer name requires ASCII DNS labels")
					}
				}
			}
		}
		out = append(out, net.JoinHostPort(host, strconv.Itoa(number)))
	}
	slices.Sort(out)
	for i := 1; i < len(out); i++ {
		if out[i] == out[i-1] {
			return nil, errors.New("duplicate canonical peer endpoint")
		}
	}
	return out, nil
}

type RemoteConfig struct {
	Endpoints []string
	Relays    int
	// Resolver is the standard Go DNS dependency. Assembly uses nil (system
	// DNS); tests can supply a resolver dialing an owned loopback DNS fixture.
	Resolver *net.Resolver
}

type RemoteWeir struct {
	endpoints                             []*remoteEndpoint
	slots, sockets, resolutions           chan struct{}
	once                                  sync.Once
	closeErr                              error
	terminations, incompletes, rejections *prometheus.CounterVec
}

type remoteEndpoint struct {
	identity string
	owner    *RemoteWeir
	conn     *grpc.ClientConn
	client   pb.WeirClient
	sockets  chan struct{}
	mu       sync.Mutex
	closed   bool
	// nil means literal IP; DNS has an explicit, possibly empty, allowed set.
	addresses map[string]bool
	physical  map[*peerConn]struct{}
	dials     sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewRemote(cfg RemoteConfig) (*RemoteWeir, error) {
	identities, err := CanonicalEndpoints(cfg.Endpoints)
	if err != nil || cfg.Relays < 1 || cfg.Relays > 16 {
		return nil, errors.New("invalid peer endpoints or relay bound")
	}
	r := &RemoteWeir{slots: make(chan struct{}, cfg.Relays), sockets: make(chan struct{}, 2*len(identities)), resolutions: make(chan struct{}, 2)}
	termOpts := prometheus.CounterOpts{Name: "weir_relay_terminations_total", Help: "Admitted relay returns, including upstream I/O errors; never database executions."}
	incOpts := prometheus.CounterOpts{Name: "weir_relay_incomplete_total", Help: "Missing End, extra frames or non-OK after End observed from downstream."}
	rejOpts := prometheus.CounterOpts{Name: "weir_relay_rejections_total", Help: "RemoteWeir relay or socket capacity rejection events."}
	r.terminations = prometheus.NewCounterVec(termOpts, []string{"method", "status"})
	r.incompletes = prometheus.NewCounterVec(incOpts, []string{"method"})
	r.rejections = prometheus.NewCounterVec(rejOpts, []string{"reason"})
	for _, method := range metricMethods[:5] {
		for _, label := range metricStatuses {
			r.terminations.WithLabelValues(method, label)
		}
	}
	for _, method := range []string{"Bulk", "Scan", "Native"} {
		r.incompletes.WithLabelValues(method)
	}
	for _, reason := range []string{"relay", "socket"} {
		r.rejections.WithLabelValues(reason)
	}
	for _, identity := range identities {
		e := &remoteEndpoint{identity: identity, owner: r, sockets: make(chan struct{}, 2), physical: make(map[*peerConn]struct{})}
		e.ctx, e.cancel = context.WithCancel(context.Background())
		reconnect := backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 2 * time.Second}
		params := grpc.ConnectParams{Backoff: reconnect, MinConnectTimeout: peerConnectTimeout}
		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithIdleTimeout(0),
			grpc.WithContextDialer(e.dial), grpc.WithConnectParams(params),
			grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(),
			grpc.WithStaticStreamWindowSize(65535), grpc.WithStaticConnWindowSize(65535),
			grpc.WithReadBufferSize(16 << 10), grpc.WithWriteBufferSize(16 << 10), grpc.WithMaxHeaderListSize(16 << 10),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(protocol.MaxFrame), grpc.MaxCallSendMsgSize(protocol.MaxFrame), grpc.MaxRetryRPCBufferSize(0), grpc.WaitForReady(false)),
		}
		target := "passthrough:///" + identity
		host, port, _ := net.SplitHostPort(identity)
		if _, err := netip.ParseAddr(host); err != nil {
			e.addresses = make(map[string]bool)
			builder := &peerDNSBuilder{endpoint: e, host: host, port: port, resolver: cfg.Resolver}
			opts = append(opts, grpc.WithResolvers(builder))
			target = builder.Scheme() + ":///" + identity
		}
		e.conn, err = grpc.NewClient(target, opts...)
		if err != nil {
			e.cancel()
			_ = r.Close()
			return nil, err
		}
		e.client = pb.NewWeirClient(e.conn)
		r.endpoints = append(r.endpoints, e)
	}
	// Eager bounded connection establishment makes READY a useful selection
	// signal. It issues no application health or database requests.
	for _, e := range r.endpoints {
		e.conn.Connect()
	}
	return r, nil
}

// affinityScore: SHA-256("weir-rendezvous-v1\x00" || u32be(len(key)) ||
// key || u32be(len(identity)) || identity). Highest unsigned digest wins;
// an exact tie goes to the lexicographically smallest canonical identity.
func affinityScore(key, identity string) [32]byte {
	data := []byte("weir-rendezvous-v1\x00")
	data = binary.BigEndian.AppendUint32(data, uint32(len(key)))
	data = append(data, key...)
	data = binary.BigEndian.AppendUint32(data, uint32(len(identity)))
	data = append(data, identity...)
	return sha256.Sum256(data)
}

func (r *RemoteWeir) selectClient(ctx context.Context, key string) (pb.WeirClient, error) {
	var chosen *remoteEndpoint
	var best [32]byte
	ready := false
	for _, e := range r.endpoints {
		state := e.conn.GetState()
		if state == connectivity.Idle {
			e.conn.Connect()
		}
		if state == connectivity.Shutdown {
			continue
		}
		isReady := state == connectivity.Ready
		score := affinityScore(key, e.identity)
		if chosen == nil || isReady && !ready || isReady == ready && (bytes.Compare(score[:], best[:]) > 0 || score == best && e.identity < chosen.identity) {
			chosen, best, ready = e, score, isReady
		}
	}
	if chosen == nil {
		return nil, status.Error(codes.Unavailable, "peer closed")
	}
	// One selection, including cold start. This wait holds the existing relay
	// credit and consumes the original deadline. No second endpoint is tried.
	wait, cancel := context.WithTimeout(ctx, peerConnectTimeout)
	defer cancel()
	for {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		state := chosen.conn.GetState()
		if state == connectivity.Ready {
			return chosen.client, nil
		}
		if state == connectivity.TransientFailure || state == connectivity.Shutdown {
			return nil, status.Error(codes.Unavailable, "selected peer unavailable")
		}
		chosen.conn.Connect()
		if !chosen.conn.WaitForStateChange(wait, state) {
			if ctx.Err() != nil {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			return nil, status.Error(codes.Unavailable, "selected peer connection timeout")
		}
	}
}

func (e *remoteEndpoint) dial(ctx context.Context, address string) (net.Conn, error) {
	e.mu.Lock()
	if e.closed || e.addresses != nil && !e.addresses[address] {
		e.mu.Unlock()
		return nil, errors.New("peer address no longer eligible")
	}
	select {
	case e.sockets <- struct{}{}:
		e.owner.sockets <- struct{}{}
		e.dials.Add(1)
	default:
		e.mu.Unlock()
		e.owner.rejections.WithLabelValues("socket").Inc()
		return nil, errors.New("peer connection bound")
	}
	e.mu.Unlock()
	defer e.dials.Done()
	ctx, cancel := context.WithTimeout(ctx, peerConnectTimeout)
	stop := context.AfterFunc(e.ctx, cancel)
	defer func() { stop(); cancel() }()
	d := net.Dialer{Timeout: peerConnectTimeout, KeepAlive: 30 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", address)
	e.mu.Lock()
	defer e.mu.Unlock()
	if err == nil && (e.closed || e.addresses != nil && !e.addresses[address]) {
		_ = conn.Close()
		err = errors.New("peer address closed during dial")
	}
	if err != nil {
		<-e.sockets
		<-e.owner.sockets
		return nil, err
	}
	bounded := &peerConn{Conn: conn, endpoint: e}
	e.physical[bounded] = struct{}{}
	return bounded, nil
}

func (r *RemoteWeir) Close() error {
	r.once.Do(func() {
		for _, e := range r.endpoints {
			e.mu.Lock()
			e.closed = true
			e.cancel()
			e.mu.Unlock()
		}
		for _, e := range r.endpoints {
			r.closeErr = errors.Join(r.closeErr, e.conn.Close())
			e.dials.Wait()
			// gRPC removes draining SubConns from its active map before their
			// last stream ends. Explicit socket ownership also closes those.
			e.mu.Lock()
			conns := make([]*peerConn, 0, len(e.physical))
			for c := range e.physical {
				conns = append(conns, c)
			}
			e.mu.Unlock()
			for _, c := range conns {
				_ = c.Close()
			}
		}
	})
	return r.closeErr
}

type peerConn struct {
	net.Conn
	endpoint *remoteEndpoint
	once     sync.Once
	err      error
}

func (c *peerConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		c.endpoint.mu.Lock()
		delete(c.endpoint.physical, c)
		<-c.endpoint.sockets
		<-c.endpoint.owner.sockets
		c.endpoint.mu.Unlock()
	})
	return c.err
}
