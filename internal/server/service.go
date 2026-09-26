package server

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// Service is a closed choice between the two static execution destinations.
// Assembly owns both; listeners only borrow references.
type Service struct {
	LocalStore *store.Runtime
	RemoteWeir *RemoteWeir
}

type Admission struct {
	slots, connections chan struct{}
	draining           chan struct{}
	once               sync.Once
	overloaded         atomic.Bool
}

func NewAdmission(l Limits) (*Admission, error) {
	if err := l.Validate(); err != nil {
		return nil, err
	}
	a := &Admission{slots: make(chan struct{}, l.Sessions), connections: make(chan struct{}, l.Connections), draining: make(chan struct{})}
	return a, nil
}
func (a *Admission) SetOverloaded(value bool) { a.overloaded.Store(value) }
func (a *Admission) BeginDrain()              { a.once.Do(func() { close(a.draining) }) }
func (a *Admission) check() error {
	select {
	case <-a.draining:
		return status.Error(codes.Unavailable, "draining")
	default:
	}
	if a.overloaded.Load() {
		return status.Error(codes.ResourceExhausted, "process overloaded")
	}
	return nil
}

type RemoteConfig struct {
	Endpoint string
	TLS      *tls.Config
	Relays   int
}

// One ClientConn per configured Service; at most two sockets allow a draining
// HTTP/2 connection to coexist with its replacement. There is no endpoint failover.
type RemoteWeir struct {
	conn     *grpc.ClientConn
	client   pb.WeirClient
	slots    chan struct{}
	sockets  chan struct{}
	once     sync.Once
	closeErr error
}

func NewRemote(cfg RemoteConfig) (*RemoteWeir, error) {
	host, port, err := net.SplitHostPort(cfg.Endpoint)
	number, portErr := strconv.Atoi(port)
	if err != nil || net.ParseIP(host) == nil || portErr != nil || number < 1 || number > 65535 || cfg.Relays < 1 || cfg.Relays > 16 {
		return nil, errors.New("invalid fixed peer endpoint or relay bound")
	}
	if cfg.TLS == nil || cfg.TLS.InsecureSkipVerify || cfg.TLS.ServerName == "" || cfg.TLS.RootCAs == nil || len(cfg.TLS.Certificates) != 1 || cfg.TLS.MinVersion < tls.VersionTLS13 {
		return nil, errors.New("peer requires verified TLS 1.3 and a client identity")
	}
	r := &RemoteWeir{slots: make(chan struct{}, cfg.Relays), sockets: make(chan struct{}, 2)}
	reconnect := backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 2 * time.Second}
	params := grpc.ConnectParams{Backoff: reconnect, MinConnectTimeout: 2 * time.Second}
	r.conn, err = grpc.NewClient("passthrough:///"+cfg.Endpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(cfg.TLS.Clone())), grpc.WithNoProxy(),
		grpc.WithContextDialer(r.dial), grpc.WithConnectParams(params),
		grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(),
		grpc.WithStaticStreamWindowSize(65535), grpc.WithStaticConnWindowSize(65535),
		grpc.WithReadBufferSize(16<<10), grpc.WithWriteBufferSize(16<<10), grpc.WithMaxHeaderListSize(16<<10),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(protocol.MaxFrame), grpc.MaxCallSendMsgSize(protocol.MaxFrame), grpc.MaxRetryRPCBufferSize(0), grpc.WaitForReady(false)))
	if err != nil {
		return nil, err
	}
	r.client = pb.NewWeirClient(r.conn)
	return r, nil
}
func (r *RemoteWeir) dial(ctx context.Context, address string) (net.Conn, error) {
	select {
	case r.sockets <- struct{}{}:
	default:
		return nil, errors.New("peer connection bound")
	}
	d := net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		<-r.sockets
		return nil, err
	}
	bounded := &peerConn{Conn: conn, slots: r.sockets}
	return bounded, nil
}
func (r *RemoteWeir) Close() error {
	r.once.Do(func() { r.closeErr = r.conn.Close() })
	return r.closeErr
}
func (r *RemoteWeir) enter(d *delivery) error {
	select {
	case r.slots <- struct{}{}:
		d.mu.Lock()
		if d.finished {
			d.mu.Unlock()
			<-r.slots
			return status.Error(codes.Canceled, "delivery closed")
		}
		d.remoteSlots = r.slots
		d.mu.Unlock()
		return nil
	default:
		return status.Error(codes.ResourceExhausted, "peer relay bound")
	}
}

type peerConn struct {
	net.Conn
	slots chan struct{}
	once  sync.Once
	err   error
}

func (c *peerConn) Close() error {
	c.once.Do(func() { c.err = c.Conn.Close(); <-c.slots })
	return c.err
}
