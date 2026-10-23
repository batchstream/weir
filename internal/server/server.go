// Package server serves StoreService over plaintext gRPC on an isolated network.
package server

import (
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// MaxMessageBytes bounds every request and response message.
const MaxMessageBytes = 64 << 20

// RecordStreamItems bounds the records one stream has queued or awaiting delivery.
const RecordStreamItems = 32

type Limits struct {
	MaxPendingRecords int
	KeepaliveInterval time.Duration
	KeepaliveTimeout  time.Duration
	Stall             time.Duration
	Handshake         time.Duration
	Idle              time.Duration
	// MaxConnectionAge ends each client connection with GOAWAY after roughly
	// this age. Streams already running finish first; the client reconnects
	// and re-resolves its address, which spreads load onto new replicas.
	MaxConnectionAge time.Duration
}

func DefaultLimits() Limits {
	limits := Limits{MaxPendingRecords: RecordStreamItems, KeepaliveInterval: 30 * time.Second, KeepaliveTimeout: 5 * time.Second, Stall: 30 * time.Second, Handshake: 5 * time.Second, Idle: time.Minute, MaxConnectionAge: 5 * time.Minute}
	return limits
}

func (l Limits) Validate() error {
	if l.Stall <= 0 || l.Handshake < 0 || l.Idle < 0 || l.MaxPendingRecords < 0 || l.KeepaliveInterval < 0 || l.KeepaliveInterval > 0 && l.KeepaliveInterval < time.Second || l.KeepaliveTimeout < 0 || l.MaxConnectionAge < 0 {
		return status.Error(codes.InvalidArgument, "invalid transport configuration")
	}
	return nil
}

type Config struct {
	Stores    map[string]*store.Runtime
	Limits    Limits
	Admission *Admission
}

type Server struct {
	stores    map[string]*store.Runtime
	admission *Admission
	limits    Limits
	grpc      *grpc.Server
	metrics   transportMetrics
	serving   chan struct{}
}

func New(cfg Config) (*Server, error) {
	l := cfg.Limits
	defaults := DefaultLimits()
	if l.Handshake == 0 {
		l.Handshake = defaults.Handshake
	}
	if l.MaxPendingRecords == 0 {
		l.MaxPendingRecords = defaults.MaxPendingRecords
	}
	if l.KeepaliveInterval == 0 {
		l.KeepaliveInterval = defaults.KeepaliveInterval
	}
	if l.KeepaliveTimeout == 0 {
		l.KeepaliveTimeout = defaults.KeepaliveTimeout
	}
	if l.MaxConnectionAge == 0 {
		l.MaxConnectionAge = defaults.MaxConnectionAge
	}
	if err := l.Validate(); err != nil {
		return nil, err
	}
	if cfg.Admission == nil {
		return nil, status.Error(codes.InvalidArgument, "missing admission")
	}
	stores := make(map[string]*store.Runtime, len(cfg.Stores))
	seen := make(map[*store.Runtime]bool)
	for name, runtime := range cfg.Stores {
		if !protocol.ValidStoreName(name) || runtime == nil || seen[runtime] {
			return nil, status.Error(codes.InvalidArgument, "invalid or aliased Store")
		}
		stores[name] = runtime
		seen[runtime] = true
	}
	s := &Server{
		stores:    stores,
		admission: cfg.Admission,
		limits:    l,
		metrics:   newTransportMetrics(),
		serving:   make(chan struct{}),
	}
	keepaliveParameters := keepalive.ServerParameters{Time: l.KeepaliveInterval, Timeout: l.KeepaliveTimeout, MaxConnectionIdle: l.Idle, MaxConnectionAge: l.MaxConnectionAge}
	options := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(MaxMessageBytes),
		grpc.MaxSendMsgSize(MaxMessageBytes),
		grpc.MaxHeaderListSize(64 << 10),
		grpc.InTapHandle(s.admitRPC),
		grpc.ChainStreamInterceptor(s.observeStream),
		grpc.KeepaliveParams(keepaliveParameters),
		grpc.WaitForHandlers(true),
		grpc.ConnectionTimeout(l.Handshake),
	}
	s.grpc = grpc.NewServer(options...)
	pb.RegisterStoreServiceServer(s.grpc, s)
	return s, nil
}
