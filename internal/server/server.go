// Package server serves StoreService over plaintext gRPC on an isolated network.
package server

import (
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// MaxMessageBytes bounds every request and response message.
const MaxMessageBytes = 64 << 20

// Fixed flow-control windows: each stream may have streamWindowBytes of input
// in flight and each connection connectionWindowBytes, the largest window
// gRPC's bandwidth estimation grows to.
const (
	streamWindowBytes     = 1 << 20
	connectionWindowBytes = 16 << 20
)

// RecordStreamItems bounds the records one stream has queued or awaiting
// delivery; clients rely on the protocol window.
const RecordStreamItems = protocol.RecordWindow

// Limits must start from DefaultLimits; zero values are not defaulted.
type Limits struct {
	// recordWindow is the protocol's stream window; only tests shrink it.
	recordWindow      int
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
	limits := Limits{recordWindow: RecordStreamItems, KeepaliveInterval: 30 * time.Second, KeepaliveTimeout: 5 * time.Second, Stall: 30 * time.Second, Handshake: 5 * time.Second, Idle: time.Minute, MaxConnectionAge: 5 * time.Minute}
	return limits
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

func New(cfg Config) *Server {
	l := cfg.Limits
	s := &Server{
		stores:    cfg.Stores,
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
		// Fixed flow-control windows turn off gRPC's bandwidth estimation,
		// which pings a client every round trip while data flows.
		grpc.InitialWindowSize(streamWindowBytes),
		grpc.InitialConnWindowSize(connectionWindowBytes),
	}
	s.grpc = grpc.NewServer(options...)
	pb.RegisterStoreServiceServer(s.grpc, s)
	return s
}
