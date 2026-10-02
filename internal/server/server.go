// Package server provides bounded application and peer transports for a deployment-isolated intranet.
package server

import (
	"net/http"
	"sync"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"github.com/batchstream/weir/internal/directory"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Limits struct {
	Connections, Sessions int
	RouteLifetime, Stall  time.Duration
}

func DefaultLimits() Limits {
	l := Limits{
		Connections:   16,
		Sessions:      4,
		RouteLifetime: 15 * time.Minute,
		Stall:         30 * time.Second,
	}
	return l
}

type Config struct {
	Stores    map[string]*store.Runtime
	Directory *directory.Directory
	Limits    Limits
	Admission *Admission
	// Peer exposes the directory control service instead of business RPCs.
	Peer bool
}

type Server struct {
	pb.UnimplementedStoreServiceServer
	stores          map[string]*store.Runtime
	directory       *directory.Directory
	admission       *Admission
	peer            bool
	control         chan struct{}
	connectionSlots chan struct{}
	limits          Limits
	slots           chan struct{}
	grpc            *grpc.Server
	controlGRPC     *grpc.Server
	http            *http.Server
	draining        chan struct{}
	once            sync.Once
	connections     sync.Map
	executionStats  executionCounters
	metrics         transportMetrics
	serving         chan struct{}
}

func (l Limits) Validate() error {
	if l.Connections < 1 || l.Connections > 64 ||
		l.Sessions < 1 || l.Sessions > 64 ||
		l.RouteLifetime <= 0 || l.RouteLifetime > 15*time.Minute ||
		l.Stall <= 0 || l.Stall > 30*time.Second {
		return status.Error(codes.InvalidArgument, "invalid transport bounds")
	}
	return nil
}

func New(cfg Config) (*Server, error) {
	l := cfg.Limits
	if err := l.Validate(); err != nil {
		return nil, err
	}
	if len(cfg.Stores) > 16 || cfg.Admission == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Stores or ingress bounds")
	}
	if cap(cfg.Admission.slots) != l.Sessions || cap(cfg.Admission.connections) != l.Connections {
		return nil, status.Error(codes.InvalidArgument, "inconsistent shared admission bounds")
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
		directory: cfg.Directory,
		admission: cfg.Admission,
		peer:      cfg.Peer,
		control:   make(chan struct{}, 2),
		limits:    l,
		slots:     cfg.Admission.slots,
		draining:  cfg.Admission.draining,
	}
	s.metrics = newTransportMetrics()
	s.connectionSlots = cfg.Admission.connections
	if cfg.Peer {
		s.connectionSlots = make(chan struct{}, 16)
	}
	s.serving = make(chan struct{})
	statistics := deliveryStats{}
	s.grpc = grpc.NewServer(
		grpc.MaxRecvMsgSize(protocol.MaxFrame),
		grpc.MaxSendMsgSize(protocol.MaxResponse),
		grpc.StatsHandler(statistics),
		grpc.WaitForHandlers(true),
	)
	s.controlGRPC = grpc.NewServer(
		grpc.MaxRecvMsgSize(directory.MaxSyncBytes),
		grpc.MaxSendMsgSize(directory.MaxSyncBytes),
		grpc.StatsHandler(statistics),
		grpc.WaitForHandlers(true),
	)
	protocols := &http.Protocols{}
	protocols.SetUnencryptedHTTP2(true)
	// Bound incomplete HTTP/2 headers/frames before a handler exists. A stalled
	// frame also prevents PING acknowledgements from being parsed; the native
	// transport closes it after the finite idle plus PING budgets.
	h2 := &http.HTTP2Config{
		MaxConcurrentStreams:          8,
		MaxReadFrameSize:              16 << 10,
		MaxReceiveBufferPerStream:     65535,
		MaxReceiveBufferPerConnection: 65535,
		SendPingTimeout:               l.Stall,
		PingTimeout:                   min(5*time.Second, l.Stall),
		WriteByteTimeout:              l.Stall,
	}
	s.http = &http.Server{
		Handler:           http.HandlerFunc(s.serveHTTP),
		Protocols:         protocols,
		HTTP2:             h2,
		ReadHeaderTimeout: min(5*time.Second, l.Stall),
		MaxHeaderBytes:    16 << 10,
		IdleTimeout:       time.Minute,
	}
	if cfg.Peer {
		if cfg.Directory != nil {
			peerpb.RegisterPeerDiscoveryServiceServer(s.controlGRPC, cfg.Directory)
		}
	} else {
		pb.RegisterStoreServiceServer(s.grpc, s)
		pb.RegisterStoreServiceServer(s.controlGRPC, s)
	}
	return s, nil
}
