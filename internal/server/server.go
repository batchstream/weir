// Package server provides bounded application and peer transports for a deployment-isolated intranet.
package server

import (
	"sync"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"github.com/batchstream/weir/internal/directory"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

type Limits struct {
	Connections, Sessions  int
	RequestLifetime, Stall time.Duration
}

func DefaultLimits() Limits {
	l := Limits{
		Connections:     16,
		Sessions:        4,
		RequestLifetime: 15 * time.Minute,
		Stall:           30 * time.Second,
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
	draining        chan struct{}
	once            sync.Once
	connections     sync.Map
	metrics         transportMetrics
	serving         chan struct{}
}

func (l Limits) Validate() error {
	if l.Connections < 1 || uint64(l.Connections) > (64<<30)/(256<<10) ||
		l.Sessions < 1 || uint64(l.Sessions) > (64<<30)/(96<<20) ||
		l.RequestLifetime <= 0 ||
		l.Stall <= 0 {
		return status.Error(codes.InvalidArgument, "invalid transport bounds")
	}
	return nil
}

func New(cfg Config) (*Server, error) {
	l := cfg.Limits
	if err := l.Validate(); err != nil {
		return nil, err
	}
	if cfg.Admission == nil {
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
	statistics := transportStats{}
	codec := &responseCodec{admission: s.admission}
	keepaliveParameters := keepalive.ServerParameters{Time: l.Stall, Timeout: min(5*time.Second, l.Stall), MaxConnectionIdle: time.Minute}
	receiveBytes, sendBytes := protocol.MaxBatchRequestBytes, protocol.MaxBatchResponseBytes
	if cfg.Peer {
		receiveBytes, sendBytes = directory.MaxSyncBytes, directory.MaxSyncBytes
	}
	options := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(receiveBytes),
		grpc.MaxSendMsgSize(sendBytes),
		grpc.MaxConcurrentStreams(uint32(l.Sessions + cap(s.control))),
		grpc.MaxHeaderListSize(16 << 10),
		grpc.InTapHandle(s.admitRPC),
		grpc.UnaryInterceptor(unaryRPC),
		grpc.StreamInterceptor(streamRPC),
		grpc.StatsHandler(statistics),
		grpc.ForceServerCodecV2(codec),
		grpc.KeepaliveParams(keepaliveParameters),
		grpc.WaitForHandlers(true),
		grpc.ConnectionTimeout(min(5*time.Second, l.Stall)),
	}
	s.grpc = grpc.NewServer(options...)
	if cfg.Peer {
		if cfg.Directory != nil {
			peerpb.RegisterPeerDiscoveryServiceServer(s.grpc, cfg.Directory)
		}
	} else {
		pb.RegisterStoreServiceServer(s.grpc, s)
	}
	return s, nil
}
