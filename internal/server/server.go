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
	Stall time.Duration
}

func DefaultLimits() Limits {
	limits := Limits{Stall: 30 * time.Second}
	return limits
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
	stores      map[string]*store.Runtime
	directory   *directory.Directory
	admission   *Admission
	peer        bool
	limits      Limits
	grpc        *grpc.Server
	once        sync.Once
	connections sync.Map
	metrics     transportMetrics
	serving     chan struct{}
}

func (l Limits) Validate() error {
	if l.Stall <= 0 {
		return status.Error(codes.InvalidArgument, "invalid transport stall timeout")
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
		limits:    l,
	}
	s.metrics = newTransportMetrics()
	s.serving = make(chan struct{})
	statistics := transportStats{}
	codec := &responseCodec{admission: s.admission}
	keepaliveParameters := keepalive.ServerParameters{Time: l.Stall, Timeout: min(5*time.Second, l.Stall), MaxConnectionIdle: time.Minute}
	receiveBytes, sendBytes := protocol.MaxExecuteRequestBytes, protocol.MaxExecuteResponseBytes
	if cfg.Peer {
		receiveBytes, sendBytes = directory.MaxSyncBytes, directory.MaxSyncBytes
	}
	options := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(receiveBytes),
		grpc.MaxSendMsgSize(sendBytes),
		grpc.MaxHeaderListSize(16 << 10),
		grpc.InitialWindowSize(64 << 10),
		grpc.InitialConnWindowSize(64 << 10),
		grpc.InTapHandle(s.admitRPC),
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
