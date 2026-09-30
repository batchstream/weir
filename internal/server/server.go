// Package server provides bounded application and peer transports for a deployment-isolated intranet.
package server

import (
	"net/http"
	"sync"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Limits struct {
	Connections, Sessions              int
	UnaryLifetime, BulkLifetime, Stall time.Duration
	ScanLifetime                       time.Duration
	NativeLifetime                     time.Duration
}

func DefaultLimits() Limits {
	l := Limits{
		Connections:    16,
		Sessions:       16,
		UnaryLifetime:  30 * time.Second,
		BulkLifetime:   15 * time.Minute,
		ScanLifetime:   5 * time.Minute,
		NativeLifetime: 5 * time.Minute,
		Stall:          30 * time.Second,
	}
	return l
}

type Config struct {
	Routes    map[string]Service
	Limits    Limits
	Admission *Admission
	// Peer selects ingress that requires an existing forwarding budget.
	Peer            bool
	InitialForwards int
}

type Server struct {
	pb.UnimplementedWeirServer
	routes          map[string]Service
	admission       *Admission
	peer            bool
	initialForwards int
	limits          Limits
	slots           chan struct{}
	grpc            *grpc.Server
	http            *http.Server
	draining        chan struct{}
	once            sync.Once
	connections     sync.Map
	metrics         transportMetrics
	serving         chan struct{}
}

func (l Limits) Validate() error {
	if l.Connections < 1 || l.Connections > 64 ||
		l.Sessions < 1 || l.Sessions > 64 ||
		l.UnaryLifetime <= 0 || l.UnaryLifetime > 30*time.Second ||
		l.BulkLifetime <= 0 || l.BulkLifetime > 15*time.Minute ||
		l.ScanLifetime <= 0 || l.ScanLifetime > 5*time.Minute ||
		l.Stall <= 0 || l.Stall > 30*time.Second ||
		l.NativeLifetime <= 0 || l.NativeLifetime > 5*time.Minute {
		return status.Error(codes.InvalidArgument, "invalid transport bounds")
	}
	return nil
}

func New(cfg Config) (*Server, error) {
	l := cfg.Limits
	if err := l.Validate(); err != nil {
		return nil, err
	}
	if len(cfg.Routes) == 0 || len(cfg.Routes) > 16 || cfg.Admission == nil || cfg.InitialForwards < 0 || cfg.InitialForwards > 8 {
		return nil, status.Error(codes.InvalidArgument, "invalid routes or ingress bounds")
	}
	if cap(cfg.Admission.slots) != l.Sessions || cap(cfg.Admission.connections) != l.Connections {
		return nil, status.Error(codes.InvalidArgument, "inconsistent shared admission bounds")
	}
	routes := make(map[string]Service, len(cfg.Routes))
	seen := make(map[*store.Runtime]bool)
	for name, service := range cfg.Routes {
		parsed, segments, err := protocol.ParseResource("weir://" + name)
		if err != nil ||
			parsed != name ||
			len(segments) != 0 ||
			(service.LocalStore == nil) == (service.RemoteWeir == nil) ||
			service.LocalStore != nil && seen[service.LocalStore] {
			return nil, status.Error(codes.InvalidArgument, "invalid or aliased service")
		}
		routes[name] = service
		if service.LocalStore != nil {
			seen[service.LocalStore] = true
		}
	}
	s := &Server{
		routes:          routes,
		admission:       cfg.Admission,
		peer:            cfg.Peer,
		initialForwards: cfg.InitialForwards,
		limits:          l,
		slots:           cfg.Admission.slots,
		draining:        cfg.Admission.draining,
	}
	s.metrics = newTransportMetrics()
	s.serving = make(chan struct{})
	statistics := deliveryStats{}
	s.grpc = grpc.NewServer(
		grpc.MaxRecvMsgSize(protocol.MaxFrame),
		grpc.MaxSendMsgSize(protocol.MaxFrame),
		grpc.UnaryInterceptor(s.unary),
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
	pb.RegisterWeirServer(s.grpc, s)
	return s, nil
}
