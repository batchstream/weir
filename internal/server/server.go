// Package server provides bounded application and peer transports for a deployment-isolated intranet.
package server

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type Limits struct {
	Connections, Sessions              int
	UnaryLifetime, BulkLifetime, Stall time.Duration
	ScanLifetime                       time.Duration
	NativeLifetime                     time.Duration
}

func DefaultLimits() Limits {
	l := Limits{Connections: 16, Sessions: 16, UnaryLifetime: 30 * time.Second, BulkLifetime: 15 * time.Minute, ScanLifetime: 5 * time.Minute, NativeLifetime: 5 * time.Minute, Stall: 30 * time.Second}
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
	if l.Connections < 1 || l.Connections > 64 || l.Sessions < 1 || l.Sessions > 64 || l.UnaryLifetime <= 0 || l.UnaryLifetime > 30*time.Second || l.BulkLifetime <= 0 || l.BulkLifetime > 15*time.Minute || l.ScanLifetime <= 0 || l.ScanLifetime > 5*time.Minute || l.Stall <= 0 || l.Stall > 30*time.Second || l.NativeLifetime <= 0 || l.NativeLifetime > 5*time.Minute {
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
		if err != nil || parsed != name || len(segments) != 0 || (service.LocalStore == nil) == (service.RemoteWeir == nil) || service.LocalStore != nil && seen[service.LocalStore] {
			return nil, status.Error(codes.InvalidArgument, "invalid or aliased service")
		}
		routes[name] = service
		if service.LocalStore != nil {
			seen[service.LocalStore] = true
		}
	}
	s := &Server{routes: routes, admission: cfg.Admission, peer: cfg.Peer, initialForwards: cfg.InitialForwards, limits: l, slots: cfg.Admission.slots, draining: cfg.Admission.draining}
	s.metrics = newTransportMetrics()
	s.serving = make(chan struct{})
	statistics := deliveryStats{}
	s.grpc = grpc.NewServer(grpc.MaxRecvMsgSize(protocol.MaxFrame), grpc.MaxSendMsgSize(protocol.MaxFrame), grpc.UnaryInterceptor(s.unary), grpc.StatsHandler(statistics), grpc.WaitForHandlers(true))
	protocols := &http.Protocols{}
	protocols.SetUnencryptedHTTP2(true)
	// Bound incomplete HTTP/2 headers/frames before a handler exists. A stalled
	// frame also prevents PING acknowledgements from being parsed; the native
	// transport closes it after the finite idle plus PING budgets.
	h2 := &http.HTTP2Config{MaxConcurrentStreams: 8, MaxReadFrameSize: 16 << 10, MaxReceiveBufferPerStream: 65535, MaxReceiveBufferPerConnection: 65535, SendPingTimeout: l.Stall, PingTimeout: min(5*time.Second, l.Stall), WriteByteTimeout: l.Stall}
	s.http = &http.Server{Handler: http.HandlerFunc(s.serveHTTP), Protocols: protocols, HTTP2: h2, ReadHeaderTimeout: min(5*time.Second, l.Stall), MaxHeaderBytes: 16 << 10, IdleTimeout: time.Minute}
	pb.RegisterWeirServer(s.grpc, s)
	return s, nil
}
func (s *Server) enter() error {
	if err := s.admission.check(); err != nil {
		return err
	}
	select {
	case s.slots <- struct{}{}:
		return nil
	default:
		s.admission.rejections.WithLabelValues("sessions").Inc()
		return status.Error(codes.ResourceExhausted, "session limit")
	}
}
func (s *Server) unary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	state, ok := ctx.Value(deliveryKey).(*delivery)
	if !ok {
		return nil, status.Error(codes.Internal, "missing HTTP/2 delivery lifetime")
	}
	result, err := handler(ctx, req)
	state.beginResponse()
	return result, err
}
func (s *Server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResult, error) {
	variant := &pb.BulkOperation_Read{Read: req}
	op := &pb.BulkOperation{Operation: variant}
	result, err := s.single(ctx, op)
	if err != nil {
		return nil, err
	}
	return result.GetRead(), nil
}
func (s *Server) Mutate(ctx context.Context, req *pb.MutateRequest) (*pb.MutationResult, error) {
	variant := &pb.BulkOperation_Mutate{Mutate: req}
	op := &pb.BulkOperation{Operation: variant}
	result, err := s.single(ctx, op)
	if err != nil {
		return nil, err
	}
	return result.GetMutation(), nil
}
func (s *Server) single(ctx context.Context, op *pb.BulkOperation) (*pb.BulkResult, error) {
	state, ok := ctx.Value(deliveryKey).(*delivery)
	if !ok {
		return nil, status.Error(codes.Internal, "missing HTTP/2 delivery lifetime")
	}
	service, name, failure := s.resolve(protocol.Resource(op), false)
	if failure == nil {
		failure = protocol.Validate(op, name)
		if failure != nil {
			s.admission.rejections.WithLabelValues("operation").Inc()
		}
	}
	if failure != nil {
		return protocol.ResultError(op, pb.MutationOutcome_NOT_STARTED, failure), nil
	}
	if service.RemoteWeir != nil {
		args := singleRelay{ctx: ctx, remote: service.RemoteWeir, operation: op, delivery: state}
		return s.remoteSingle(args)
	}
	runtime := service.LocalStore
	plan, f := runtime.Prepare(op)
	if f != nil {
		return protocol.ResultError(op, pb.MutationOutcome_NOT_STARTED, f), nil
	}
	ticket, f, _ := runtime.Submit(ctx, plan, nil)
	if f != nil {
		return protocol.ResultError(op, pb.MutationOutcome_NOT_STARTED, f), nil
	}
	result, err := ticket.Wait(ctx)
	if err != nil {
		return nil, status.FromContextError(err).Err()
	}
	state.retain(ticket)
	return result, nil
}

func (s *Server) resolve(resource string, root bool) (Service, string, *pb.Failure) {
	empty := Service{}
	name, segments, err := protocol.ParseResource(resource)
	if err != nil || root && len(segments) != 0 {
		s.admission.rejections.WithLabelValues("route").Inc()
		return empty, "", protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid resource")
	}
	service, ok := s.routes[name]
	if !ok {
		s.admission.rejections.WithLabelValues("route").Inc()
		return empty, name, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "unknown store")
	}
	return service, name, nil
}

type receiveEnd struct {
	count uint64
	err   error
}

func (s *Server) Bulk(stream grpc.BidiStreamingServer[pb.BulkRequestFrame, pb.BulkResponseFrame]) error {
	ctx, cancel := context.WithTimeout(stream.Context(), s.limits.BulkLifetime)
	defer cancel()
	opening := make(chan *pb.BulkRequestFrame, 1)
	openError := make(chan error, 1)
	go func() {
		first, err := stream.Recv()
		if err != nil {
			openError <- err
			return
		}
		opening <- first
	}()
	timer := time.NewTimer(s.limits.Stall)
	defer timer.Stop()
	var first *pb.BulkRequestFrame
	select {
	case first = <-opening:
	case <-openError:
		return status.Error(codes.InvalidArgument, "Bulk requires Open")
	case <-timer.C:
		s.metrics.watchdogs.WithLabelValues("open").Inc()
		s.abortPeer(ctx)
		return status.Error(codes.DeadlineExceeded, "Bulk Open stalled")
	case <-ctx.Done():
		s.abortPeer(ctx)
		return status.FromContextError(ctx.Err()).Err()
	case <-s.draining:
		s.abortPeer(ctx)
		return status.Error(codes.Unavailable, "draining")
	}
	if first.GetOpen() == nil {
		return status.Error(codes.InvalidArgument, "Bulk requires Open")
	}
	service, name, failure := s.resolve(first.GetOpen().Store, true)
	if failure != nil {
		return status.Error(codes.InvalidArgument, "invalid Bulk Open")
	}
	if service.RemoteWeir != nil {
		return s.remoteBulk(stream, first, service.RemoteWeir, name)
	}
	runtime := service.LocalStore
	session := runtime.NewSession()
	defer session.Close()
	invalid := make(chan *pb.BulkResult, 1)
	ended := make(chan receiveEnd, 1)
	activity := make(chan struct{}, 1)
	args := receiveArgs{name: name, runtime: runtime, ctx: ctx, stream: stream, session: session, invalid: invalid, ended: ended, activity: activity}
	go s.receive(args)
	idle := time.NewTimer(s.limits.Stall)
	defer idle.Stop()
	var count, delivered uint64
	finished := false
	draining := s.draining
	drainMode := false
	for {
		if drainMode && session.Outstanding() == 0 {
			return status.Error(codes.Unavailable, "draining; unreported operations are indeterminate")
		}
		if finished && delivered == count {
			end := &pb.BulkEnd{ReceivedCount: count, ResultCount: delivered}
			variant := &pb.BulkResponseFrame_End{End: end}
			frame := &pb.BulkResponseFrame{Frame: variant}
			return s.send(ctx, stream, frame)
		}
		var result *pb.BulkResult
		var ticket *store.Ticket
		select {
		case <-ctx.Done():
			if stream.Context().Err() == nil {
				s.abortPeer(stream.Context())
			}
			return status.FromContextError(ctx.Err()).Err()
		case <-draining:
			drainMode = true
			draining = nil
			continue
		case <-idle.C:
			s.metrics.watchdogs.WithLabelValues("input_or_result").Inc()
			s.abortPeer(stream.Context())
			return status.Error(codes.DeadlineExceeded, "input/result stall")
		case <-activity:
			idle.Reset(s.limits.Stall)
			continue
		case end := <-ended:
			if end.err != nil {
				select {
				case <-s.draining:
					drainMode = true
					draining = nil
					continue
				default:
				}
				return end.err
			}
			count = end.count
			finished = true
			continue
		case result = <-invalid:
		case ticket = <-session.Results:
			result = ticket.Result()
		}
		variant := &pb.BulkResponseFrame_Result{Result: result}
		frame := &pb.BulkResponseFrame{Frame: variant}
		err := s.send(ctx, stream, frame)
		if ticket != nil {
			ticket.Ack()
		}
		if err != nil {
			return err
		}
		delivered++
		idle.Reset(s.limits.Stall)
	}
}

type receiveArgs struct {
	name     string
	runtime  *store.Runtime
	ctx      context.Context
	stream   grpc.BidiStreamingServer[pb.BulkRequestFrame, pb.BulkResponseFrame]
	session  *store.Session
	invalid  chan<- *pb.BulkResult
	ended    chan<- receiveEnd
	activity chan<- struct{}
}

func (s *Server) receive(args receiveArgs) {
	ctx, stream, session, invalid, ended, activity := args.ctx, args.stream, args.session, args.invalid, args.ended, args.activity
	var count uint64
	finish := func(err error) { end := receiveEnd{count: count, err: err}; ended <- end }
	for {
		select {
		case <-ctx.Done():
			finish(status.FromContextError(ctx.Err()).Err())
			return
		case <-s.draining:
			finish(status.Error(codes.Unavailable, "draining"))
			return
		default:
		}
		if err := s.admission.check(); err != nil {
			finish(err)
			return
		}
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			finish(nil)
			return
		}
		if err != nil {
			finish(err)
			return
		}
		select {
		case activity <- struct{}{}:
		default:
		}
		op := frame.GetOperation()
		if op == nil || op.Index != count || count == math.MaxUint64 {
			finish(status.Error(codes.InvalidArgument, "Bulk indexes must be consecutive"))
			return
		}
		count++
		f, operationErr := checkOperation(args.name, op)
		if f != nil {
			s.admission.rejections.WithLabelValues("operation").Inc()
		}
		if operationErr != nil {
			s.admission.rejections.WithLabelValues("operation").Inc()
			finish(operationErr)
			return
		}
		if err := s.admission.check(); err != nil {
			finish(err)
			return
		}
		var plan *execution.Plan
		if f == nil {
			plan, f = args.runtime.Prepare(op)
		}
		if f == nil {
			for {
				var changed <-chan struct{}
				_, f, changed = args.runtime.Submit(ctx, plan, session)
				if f == nil || f.Code != pb.FailureCode_RESOURCE_EXHAUSTED || args.runtime.Snapshot().Overloaded {
					break
				}
				select {
				case <-changed:
				case <-ctx.Done():
					finish(status.FromContextError(ctx.Err()).Err())
					return
				case <-s.draining:
					finish(status.Error(codes.Unavailable, "draining"))
					return
				}
			}
		}
		if f != nil {
			result := protocol.ResultError(op, pb.MutationOutcome_NOT_STARTED, f)
			select {
			case invalid <- result:
			case <-ctx.Done():
				finish(status.FromContextError(ctx.Err()).Err())
				return
			}
		}
		if args.runtime.Snapshot().Overloaded {
			finish(status.Error(codes.ResourceExhausted, "process overloaded"))
			return
		}
	}
}
func (s *Server) send(ctx context.Context, stream grpc.BidiStreamingServer[pb.BulkRequestFrame, pb.BulkResponseFrame], frame *pb.BulkResponseFrame) error {
	done := make(chan error, 1)
	go func() { done <- stream.Send(frame) }()
	timer := time.NewTimer(s.limits.Stall)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		if stream.Context().Err() == nil {
			s.abortPeer(stream.Context())
		}
		return status.FromContextError(ctx.Err()).Err()
	case <-timer.C:
		s.metrics.watchdogs.WithLabelValues("output").Inc()
		s.abortPeer(stream.Context())
		return status.Error(codes.DeadlineExceeded, "result send stalled")
	}
}
func (s *Server) Serving() <-chan struct{} { return s.serving }
func (s *Server) Serve(listener net.Listener) error {
	close(s.serving)
	bounded := &limitedListener{Listener: listener, slots: s.admission.connections, server: s}
	err := s.http.Serve(bounded)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func (s *Server) Shutdown(ctx context.Context) error {
	s.once.Do(func() {
		drain, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		s.admission.BeginDrain()
		stopped := make(chan struct{})
		go func() {
			if err := s.http.Shutdown(drain); err != nil {
				s.metrics.forced.WithLabelValues("drain").Inc()
				_ = s.http.Close()
			}
			// ServeHTTP has no gRPC Drain. net/http owns HTTP/2 graceful shutdown;
			// Stop joins the remaining bounded handlers after transport cancellation.
			s.grpc.Stop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-drain.Done():
			_ = s.http.Close()
			<-stopped
		}
	})
	return nil
}

type limitedListener struct {
	server *Server
	net.Listener
	slots chan struct{}
}
type limitedConn struct {
	server *Server
	net.Conn
	slots  chan struct{}
	closed atomic.Bool
}

func (l *limitedListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.slots <- struct{}{}:
			c := &limitedConn{Conn: conn, slots: l.slots, server: l.server}
			l.server.connections.Store(conn.RemoteAddr().String(), c)
			return c, nil
		default:
			l.server.admission.rejections.WithLabelValues("connections").Inc()
			_ = conn.Close()
		}
	}
}
func (c *limitedConn) Close() error { return c.close("") }
func (c *limitedConn) close(reason string) error {
	if c.closed.CompareAndSwap(false, true) {
		if reason != "" {
			c.server.metrics.forced.WithLabelValues(reason).Inc()
		}
		defer func() { <-c.slots }()
		c.server.connections.CompareAndDelete(c.RemoteAddr().String(), c)
		return c.Conn.Close()
	}
	return nil
}

// gRPC queues trailers behind already-buffered DATA. A peer that never reads can
// prevent even an error trailer/RST from progressing. Close that bounded connection
// on a send/input stall; co-resident RPCs lose their responses, never their evidence.
func (s *Server) abortPeer(ctx context.Context) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return
	}
	if conn, ok := s.connections.Load(p.Addr.String()); ok {
		_ = conn.(*limitedConn).close("abort")
	}
}
