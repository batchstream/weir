// Package server provides bounded, loopback-only milestone gRPC transport.
package server

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type Limits struct {
	Connections, Sessions              int
	UnaryLifetime, BulkLifetime, Stall time.Duration
}

func DefaultLimits() Limits {
	l := Limits{Connections: 16, Sessions: 16, UnaryLifetime: 30 * time.Second, BulkLifetime: 15 * time.Minute, Stall: 30 * time.Second}
	return l
}

type Server struct {
	pb.UnimplementedWeirServer
	Runtime     *store.Runtime
	Store       string
	limits      Limits
	slots       chan struct{}
	grpc        *grpc.Server
	draining    chan struct{}
	once        sync.Once
	connections sync.Map
}

func New(runtime *store.Runtime, name string, l Limits) (*Server, error) {
	if l.Connections < 1 || l.Connections > 64 || l.Sessions < 1 || l.Sessions > 64 || l.UnaryLifetime <= 0 || l.BulkLifetime <= 0 || l.Stall <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid transport bounds")
	}
	s := &Server{Runtime: runtime, Store: name, limits: l, slots: make(chan struct{}, l.Sessions), draining: make(chan struct{})}
	keep := keepalive.ServerParameters{MaxConnectionIdle: time.Minute, MaxConnectionAge: 20 * time.Minute, MaxConnectionAgeGrace: time.Second, Time: time.Minute, Timeout: 5 * time.Second}
	s.grpc = grpc.NewServer(grpc.MaxRecvMsgSize(protocol.MaxFrame), grpc.MaxSendMsgSize(protocol.MaxFrame), grpc.MaxConcurrentStreams(8), grpc.MaxHeaderListSize(16<<10), grpc.InitialWindowSize(64<<10), grpc.InitialConnWindowSize(256<<10), grpc.WriteBufferSize(32<<10), grpc.ReadBufferSize(32<<10), grpc.ConnectionTimeout(5*time.Second), grpc.KeepaliveParams(keep), grpc.UnaryInterceptor(s.unary), grpc.StreamInterceptor(s.stream))
	pb.RegisterWeirServer(s.grpc, s)
	return s, nil
}
func (s *Server) enter() error {
	select {
	case <-s.draining:
		return status.Error(codes.Unavailable, "draining")
	default:
	}
	if s.Runtime.Snapshot().Overloaded {
		return status.Error(codes.ResourceExhausted, "process overloaded")
	}
	select {
	case s.slots <- struct{}{}:
		return nil
	default:
		return status.Error(codes.ResourceExhausted, "session limit")
	}
}
func (s *Server) unary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}
	defer func() { <-s.slots }()
	ctx, cancel := context.WithTimeout(ctx, s.limits.UnaryLifetime)
	defer cancel()
	return handler(ctx, req)
}
func (s *Server) stream(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := s.enter(); err != nil {
		return err
	}
	defer func() { <-s.slots }()
	return handler(srv, ss)
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
	plan, f := s.Runtime.Prepare(op)
	if f != nil {
		return protocol.ResultError(op, pb.MutationOutcome_NOT_STARTED, f), nil
	}
	ticket, f, _ := s.Runtime.Submit(ctx, plan, nil)
	if f != nil {
		return protocol.ResultError(op, pb.MutationOutcome_NOT_STARTED, f), nil
	}
	result, err := ticket.Wait(ctx)
	if err != nil {
		return nil, status.FromContextError(err).Err()
	}
	defer ticket.Ack()
	return result, nil
}

type receiveEnd struct {
	count uint64
	err   error
}

func (s *Server) Bulk(stream grpc.BidiStreamingServer[pb.BulkRequestFrame, pb.BulkResponseFrame]) error {
	ctx, cancel := context.WithTimeout(stream.Context(), s.limits.BulkLifetime)
	defer cancel()
	session := s.Runtime.NewSession()
	defer session.Close()
	invalid := make(chan *pb.BulkResult, 1)
	ended := make(chan receiveEnd, 1)
	activity := make(chan struct{}, 1)
	args := receiveArgs{ctx: ctx, stream: stream, session: session, invalid: invalid, ended: ended, activity: activity}
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
	first, err := stream.Recv()
	if err != nil {
		finish(status.Error(codes.InvalidArgument, "Bulk requires Open"))
		return
	}
	if first.GetOpen() == nil || first.GetOpen().Store != "weir://"+s.Store {
		finish(status.Error(codes.InvalidArgument, "invalid Bulk Open"))
		return
	}
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
		plan, f := s.Runtime.Prepare(op)
		if f == nil {
			for {
				var changed <-chan struct{}
				_, f, changed = s.Runtime.Submit(ctx, plan, session)
				if f == nil || f.Code != pb.FailureCode_RESOURCE_EXHAUSTED || s.Runtime.Snapshot().Overloaded {
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
		if s.Runtime.Snapshot().Overloaded {
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
		s.abortPeer(stream.Context())
		return status.Error(codes.DeadlineExceeded, "result send stalled")
	}
}
func (s *Server) Serve(listener net.Listener) error {
	bounded := &limitedListener{Listener: listener, slots: make(chan struct{}, s.limits.Connections), server: s}
	return s.grpc.Serve(bounded)
}
func (s *Server) Shutdown(ctx context.Context) error {
	s.once.Do(func() { close(s.draining); s.Runtime.BeginDrain() })
	stopped := make(chan struct{})
	go func() { s.grpc.GracefulStop(); close(stopped) }()
	err := s.Runtime.Close(ctx)
	select {
	case <-stopped:
	case <-ctx.Done():
		s.grpc.Stop()
		<-stopped
	}
	return err
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
			_ = conn.Close()
		}
	}
}
func (c *limitedConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
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
		_ = conn.(*limitedConn).Close()
	}
}
