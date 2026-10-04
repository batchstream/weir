package server

import (
	"context"
	"sync"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"github.com/batchstream/weir/internal/directory"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
)

type rpcContextKey uint8

const rpcKey rpcContextKey = 0

// rpcState admits input before gRPC reads or decodes DATA. Handler completion
// releases that slot; result and encoded output have their own byte budgets.
type rpcState struct {
	mu       sync.Mutex
	server   *Server
	method   string
	slots    chan struct{}
	cancel   context.CancelFunc
	input    *time.Timer
	lifetime func() bool
	ticket   *store.Ticket
	finished bool
	started  bool
}

func (s *Server) admitRPC(ctx context.Context, info *tap.Info) (context.Context, error) {
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if remote, ok := peer.FromContext(ctx); ok {
		if connection, ok := s.connections.Load(remote.Addr.String()); ok {
			if timer := connection.(*limitedConn).opening.Load(); timer != nil {
				timer.Stop()
			}
		}
	}
	control := false
	allowed := false
	switch info.FullMethodName {
	case pb.StoreService_Read_FullMethodName, pb.StoreService_Mutate_FullMethodName, pb.StoreService_Execute_FullMethodName:
		allowed = !s.peer
	case pb.StoreService_ResolveStore_FullMethodName:
		allowed, control = !s.peer, true
	case peerpb.PeerDiscoveryService_SyncDirectory_FullMethodName:
		allowed, control = s.peer && s.directory != nil, true
	}
	if !allowed {
		s.admission.rejections.WithLabelValues("method").Inc()
		return nil, status.Error(codes.Unimplemented, "method is not served on this listener")
	}
	ingress, err := s.ingress(ctx)
	if err != nil {
		s.admission.rejections.WithLabelValues("ingress").Inc()
		return nil, err
	}
	slots := s.slots
	lifetime := s.limits.RequestLifetime
	if control {
		slots = s.control
		lifetime = min(directory.SyncTimeout, s.limits.Stall)
	}
	if err := s.enterSlots(slots); err != nil {
		return nil, err
	}
	rpcContext, cancel := context.WithTimeout(ingress, lifetime)
	state := &rpcState{server: s, method: methodLabel(info.FullMethodName), slots: slots, cancel: cancel}
	state.mu.Lock()
	state.input = time.AfterFunc(s.limits.Stall, func() {
		s.metrics.watchdogs.WithLabelValues("open").Inc()
		s.abortPeer(rpcContext)
	})
	state.lifetime = context.AfterFunc(rpcContext, func() {
		state.cancelBeforeDispatch(rpcContext.Err())
		if rpcContext.Err() == context.DeadlineExceeded {
			s.abortPeer(rpcContext)
		}
	})
	state.mu.Unlock()
	tagged := context.WithValue(rpcContext, rpcKey, state)
	if rpcContext.Err() != nil {
		state.finish(rpcContext.Err())
		return nil, status.FromContextError(rpcContext.Err()).Err()
	}
	return tagged, nil
}

// The transport may abort an already expired tap before TagRPC/Stats.End.
// Once dispatched, cancellation cannot release a still-running handler's slot.
func (state *rpcState) cancelBeforeDispatch(err error) {
	state.mu.Lock()
	if state.started || state.finished {
		state.mu.Unlock()
		return
	}
	state.finishLocked(err)
	state.mu.Unlock()
}

func (state *rpcState) decoded() {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.input != nil {
		state.input.Stop()
		state.input = nil
	}
}

func (state *rpcState) retain(ticket *store.Ticket) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.ticket = ticket
}

func (state *rpcState) releaseAdmission() {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.releaseAdmissionLocked()
}

func (state *rpcState) releaseAdmissionLocked() {
	if state.slots != nil {
		<-state.slots
		state.slots = nil
	}
}

func unaryRPC(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if state, ok := ctx.Value(rpcKey).(*rpcState); ok {
		defer state.releaseAdmission()
	}
	return handler(ctx, request)
}

func streamRPC(server any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if state, ok := stream.Context().Value(rpcKey).(*rpcState); ok {
		defer state.releaseAdmission()
	}
	return handler(server, stream)
}

func (state *rpcState) finish(err error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.finishLocked(err)
}

func (state *rpcState) finishLocked(err error) {
	if state.finished {
		return
	}
	state.finished = true
	if state.input != nil {
		state.input.Stop()
	}
	if state.lifetime != nil {
		state.lifetime()
	}
	if state.ticket != nil {
		state.ticket.Ack()
	}
	state.server.metrics.rpcs.WithLabelValues(state.method, statusLabel(err)).Inc()
	state.releaseAdmissionLocked()
	state.cancel()
}

type transportStats struct{}

func (transportStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	if state, ok := ctx.Value(rpcKey).(*rpcState); ok {
		state.mu.Lock()
		state.started = true
		state.mu.Unlock()
	}
	return ctx
}

func (transportStats) HandleRPC(ctx context.Context, event stats.RPCStats) {
	state, ok := ctx.Value(rpcKey).(*rpcState)
	if !ok {
		return
	}
	switch value := event.(type) {
	case *stats.OutPayload:
		if value, ok := state.server.admission.responses.LoadAndDelete(value.Payload); ok {
			value.(*responseBufferOwner).watch(ctx, state.server)
		}
	case *stats.InPayload:
		state.decoded()
	case *stats.End:
		state.finish(value.Error)
	}
}

func (transportStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (transportStats) HandleConn(context.Context, stats.ConnStats) {}
