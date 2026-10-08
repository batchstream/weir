package server

import (
	"context"
	"sync"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
)

type rpcContextKey uint8

const rpcKey rpcContextKey = 0

// rpcState checks ingress before gRPC reads DATA, tracks RPC lifetime and
// cancels streams that stop making input progress.
type rpcState struct {
	mu       sync.Mutex
	server   *Server
	method   string
	cancel   context.CancelFunc
	input    *time.Timer
	lifetime func() bool
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
	allowed := false
	switch info.FullMethodName {
	case pb.StoreService_Execute_FullMethodName:
		allowed = !s.peer
	case pb.StoreService_ResolveStore_FullMethodName:
		allowed = !s.peer
	case peerpb.PeerDiscoveryService_SyncDirectory_FullMethodName:
		allowed = s.peer && s.directory != nil
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
	if err := s.admission.check(); err != nil {
		return nil, err
	}
	rpcContext, cancel := context.WithCancel(ingress)
	s.admission.activeRPCs.Add(1)
	state := &rpcState{server: s, method: methodLabel(info.FullMethodName), cancel: cancel}
	state.mu.Lock()
	state.input = time.AfterFunc(s.limits.Stall, func() {
		state.mu.Lock()
		defer state.mu.Unlock()
		if state.input == nil || state.finished || rpcContext.Err() != nil {
			return
		}
		state.input = nil
		s.metrics.watchdogs.WithLabelValues("open").Inc()
		state.cancel()
	})
	state.lifetime = context.AfterFunc(rpcContext, func() {
		state.cancelBeforeDispatch(rpcContext.Err())
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
// Once dispatched, Stats.End owns completion accounting.
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
	state.server.admission.activeRPCs.Add(-1)
	if state.input != nil {
		state.input.Stop()
	}
	if state.lifetime != nil {
		state.lifetime()
	}
	state.server.metrics.rpcs.WithLabelValues(state.method, statusLabel(err)).Inc()
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
		if value, ok := state.server.admission.responses.Load(value.Payload); ok {
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
