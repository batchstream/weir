package server

import (
	"context"
	"github.com/batchstream/weir-protocol/api/protocol"
	"github.com/batchstream/weir/internal/execution"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"github.com/batchstream/weir/internal/directory"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/tap"
)

func TestTapRejectsExpiredAndReclaimsUndispatchedRPC(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, _ := startPeerServer(t, opts)
	info := &tap.Info{FullMethodName: pb.StoreService_Read_FullMethodName}
	for range 50 {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := server.admitRPC(ctx, info); err == nil {
			t.Fatal("expired tap admitted")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	accepted, err := server.admitRPC(ctx, info)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	waitPeerIdle(t, server)
	if accepted.Err() == nil {
		t.Fatal("RPC cancellation missing")
	}
}

func TestDispatchedRPCAdmissionRemainsWhileHandlerWorks(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, _ := startPeerServer(t, opts)
	ctx, cancel := context.WithCancel(t.Context())
	info := &tap.Info{FullMethodName: pb.StoreService_Read_FullMethodName}
	accepted, err := server.admitRPC(ctx, info)
	if err != nil {
		t.Fatal(err)
	}
	tags := &stats.RPCTagInfo{FullMethodName: info.FullMethodName}
	handler := transportStats{}
	handler.TagRPC(accepted, tags)
	cancel()
	state := accepted.Value(rpcKey).(*rpcState)
	state.cancelBeforeDispatch(context.Canceled)
	if len(server.slots) != 1 {
		t.Fatal("active handler admission released on caller cancellation")
	}
	end := &stats.End{Error: context.Canceled, EndTime: time.Now()}
	handler.HandleRPC(accepted, end)
	waitPeerIdle(t, server)
}

type admissionStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (stream *admissionStream) Context() context.Context { return stream.ctx }

func TestHandlerCompletionReleasesAdmissionBeforeTransportEnd(t *testing.T) {
	methods := []string{pb.StoreService_Read_FullMethodName, pb.StoreService_Execute_FullMethodName, pb.StoreService_ResolveStore_FullMethodName, peerpb.PeerDiscoveryService_SyncDirectory_FullMethodName}
	for _, method := range methods {
		t.Run(methodLabel(method), func(t *testing.T) {
			_, local := peerLocal(t, "records")
			limits := DefaultLimits()
			limits.Sessions = 1
			opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}, limits: limits}
			if method == peerpb.PeerDiscoveryService_SyncDirectory_FullMethodName {
				config := directory.Config{Group: "control", PeerAddress: "127.0.0.1:1", Targets: []string{"127.0.0.1:2"}, Stores: []string{"records"}}
				var err error
				opts.directory, err = directory.New(config)
				if err != nil {
					t.Fatal(err)
				}
				opts.peer = true
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					if err := opts.directory.Close(ctx); err != nil {
						t.Error(err)
					}
				})
			}
			server, _ := startPeerServer(t, opts)
			info := &tap.Info{FullMethodName: method}
			slots := server.slots
			if method == pb.StoreService_ResolveStore_FullMethodName || opts.peer {
				slots = server.control
			}
			var accepted []context.Context
			statistics := transportStats{}
			for range cap(slots) {
				ctx, err := server.admitRPC(t.Context(), info)
				if err != nil {
					t.Fatal(err)
				}
				tags := &stats.RPCTagInfo{FullMethodName: method}
				statistics.TagRPC(ctx, tags)
				accepted = append(accepted, ctx)
			}
			end := &stats.End{}
			defer func() {
				for _, ctx := range accepted {
					statistics.HandleRPC(ctx, end)
				}
			}()
			if _, err := server.admitRPC(t.Context(), info); status.Code(err) != codes.ResourceExhausted {
				t.Fatal("input admission no longer enforces active handlers", err)
			}
			ctx := accepted[0]
			payload := &stats.InPayload{}
			statistics.HandleRPC(ctx, payload)
			if method == pb.StoreService_Execute_FullMethodName {
				stream := &admissionStream{ctx: ctx}
				streamInfo := &grpc.StreamServerInfo{FullMethod: method, IsServerStream: true}
				err := streamRPC(server, stream, streamInfo, func(any, grpc.ServerStream) error { return nil })
				if err != nil {
					t.Fatal(err)
				}
			} else {
				unaryInfo := &grpc.UnaryServerInfo{Server: server, FullMethod: method}
				response := &pb.ReadBatchResponse{}
				_, err := unaryRPC(ctx, nil, unaryInfo, func(context.Context, any) (any, error) { return response, nil })
				if err != nil {
					t.Fatal(err)
				}
			}
			if ctx.Err() != nil || len(slots) != cap(slots)-1 {
				t.Fatal("handler did not release only its admission slot", ctx.Err(), len(slots))
			}
			codec := &responseCodec{admission: server.admission}
			response := &pb.ReadBatchResponse{}
			encoded, err := codec.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { encoded.Free() }()
			next, err := server.admitRPC(t.Context(), info)
			if err != nil {
				t.Fatal("next input rejected after prior handler completed", err)
			}
			accepted = append(accepted, next)
			statistics.HandleRPC(ctx, end)
			if server.admission.wireBytes.Load() != 1025 || len(slots) != cap(slots) {
				t.Fatal("transport End released output bytes or another handler's slot")
			}
			encoded.Free()
			encoded = nil
			if server.admission.wireBytes.Load() != 0 {
				t.Fatal("encoded ownership did not return output bytes")
			}
		})
	}
}

func TestUnaryBatchResultsReleaseAtTransportEnd(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, _ := startPeerServer(t, opts)
	info := &tap.Info{FullMethodName: pb.StoreService_Read_FullMethodName}
	ctx, err := server.admitRPC(t.Context(), info)
	if err != nil {
		t.Fatal(err)
	}
	statistics := transportStats{}
	tags := &stats.RPCTagInfo{FullMethodName: info.FullMethodName}
	statistics.TagRPC(ctx, tags)
	payload := &stats.InPayload{}
	statistics.HandleRPC(ctx, payload)
	end := &stats.End{}
	defer statistics.HandleRPC(ctx, end)

	request := &pb.ReadBatchRequest{StoreName: "records", Requests: []*pb.ReadRequest{testRequest()}}
	unaryInfo := &grpc.UnaryServerInfo{Server: server, FullMethod: info.FullMethodName}
	response, err := unaryRPC(ctx, request, unaryInfo, func(ctx context.Context, request any) (any, error) {
		return server.Read(ctx, request.(*pb.ReadBatchRequest))
	})
	if err != nil {
		t.Fatal(err)
	}
	codec := &responseCodec{admission: server.admission}
	encoded, err := codec.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	encoded.Free()
	if len(server.slots) != 0 || server.admission.wireBytes.Load() != 0 {
		t.Fatal("handler or encoded response retained transport credits")
	}
	// Returning the response and its encoded buffer precedes stats.End. Neither
	// transport credit is evidence that the Store result has been acknowledged.
	snapshot := local.Snapshot()
	if snapshot.Pending != 0 || snapshot.Active != 0 || snapshot.WorkingBytes != 0 || snapshot.Publishers != 0 || snapshot.Ready != 1 || snapshot.Retained != 1 || snapshot.ResultBytes != execution.ResultOverheadBytes {
		t.Fatal("completed unary batch lost result ownership before transport End", snapshot)
	}
	statistics.HandleRPC(ctx, end)
	waitPeerIdle(t, server)
	snapshot = local.Snapshot()
	if snapshot.PendingBytes != 0 || snapshot.Retained != 0 || snapshot.ResultBytes != 0 {
		t.Fatal("transport End did not acknowledge the retained unary batch", snapshot)
	}
}

func TestNativeBatchDecodeLimitFailsBeforeBackendWork(t *testing.T) {
	adapter, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	count := protocol.MaxBatchResponseBytes/execution.ResultOverheadBytes + 1
	read := &pb.ReadRequest{Resource: "records/s:key"}
	requests := make([]*pb.ReadRequest, count)
	for i := range requests {
		requests[i] = read
	}
	request := &pb.ReadBatchRequest{StoreName: "records", Requests: requests}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	response, err := client.Read(ctx, request)
	// Native gRPC wraps codec errors as Internal before the unary interceptor.
	if response != nil || status.Code(err) != codes.Internal || !strings.Contains(status.Convert(err).Message(), "request metadata exceeds decode budget") {
		t.Fatal("native decode status changed", response, err)
	}
	if len(adapter.seen) != 0 || adapter.commands.Load() != 0 || local.Snapshot().Retained != 0 {
		t.Fatal("over-budget raw request reached backend")
	}
	waitPeerIdle(t, server)
}
