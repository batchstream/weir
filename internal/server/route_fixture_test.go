package server

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/directory"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type peerAdapter struct {
	name             string
	mu               sync.Mutex
	documents        map[string]*pb.Document
	commands, closed atomic.Int32
	block            <-chan struct{}
	seen             chan context.Context
	ackFailureKey    string
	ackFailure       *pb.Failure
	batchSizes       []int
}

func newPeerAdapter(name string) *peerAdapter {
	adapter := &peerAdapter{name: name, documents: make(map[string]*pb.Document), seen: make(chan context.Context, 64)}
	return adapter
}

func (*peerAdapter) PrepareNative(context.Context, *pb.Command) (*execution.Plan, *pb.Failure) {
	return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "record fixture")
}

func (*peerAdapter) ExecuteNative(context.Context, *execution.Plan, execution.Emit) *pb.NativeEnd {
	failure := protocol.Fail(pb.FailureCode_UNSUPPORTED, "record fixture")
	return protocol.NativeFailure(false, failure)
}

func (a *peerAdapter) PrepareRecord(ctx context.Context, record *execution.Record) (*execution.Plan, *pb.Failure) {
	plan := &execution.Plan{Command: record.Command(), Key: record.Key(), BatchKey: "records", Bytes: record.Bytes()}
	return plan, nil
}

func (a *peerAdapter) ExecuteRecords(ctx context.Context, plans []*execution.Plan) []*pb.Event {
	events := make([]*pb.Event, len(plans))
	a.mu.Lock()
	a.batchSizes = append(a.batchSizes, len(plans))
	a.mu.Unlock()
	select {
	case a.seen <- ctx:
	default:
	}
	if a.block != nil {
		select {
		case <-a.block:
		case <-ctx.Done():
			for i, plan := range plans {
				events[i] = execution.FailedEvent(plan.Command, pb.MutationOutcome_UNKNOWN, protocol.ContextFailure(ctx))
			}
			return events
		}
	}
	for i, plan := range plans {
		a.mu.Lock()
		event := &pb.Event{}
		if plan.Command.GetRead() != nil {
			read := protocol.Missing()
			if document := a.documents[plan.Key]; document != nil {
				read = protocol.ReadDocument(document)
			}
			event.Value = &pb.Event_ReadResult{ReadResult: read}
		} else {
			a.commands.Add(1)
			request := plan.Command.GetMutate()
			document := request.GetPut()
			if document == nil {
				document = request.GetCreate()
			}
			if document == nil {
				document = request.GetReplace()
			}
			if document == nil {
				delete(a.documents, plan.Key)
			} else {
				a.documents[plan.Key] = document
			}
			var failure *pb.Failure
			if plan.Key == a.ackFailureKey {
				failure = a.ackFailure
			}
			event.Value = &pb.Event_MutationResult{MutationResult: protocol.Mutation(pb.MutationOutcome_APPLIED, failure)}
		}
		a.mu.Unlock()
		events[i] = event
	}
	return events
}

func (a *peerAdapter) Close(context.Context) error { a.closed.Add(1); return nil }

func peerLocal(t *testing.T, name string) (*peerAdapter, *store.Runtime) {
	t.Helper()
	adapter := newPeerAdapter(name)
	limits := store.DefaultLimits()
	local, err := store.New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := local.Close(ctx); err != nil {
			t.Error(err)
		}
		if adapter.closed.Load() != 1 {
			t.Error("adapter not closed once")
		}
	})
	return adapter, local
}

type peerServerOptions struct {
	stores    map[string]*store.Runtime
	directory *directory.Directory
	peer      bool
	limits    Limits
	admission *Admission
	listener  net.Listener
}

func startPeerServer(t *testing.T, opts peerServerOptions) (*Server, string) {
	t.Helper()
	if opts.limits.Stall == 0 {
		opts.limits = DefaultLimits()
		opts.limits.Stall = time.Second
	}
	if opts.admission == nil {
		opts.admission = NewAdmission()
	}
	cfg := Config{Stores: opts.stores, Directory: opts.directory, Limits: opts.limits, Admission: opts.admission, Peer: opts.peer}
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	listener := opts.listener
	if listener == nil {
		listener, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = listener.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return srv, listener.Addr().String()
}

func peerClient(t *testing.T, address string) (*grpc.ClientConn, pb.StoreServiceClient) {
	t.Helper()
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0), grpc.MaxCallRecvMsgSize(protocol.MaxTransportMessageBytes), grpc.MaxCallSendMsgSize(protocol.MaxTransportMessageBytes))}
	conn, err := grpc.NewClient("passthrough:///"+address, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, pb.NewStoreServiceClient(conn)
}

func testRequest() *pb.ReadRequest {
	request := &pb.ReadRequest{Resource: "data/s:key"}
	return request
}

func testMutation(value string) *pb.MutateRequest {
	document := &pb.Document{ContentType: "application/octet-stream", Data: []byte(value)}
	action := &pb.MutateRequest_Put{Put: document}
	request := &pb.MutateRequest{Resource: testRequest().Resource, Action: action}
	return request
}

func waitPeerIdle(t *testing.T, s *Server) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for s.admission.activeRPCs.Load() != 0 {
		if time.Now().After(until) {
			t.Fatal("RPC handlers did not return", s.admission.activeRPCs.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

func routeRead(client pb.StoreServiceClient, ctx context.Context, read *pb.ReadRequest) (*pb.ReadResult, error) {
	request := &pb.ExecuteRequest{StoreName: "records",
		Index: 1, Command: &pb.Command{
			Operation: &pb.Command_Read{Read: read}}}

	response, err := testutil.ReadRecords(ctx, client, request.StoreName, []*pb.ReadRequest{request.Command.GetRead()})
	if err != nil {
		return nil, err
	}
	return response[0], nil
}

func routeMutate(client pb.StoreServiceClient, ctx context.Context, mutation *pb.MutateRequest) (*pb.MutationResult, error) {
	request := &pb.ExecuteRequest{StoreName: "records",
		Index: 1, Command: &pb.Command{
			Operation: &pb.Command_Mutate{Mutate: mutation}}}

	response, err := testutil.MutateRecords(ctx, client, request.StoreName, []*pb.MutateRequest{request.Command.GetMutate()})
	if err != nil {
		return nil, err
	}
	return response[0], nil
}
