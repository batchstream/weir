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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
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

func (*peerAdapter) PrepareCommand(uint64, *pb.Command) (*execution.Plan, *pb.Failure) {
	return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "record fixture")
}

func (a *peerAdapter) PrepareOperation(operation *pb.Operation) (*execution.Plan, *pb.Failure) {
	key := protocol.Resource(operation)
	if _, _, err := protocol.ParseResource("weir://" + a.name + "/" + key); err != nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid relative target")
	}
	plan := &execution.Plan{ID: operation.Index, Operation: operation, Key: key, BatchKey: "records", Bytes: proto.Size(operation) + protocol.EntryOverhead, ResultBytes: protocol.ResultOverhead, WorkingBytes: 1024}
	return plan, nil
}

func (a *peerAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) execution.Feedback {
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
			return execution.Neutral
		}
	}
	for _, plan := range plans {
		a.mu.Lock()
		result := &pb.Result{Index: plan.ID}
		if plan.Operation.GetRead() != nil {
			read := protocol.Missing()
			if document := a.documents[plan.Key]; document != nil {
				if plan.Results.Reserve(len(document.Data)) {
					read = protocol.ReadDocument(document)
				} else {
					read = protocol.ReadFailure(protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "record result budget exhausted"))
				}
			}
			result.Result = &pb.Result_Read{Read: read}
		} else {
			a.commands.Add(1)
			request := plan.Operation.GetMutate()
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
			result.Result = &pb.Result_Mutation{Mutation: protocol.Mutation(pb.MutationOutcome_APPLIED, failure)}
		}
		a.mu.Unlock()
		output := &execution.Output{Result: result}
		_ = emit(plan, output)
	}
	return execution.Healthy
}

func (a *peerAdapter) Close() error                                         { a.closed.Add(1); return nil }
func (*peerAdapter) ClosePlan(context.Context, *execution.Plan) *pb.Failure { return nil }

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
	if opts.limits.Sessions == 0 {
		opts.limits = DefaultLimits()
		opts.limits.Stall = time.Second
	}
	if opts.admission == nil {
		var err error
		opts.admission, err = NewAdmission(opts.limits)
		if err != nil {
			t.Fatal(err)
		}
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
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0), grpc.MaxCallRecvMsgSize(protocol.MaxBatchResponseBytes), grpc.MaxCallSendMsgSize(protocol.MaxBatchRequestBytes))}
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
	document := &pb.Document{MediaType: "application/octet-stream", Data: []byte(value)}
	action := &pb.MutateRequest_Put{Put: document}
	request := &pb.MutateRequest{Resource: testRequest().Resource, Action: action}
	return request
}

func waitPeerIdle(t *testing.T, s *Server) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for len(s.slots) != 0 || s.admission.wireBytes.Load() != 0 {
		if time.Now().After(until) {
			t.Fatal("RPC transport credits did not release", s.Snapshot(), s.admission.wireBytes.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

func routeRead(client pb.StoreServiceClient, ctx context.Context, read *pb.ReadRequest) (*pb.ReadResult, error) {
	request := &pb.ReadBatchRequest{StoreName: "records", Requests: []*pb.ReadRequest{read}}
	response, err := client.Read(ctx, request)
	if err != nil {
		return nil, err
	}
	return response.Results[0], nil
}

func routeMutate(client pb.StoreServiceClient, ctx context.Context, mutation *pb.MutateRequest) (*pb.MutationResult, error) {
	request := &pb.MutateBatchRequest{StoreName: "records", Requests: []*pb.MutateRequest{mutation}}
	response, err := client.Mutate(ctx, request)
	if err != nil {
		return nil, err
	}
	return response.Results[0], nil
}
