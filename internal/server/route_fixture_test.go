package server

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
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
	"google.golang.org/protobuf/encoding/protodelim"
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
}

func newPeerAdapter(name string) *peerAdapter {
	adapter := &peerAdapter{name: name, documents: make(map[string]*pb.Document), seen: make(chan context.Context, 64)}
	return adapter
}
func (a *peerAdapter) PrepareCall(id uint64, call *pb.Call) (*execution.Plan, *pb.Failure) {
	op := &pb.Operation{Index: id}
	if request := call.GetRead(); request != nil {
		op.Operation = &pb.Operation_Read{Read: request}
	} else if request := call.GetMutate(); request != nil {
		op.Operation = &pb.Operation_Mutate{Mutate: request}
	} else {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "record fixture")
	}
	key := protocol.Resource(op)
	_, segments, err := protocol.ParseResource("weir://" + a.name + "/" + key)
	if err != nil || len(segments) == 0 {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid relative target")
	}
	resultBytes := protocol.ResultOverhead
	if op.GetRead() != nil {
		resultBytes += protocol.MaxDocument
	}
	plan := &execution.Plan{ID: id, Call: call, Operation: op, Key: key, BatchKey: "records", Bytes: proto.Size(call) + protocol.EntryOverhead, ResultBytes: resultBytes, WorkingBytes: resultBytes}
	return plan, nil
}
func (a *peerAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) execution.Feedback {
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
		op := plan.Operation
		result := &pb.Result{Index: plan.ID}
		if op.GetRead() != nil {
			read := protocol.Missing()
			if document := a.documents[plan.Key]; document != nil {
				read = protocol.ReadDocument(document)
			}
			result.Result = &pb.Result_Read{Read: read}
		} else {
			a.commands.Add(1)
			request := op.GetMutate()
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
		event := &pb.Event{Version: 1, Value: &pb.Event_Result{Result: result}}
		_ = emit(plan, event)
	}
	return execution.Healthy
}
func (a *peerAdapter) Close() error                                           { a.closed.Add(1); return nil }
func (a *peerAdapter) ClosePlan(context.Context, *execution.Plan) *pb.Failure { return nil }
func peerLocal(t *testing.T, name string) (*peerAdapter, *store.Runtime) {
	t.Helper()
	adapter := newPeerAdapter(name)
	limits := store.DefaultLimits()
	runtime, err := store.New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		if adapter.closed.Load() != 1 {
			t.Error("adapter not closed once")
		}
	})
	return adapter, runtime
}

type peerServerOptions struct {
	observe   chan http.Header
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
	if opts.observe != nil {
		original := srv.http.Handler
		srv.http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case opts.observe <- r.Header.Clone():
			default:
			}
			original.ServeHTTP(w, r)
		})
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
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithStaticStreamWindowSize(65535), grpc.WithStaticConnWindowSize(65535), grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0), grpc.MaxCallRecvMsgSize(protocol.MaxResponse), grpc.MaxCallSendMsgSize(protocol.MaxFrame))}
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
	for len(s.slots) != 0 || s.Snapshot().Outstanding != 0 {
		if time.Now().After(until) {
			t.Fatal("route did not release", s.Snapshot())
		}
		time.Sleep(time.Millisecond)
	}
}
func routeRecord(client pb.StoreServiceClient, ctx context.Context, call *pb.Call) (*pb.Result, error) {
	data, err := proto.Marshal(call)
	if err != nil {
		return nil, err
	}
	stream, err := client.Execute(ctx)
	if err != nil {
		return nil, err
	}
	request := &pb.ExecuteRequest{RequestId: 1, StoreName: "records", CallPayload: data}
	if err := stream.Send(request); err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	var encoded bytes.Buffer
	ended := false
	for {
		response, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			if !ended {
				return nil, io.ErrUnexpectedEOF
			}
			break
		}
		if err := protocol.ValidateExecuteResponse(response); err != nil {
			return nil, err
		}
		if response.RequestId != 1 || ended {
			return nil, io.ErrUnexpectedEOF
		}
		if response.RequestComplete {
			ended = true
		} else {
			_, _ = encoded.Write(response.EventFragment)
		}
	}
	event := &pb.Event{}
	if err := protodelim.UnmarshalFrom(&encoded, event); err != nil {
		return nil, err
	}
	if encoded.Len() != 0 || event.GetResult() == nil {
		return nil, io.ErrUnexpectedEOF
	}
	return event.GetResult(), nil
}
func routeRead(client pb.StoreServiceClient, ctx context.Context, request *pb.ReadRequest) (*pb.ReadResult, error) {
	value := &pb.Call_Read{Read: request}
	call := &pb.Call{Version: 1, Operation: value}
	result, err := routeRecord(client, ctx, call)
	if err != nil {
		return nil, err
	}
	return result.GetRead(), nil
}
func routeMutate(client pb.StoreServiceClient, ctx context.Context, request *pb.MutateRequest) (*pb.MutationResult, error) {
	value := &pb.Call_Mutate{Mutate: request}
	call := &pb.Call{Version: 1, Operation: value}
	result, err := routeRecord(client, ctx, call)
	if err != nil {
		return nil, err
	}
	return result.GetMutation(), nil
}
