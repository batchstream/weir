package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// This adapter exercises the real Runtime/transport boundary without a database.
// Real database response-loss evidence lives in the integration suite.
type peerAdapter struct {
	name        string
	mu          sync.Mutex
	documents   map[string]*pb.Document
	commands    atomic.Int32
	closed      atomic.Int32
	scans       atomic.Int32
	cleanups    atomic.Int32
	native      atomic.Int32
	block       <-chan struct{}
	seen        chan context.Context
	scanFailure bool
}

func newPeerAdapter(name string) *peerAdapter {
	a := &peerAdapter{name: name, documents: make(map[string]*pb.Document), seen: make(chan context.Context, 64)}
	return a
}
func (a *peerAdapter) Prepare(op *pb.BulkOperation) (*execution.Plan, *pb.Failure) {
	if failure := protocol.Validate(op, a.name); failure != nil {
		return nil, failure
	}
	resultBytes := protocol.ResultOverhead
	if op.GetRead() != nil {
		resultBytes += protocol.MaxDocument
	}
	plan := &execution.Plan{Operation: op, Key: protocol.Resource(op), Bytes: protocol.EntryOverhead + protocol.MaxDocument, ResultBytes: resultBytes}
	return plan, nil
}
func (a *peerAdapter) Execute(ctx context.Context, plans []*execution.Plan) ([]*pb.BulkResult, execution.Feedback) {
	select {
	case a.seen <- ctx:
	default:
	}
	if a.block != nil {
		select {
		case <-a.block:
		case <-ctx.Done():
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	results := make([]*pb.BulkResult, len(plans))
	for i, plan := range plans {
		op := plan.Operation
		result := &pb.BulkResult{Index: op.Index}
		if op.GetRead() != nil {
			read := protocol.Missing()
			if doc := a.documents[plan.Key]; doc != nil {
				read = protocol.ReadDocument(doc)
			}
			result.Result = &pb.BulkResult_Read{Read: read}
		} else {
			a.commands.Add(1)
			request := op.GetMutate()
			doc := request.GetPut()
			if doc == nil {
				doc = request.GetCreate()
			}
			if doc == nil {
				doc = request.GetReplace()
			}
			if doc == nil {
				delete(a.documents, plan.Key)
			} else {
				a.documents[plan.Key] = doc
			}
			result.Result = &pb.BulkResult_Mutation{Mutation: protocol.Mutation(pb.MutationOutcome_APPLIED, nil)}
		}
		results[i] = result
	}
	return results, execution.Healthy
}
func (a *peerAdapter) PrepareScan(req *pb.ScanRequest) (*execution.Plan, *pb.Failure) {
	if failure := protocol.ValidateScan(req, a.name); failure != nil {
		return nil, failure
	}
	plan := &execution.Plan{Scan: true, Bytes: 512, PageBytes: 1 << 20, ResultBytes: 1 << 20, Backend: new(int)}
	return plan, nil
}
func (a *peerAdapter) FetchScan(_ context.Context, plan *execution.Plan) (*execution.ScanPage, execution.Feedback) {
	a.scans.Add(1)
	cursor := plan.Backend.(*int)
	*cursor++
	page := &execution.ScanPage{Exhausted: *cursor == 3}
	if a.scanFailure && *cursor == 2 {
		page.Failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "page failed")
		return page, execution.Neutral
	}
	for range 2 {
		doc := &pb.Document{MediaType: "application/octet-stream", Data: bytes.Repeat([]byte{byte(*cursor)}, 200<<10)}
		page.Documents = append(page.Documents, doc)
	}
	return page, execution.Healthy
}
func (a *peerAdapter) CloseScan(context.Context, *execution.Plan) *pb.Failure {
	a.cleanups.Add(1)
	return nil
}
func (a *peerAdapter) PrepareNative(open *pb.NativeOpen) (*execution.Plan, *pb.Failure) {
	if failure := protocol.ValidateNative(open, a.name); failure != nil {
		return nil, failure
	}
	plan := &execution.Plan{Native: true, Bytes: 512, ResultBytes: protocol.NativeChunk + 512, PageBytes: 1 << 20, Backend: open}
	return plan, nil
}
func (a *peerAdapter) ExecuteNative(_ context.Context, plan *execution.Plan, exchange *execution.NativeExchange) (*pb.NativeEnd, execution.Feedback) {
	a.native.Add(1)
	open := plan.Backend.(*pb.NativeOpen)
	head := &pb.NativeHead{BodyMediaType: "application/octet-stream"}
	if err := exchange.Sink.Head(head); err != nil {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "head failed")), execution.Neutral
	}
	var body []byte
	if string(open.Descriptor_.Data) == "early" {
		body = []byte("native business error")
	} else {
		var err error
		body, err = io.ReadAll(io.LimitReader(exchange.Source, 8<<20))
		if err != nil {
			return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "input failed")), execution.Neutral
		}
	}
	for len(body) > 0 {
		count := min(len(body), protocol.NativeChunk)
		if err := exchange.Sink.Chunk(body[:count]); err != nil {
			return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "output failed")), execution.Neutral
		}
		body = body[count:]
	}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	return end, execution.Neutral
}
func (a *peerAdapter) Close() error { a.closed.Add(1); return nil }
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
			t.Error("adapter close count", adapter.closed.Load())
		}
	})
	return adapter, runtime
}

type peerServerOptions struct {
	observe   chan http.Header
	routes    map[string]Service
	peer      bool
	limits    Limits
	budget    int
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
	cfg := Config{Routes: opts.routes, Limits: opts.limits, Admission: opts.admission, InitialForwards: opts.budget}
	cfg.Peer = opts.peer
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
func peerClient(t *testing.T, address string) (*grpc.ClientConn, pb.WeirClient) {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithStaticStreamWindowSize(65535), grpc.WithStaticConnWindowSize(65535), grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, pb.NewWeirClient(conn)
}
func testRemote(t *testing.T, address string) *RemoteWeir {
	t.Helper()
	cfg := RemoteConfig{Endpoints: []string{address}, Relays: 8}
	remote, err := NewRemote(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := remote.Close(); err != nil {
			t.Error(err)
		}
		_ = remote.Close()
	})
	return remote
}
func testRequest() *pb.ReadRequest {
	req := &pb.ReadRequest{Resource: "weir://records/data/s:key"}
	return req
}
func testMutation(value string) *pb.MutateRequest {
	doc := &pb.Document{MediaType: "application/octet-stream", Data: []byte(value)}
	put := &pb.MutateRequest_Put{Put: doc}
	req := &pb.MutateRequest{Resource: testRequest().Resource, Action: put}
	return req
}
func peerContext(hops ...string) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	md := metadata.MD{HopMetadata: hops}
	return metadata.NewOutgoingContext(ctx, md), cancel
}
func TestPeerPlaintextIngressHopAndValidation(t *testing.T) {
	_, runtime := peerLocal(t, "records")
	service := Service{LocalStore: runtime}
	routes := map[string]Service{"records": service}

	opts := peerServerOptions{routes: routes, peer: true}
	_, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	for _, hops := range [][]string{nil, {""}, {"00"}, {"01"}, {"-1"}, {" 1"}, {"1 "}, {"9"}, {"1", "1"}} {
		ctx, cancel := peerContext(hops...)
		_, err := client.Read(ctx, testRequest())
		cancel()
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("hop %q: %v", hops, err)
		}
	}
	ctx, cancel := peerContext("0")
	defer cancel()
	if result, err := client.Read(ctx, testRequest()); err != nil || result.GetMissing() == nil {
		t.Fatal(result, err)
	}
	if result, err := client.Mutate(ctx, testMutation("x")); err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal(result, err)
	}
	other := &pb.ReadRequest{Resource: "weir://other/data/s:key"}
	if result, err := client.Read(ctx, other); err != nil || result.GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT {
		t.Fatal(result, err)
	}

	public := peerServerOptions{routes: routes, budget: 4}
	_, publicAddress := startPeerServer(t, public)
	_, publicClient := peerClient(t, publicAddress)
	if _, err := publicClient.Read(ctx, testRequest()); status.Code(err) != codes.InvalidArgument {
		t.Fatal("public spoof", err)
	}
}

type chain struct {
	headers chan http.Header
	client  pb.WeirClient
	conn    *grpc.ClientConn
	adapter *peerAdapter
	servers []*Server
	remotes []*RemoteWeir
	runtime *store.Runtime
}

func newChain(t *testing.T, hops int) chain {
	limits := DefaultLimits()
	limits.Stall = time.Second
	return newChainWithLimits(t, hops, limits)
}
func newChainWithLimits(t *testing.T, hops int, limits Limits) chain {
	t.Helper()
	adapter, runtime := peerLocal(t, "records")
	local := Service{LocalStore: runtime}
	headers := make(chan http.Header, 64)
	opts := peerServerOptions{routes: map[string]Service{"records": local}, budget: 4, observe: headers, limits: limits}
	if hops > 0 {
		opts.peer = true
	}
	srv, address := startPeerServer(t, opts)
	result := chain{headers: headers, adapter: adapter, runtime: runtime, servers: []*Server{srv}}
	for i := 0; i < hops; i++ {
		remote := testRemote(t, address)
		service := Service{RemoteWeir: remote}
		opts := peerServerOptions{routes: map[string]Service{"records": service}, budget: 4, limits: limits}
		if i < hops-1 {
			opts.peer = true
		}
		srv, address = startPeerServer(t, opts)
		result.servers = append(result.servers, srv)
		result.remotes = append(result.remotes, remote)
	}
	result.conn, result.client = peerClient(t, address)
	return result
}
func TestPeerFiveRPCsAndBoundedBulkCorrelation(t *testing.T) {
	for _, hops := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(hops), func(t *testing.T) {
			f := newChain(t, hops)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			payload := string(bytes.Repeat([]byte{0, 255, 37}, 60000))
			if result, err := f.client.Mutate(ctx, testMutation(payload)); err != nil || result.Outcome != pb.MutationOutcome_APPLIED {
				t.Fatal(result, err)
			}
			if result, err := f.client.Read(ctx, testRequest()); err != nil || string(result.GetDocument().GetData()) != payload {
				t.Fatal("opaque read", err)
			}
			stream, err := f.client.Bulk(ctx)
			if err != nil {
				t.Fatal(err)
			}
			open := &pb.BulkOpen{Store: "weir://records"}
			variant := &pb.BulkRequestFrame_Open{Open: open}
			frame := &pb.BulkRequestFrame{Frame: variant}
			if err := stream.Send(frame); err != nil {
				t.Fatal(err)
			}
			sent := make(chan error, 1)
			go func() {
				for i := 0; i < 41; i++ {
					op := &pb.BulkOperation{Index: uint64(i)}
					if i%3 == 0 {
						op.Operation = &pb.BulkOperation_Mutate{Mutate: testMutation(fmt.Sprint(i))}
					} else if i%3 == 1 {
						op.Operation = &pb.BulkOperation_Read{Read: testRequest()}
					} else {
						wrong := &pb.ReadRequest{Resource: "weir://other/data/s:key"}
						op.Operation = &pb.BulkOperation_Read{Read: wrong}
					}
					variant := &pb.BulkRequestFrame_Operation{Operation: op}
					frame := &pb.BulkRequestFrame{Frame: variant}
					if err := stream.Send(frame); err != nil {
						sent <- err
						return
					}
				}
				sent <- stream.CloseSend()
			}()
			seen := make(map[uint64]bool)
			for {
				frame, err := stream.Recv()
				if err != nil {
					t.Fatal(err)
				}
				if end := frame.GetEnd(); end != nil {
					if end.ResultCount != 41 || end.ReceivedCount != 41 || len(seen) != 41 {
						t.Fatal(end, len(seen))
					}
					break
				}
				result := frame.GetResult()
				if result == nil || seen[result.Index] {
					t.Fatal("duplicate/missing result")
				}
				seen[result.Index] = true
				switch result.Index % 3 {
				case 0:
					if result.GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
						t.Fatal(result)
					}
				case 1:
					if string(result.GetRead().GetDocument().GetData()) != fmt.Sprint(result.Index-1) {
						t.Fatal("same key order", result.Index)
					}
				case 2:
					if result.GetRead().GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT {
						t.Fatal(result)
					}
				}
			}
			if _, err := stream.Recv(); err != io.EOF {
				t.Fatal(err)
			}
			if err := <-sent; err != nil {
				t.Fatal(err)
			}
			scanReq := &pb.ScanRequest{Resource: "weir://records/data"}
			scan, err := f.client.Scan(ctx, scanReq)
			if err != nil {
				t.Fatal(err)
			}
			count := uint64(0)
			for {
				frame, err := scan.Recv()
				if err != nil {
					t.Fatal(err)
				}
				if end := frame.GetEnd(); end != nil {
					if end.DocumentCount != count || count != 6 || end.Failure != nil {
						t.Fatal(end)
					}
					break
				}
				count++
				if len(frame.GetDocument().Data) != 200<<10 {
					t.Fatal("document changed")
				}
			}
			if _, err := scan.Recv(); err != io.EOF {
				t.Fatal(err)
			}
			native, err := f.client.Native(ctx)
			if err != nil {
				t.Fatal(err)
			}
			descriptor := &pb.Document{MediaType: "application/octet-stream"}
			nativeOpen := &pb.NativeOpen{Resource: "weir://records/data", Descriptor_: descriptor}
			openVariant := &pb.NativeRequestFrame_Open{Open: nativeOpen}
			opening := &pb.NativeRequestFrame{Frame: openVariant}
			if err := native.Send(opening); err != nil {
				t.Fatal(err)
			}
			raw := bytes.Repeat([]byte{0, 1, 255}, 100000)
			go func() {
				for offset := 0; offset < len(raw); {
					size := min(protocol.NativeChunk, len(raw)-offset)
					v := &pb.NativeRequestFrame_Chunk{Chunk: raw[offset : offset+size]}
					frame := &pb.NativeRequestFrame{Frame: v}
					if err := native.Send(frame); err != nil {
						sent <- err
						return
					}
					offset += size
				}
				sent <- native.CloseSend()
			}()
			var got []byte
			for {
				frame, err := native.Recv()
				if err != nil {
					t.Fatal(err)
				}
				if end := frame.GetEnd(); end != nil {
					if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil {
						t.Fatal(end)
					}
					break
				}
				got = append(got, frame.GetChunk()...)
			}
			if !bytes.Equal(got, raw) {
				t.Fatal("native body changed")
			}
			if _, err := native.Recv(); err != io.EOF {
				t.Fatal(err)
			}
			if err := <-sent; err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestPeerNativeEarlyResponse(t *testing.T) {
	for _, continuous := range []bool{false, true} {
		t.Run(fmt.Sprint(continuous), func(t *testing.T) {
			f := newChain(t, 2)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			stream, err := f.client.Native(ctx)
			if err != nil {
				t.Fatal(err)
			}
			descriptor := &pb.Document{MediaType: "application/octet-stream", Data: []byte("early")}
			open := &pb.NativeOpen{Resource: "weir://records/data", Descriptor_: descriptor}
			variant := &pb.NativeRequestFrame_Open{Open: open}
			frame := &pb.NativeRequestFrame{Frame: variant}
			if err := stream.Send(frame); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			if continuous {
				go func() {
					defer close(done)
					for {
						v := &pb.NativeRequestFrame_Chunk{Chunk: bytes.Repeat([]byte("x"), protocol.NativeChunk)}
						frame := &pb.NativeRequestFrame{Frame: v}
						if stream.Send(frame) != nil {
							return
						}
					}
				}()
			} else {
				close(done)
			}
			var complete bool
			for {
				frame, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if end := frame.GetEnd(); end != nil {
					complete = end.Completion == pb.NativeCompletion_RESPONSE_COMPLETE && end.Failure == nil
				}
			}
			if !complete {
				t.Fatal("early response lost")
			}
			cancel()
			<-done
		})
	}
}

func TestPeerZeroBudgetCycleAndMixedStores(t *testing.T) {
	aListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	bListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = aListener.Close(); _ = bListener.Close() })
	toA, toB := testRemote(t, aListener.Addr().String()), testRemote(t, bListener.Addr().String())
	_, local := peerLocal(t, "local")
	localService := Service{LocalStore: local}
	aService, bService := Service{RemoteWeir: toB}, Service{RemoteWeir: toA}
	routesA := map[string]Service{"records": aService, "local": localService}
	routesB := map[string]Service{"records": bService}

	aOpts := peerServerOptions{routes: routesA, peer: true, listener: aListener}
	bOpts := peerServerOptions{routes: routesB, peer: true, listener: bListener}
	_, aAddress := startPeerServer(t, aOpts)
	startPeerServer(t, bOpts)
	_, client := peerClient(t, aAddress)
	for _, hops := range []string{"0", "1", "4", "8"} {
		ctx, cancel := peerContext(hops)
		started := time.Now()
		_, err := client.Read(ctx, testRequest())
		cancel()
		if status.Code(err) != codes.ResourceExhausted || time.Since(started) > time.Second {
			t.Fatalf("cycle budget=%s %v", hops, err)
		}
	}
	ctx, cancel := peerContext("0")
	defer cancel()
	localRequest := &pb.ReadRequest{Resource: "weir://local/data/s:key"}
	if result, err := client.Read(ctx, localRequest); err != nil || result.GetMissing() == nil {
		t.Fatal("zero-hop local", result, err)
	}
}
func TestPeerDeadlineCancellationAndMetadata(t *testing.T) {
	f := newChain(t, 2)
	block := make(chan struct{})
	f.adapter.block = block
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	md := metadata.Pairs("authorization", "do-not-forward", "baggage", "private=do-not-forward", "weir-request-id", "bounded-id", "traceparent", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
	ctx = metadata.NewOutgoingContext(ctx, md)
	done := make(chan error, 1)
	go func() { _, err := f.client.Read(ctx, testRequest()); done <- err }()
	select {
	case received := <-f.adapter.seen:
		effective, ok := received.Deadline()
		if !ok || effective.After(deadline.Add(3*time.Millisecond)) {
			t.Fatal("deadline extended", effective, deadline)
		}
		incoming := <-f.headers
		if incoming.Get("authorization") != "" || incoming.Get("baggage") != "" || incoming.Get("weir-request-id") != "bounded-id" || len(incoming.Values(HopMetadata)) != 1 || incoming.Get(HopMetadata) != "2" || incoming.Get("traceparent") != md.Get("traceparent")[0] {
			t.Fatal("metadata not filtered")
		}

	case <-ctx.Done():
		t.Fatal("downstream not reached")
	}
	err := <-done
	// The native HTTP/2 write deadline can reset the stream before gRPC sends
	// DeadlineExceeded trailers. Accept only that reset at the deadline; earlier
	// failures and other Internal errors still fail this cancellation test.
	deadlineReset := status.Code(err) == codes.Internal && strings.Contains(status.Convert(err).Message(), "RST_STREAM with error code: INTERNAL_ERROR") && time.Until(deadline) <= 5*time.Millisecond
	if status.Code(err) != codes.DeadlineExceeded && !deadlineReset {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatal("caller did not reach its deadline", ctx.Err())
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatal("transport ended before the caller deadline")
	}
	deadlineWait := time.Now().Add(time.Second)
	for f.runtime.Snapshot().Active != 0 && time.Now().Before(deadlineWait) {
		time.Sleep(time.Millisecond)
	}
	if f.runtime.Snapshot().Active != 0 {
		t.Fatal("cancellation not propagated")
	}
	for _, srv := range f.servers {
		waitPeerIdle(t, srv)
	}
}
func waitPeerIdle(t *testing.T, srv *Server) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for len(srv.slots) > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(srv.slots) != 0 {
		t.Fatal("relay/session credit leaked", len(srv.slots))
	}
}
func TestPeerBulkRejectsMissingOperationFamily(t *testing.T) {
	adapter, runtime := peerLocal(t, "records")
	local := Service{LocalStore: runtime}

	opts := peerServerOptions{routes: map[string]Service{"records": local}, peer: true}
	_, address := startPeerServer(t, opts)
	remote := testRemote(t, address)
	service := Service{RemoteWeir: remote}
	opts = peerServerOptions{routes: map[string]Service{"records": service}, budget: 4}
	srv, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	open := &pb.BulkOpen{Store: "weir://records"}
	variant := &pb.BulkRequestFrame_Open{Open: open}
	frame := &pb.BulkRequestFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		t.Fatal(err)
	}
	read := &pb.BulkOperation_Read{Read: testRequest()}
	op := &pb.BulkOperation{Operation: read}
	opVariant := &pb.BulkRequestFrame_Operation{Operation: op}
	frame = &pb.BulkRequestFrame{Frame: opVariant}
	if err := stream.Send(frame); err != nil {
		t.Fatal(err)
	}
	if result, err := stream.Recv(); err != nil || result.GetResult().GetRead().GetMissing() == nil {
		t.Fatal(result, err)
	}
	op = &pb.BulkOperation{Index: 1}
	opVariant = &pb.BulkRequestFrame_Operation{Operation: op}
	frame = &pb.BulkRequestFrame{Frame: opVariant}
	_ = stream.Send(frame)
	_ = stream.CloseSend()
	if result, err := stream.Recv(); status.Code(err) != codes.InvalidArgument || result != nil {
		t.Fatal(result, err)
	}
	if adapter.commands.Load() != 0 {
		t.Fatal("invalid operation executed")
	}
	waitPeerIdle(t, srv)
}
func TestPeerOverloadKeepsAdmittedNativeAndScan(t *testing.T) {
	f := newChain(t, 2)
	entry := f.servers[len(f.servers)-1]
	entry.admission.SetOverloaded(true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := f.client.Read(ctx, testRequest()); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	if f.adapter.commands.Load() != 0 {
		t.Fatal("forward-only overload executed")
	}
	entry.admission.SetOverloaded(false)
	stream, err := f.client.Native(ctx)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := &pb.Document{MediaType: "application/octet-stream"}
	open := &pb.NativeOpen{Resource: "weir://records/data", Descriptor_: descriptor}
	variant := &pb.NativeRequestFrame_Open{Open: open}
	frame := &pb.NativeRequestFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		t.Fatal(err)
	}
	if head, err := stream.Recv(); err != nil || head.GetHead() == nil {
		t.Fatal(head, err)
	}
	for _, srv := range f.servers {
		srv.admission.SetOverloaded(true)
	}
	chunk := &pb.NativeRequestFrame_Chunk{Chunk: []byte("already admitted")}
	frame = &pb.NativeRequestFrame{Frame: chunk}
	if err := stream.Send(frame); err != nil {
		t.Fatal(err)
	}
	_ = stream.CloseSend()
	var complete bool
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if end := frame.GetEnd(); end != nil {
			complete = end.Completion == pb.NativeCompletion_RESPONSE_COMPLETE
		}
	}
	if !complete {
		t.Fatal("overload rejected admitted body")
	}
	for _, srv := range f.servers {
		srv.admission.SetOverloaded(false)
	}
	scanReq := &pb.ScanRequest{Resource: "weir://records/data"}
	scan, err := f.client.Scan(ctx, scanReq)
	if err != nil {
		t.Fatal(err)
	}
	if frame, err := scan.Recv(); err != nil || frame.GetDocument() == nil {
		t.Fatal(frame, err)
	}
	for _, srv := range f.servers {
		srv.admission.SetOverloaded(true)
	}
	var end *pb.ScanEnd
	for {
		frame, err := scan.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if frame.GetEnd() != nil {
			end = frame.GetEnd()
		}
	}
	if end == nil || end.Failure != nil || end.DocumentCount != 6 {
		t.Fatal(end)
	}
}
func TestPeerScanFailureNeverRestarts(t *testing.T) {
	f := newChain(t, 2)
	f.adapter.scanFailure = true
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request := &pb.ScanRequest{Resource: "weir://records/data"}
	scan, err := f.client.Scan(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	var count uint64
	var end *pb.ScanEnd
	for {
		frame, err := scan.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if frame.GetDocument() != nil {
			count++
		}
		if frame.GetEnd() != nil {
			end = frame.GetEnd()
		}
	}
	if end == nil || end.Failure.GetCode() != pb.FailureCode_UNAVAILABLE || count != 2 || end.DocumentCount != 2 || f.adapter.scans.Load() != 2 || f.adapter.cleanups.Load() != 1 {
		t.Fatal("failure/restart evidence", end, count, f.adapter.scans.Load(), f.adapter.cleanups.Load())
	}
}

func TestPeerDeadlineIncludesConcurrentConnectionSetups(t *testing.T) {
	adapter, runtime := peerLocal(t, "records")
	adapter.block = make(chan struct{})
	local := Service{LocalStore: runtime}
	limits := DefaultLimits()
	limits.UnaryLifetime = 350 * time.Millisecond
	limits.Stall = time.Second

	opts := peerServerOptions{routes: map[string]Service{"records": local}, peer: true, limits: limits}
	var setups atomic.Int32
	opts.listener = delayedPeerListener(t, &setups)
	_, address := startPeerServer(t, opts)
	for hop := 0; hop < 2; hop++ {
		// Delay accepting the real HTTP/2 connection at each destination.
		// No client dial override or test hook enters production code.
		remote := testRemote(t, address)

		service := Service{RemoteWeir: remote}
		opts = peerServerOptions{routes: map[string]Service{"records": service}, limits: limits, budget: 4}
		if hop == 0 {
			opts.peer = true
			opts.listener = delayedPeerListener(t, &setups)
		}
		_, address = startPeerServer(t, opts)
	}
	_, client := peerClient(t, address)
	started := time.Now()
	done := make(chan error, 1)
	go func() { _, err := client.Read(context.Background(), testRequest()); done <- err }()
	select {
	case received := <-adapter.seen:
		deadline, ok := received.Deadline()
		remaining := time.Until(deadline)
		if !ok || setups.Load() != 2 || remaining > 350*time.Millisecond-time.Since(started)+20*time.Millisecond || deadline.After(started.Add(375*time.Millisecond)) {
			t.Fatal("connection time was not charged", remaining, deadline, setups.Load())
		}
		t.Logf("two HTTP/2 connection setups consumed %v; backend remaining deadline %v", time.Since(started), remaining)
	case <-time.After(time.Second):
		t.Fatal("backend not reached")
	}
	select {
	case err := <-done:
		// The server's final-status write deadline can reset HTTP/2 before a
		// DEADLINE_EXCEEDED trailer is delivered. Either outcome must be non-OK.
		if err == nil || time.Since(started) > 650*time.Millisecond {
			t.Fatal("entry lifetime not preserved", err, time.Since(started))
		}
	case <-time.After(time.Second):
		t.Fatal("unbounded no-deadline caller")
	}
}

type delayedListener struct {
	net.Listener
	setups *atomic.Int32
}

func delayedPeerListener(t *testing.T, setups *atomic.Int32) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	delayed := &delayedListener{Listener: listener, setups: setups}
	return delayed
}

func (l *delayedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	time.Sleep(60 * time.Millisecond)
	l.setups.Add(1)
	return conn, nil
}

func TestPeerBoundedDiagnosticMetadata(t *testing.T) {
	f := newChain(t, 2)
	trace := "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
	cases := []metadata.MD{
		{"weir-request-id": {""}},
		{"weir-request-id": {string(bytes.Repeat([]byte{'a'}, 129))}},
		{"weir-request-id": {"two words"}},
		{"weir-request-id": {"one", "two"}},
		{"traceparent": {"bad"}},
		{"traceparent": {trace, trace}},
	}
	for _, md := range cases {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		ctx = metadata.NewOutgoingContext(ctx, md)
		_, err := f.client.Read(ctx, testRequest())
		cancel()
		if status.Code(err) != codes.InvalidArgument {
			t.Fatal("invalid diagnostics reached forwarding", err)
		}
	}
	if len(f.adapter.seen) != 0 {
		t.Fatal("invalid diagnostic metadata executed")
	}
	state := &ingress{hops: 4, diagnostic: metadata.Pairs("weir-request-id", "bounded")}
	ctx := context.WithValue(context.Background(), ingressKey, state)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(HopMetadata, "8", HopMetadata, "8", "baggage", "discard"))
	forwarded, err := forwardContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	md, _ := metadata.FromOutgoingContext(forwarded)
	if len(md) != 2 || len(md.Get(HopMetadata)) != 1 || md.Get(HopMetadata)[0] != "3" || md.Get("weir-request-id")[0] != "bounded" {
		t.Fatal("metadata was appended instead of replaced", md)
	}
}
