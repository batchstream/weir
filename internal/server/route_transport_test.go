package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/routeclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type opaqueDestination struct {
	pb.UnimplementedWeirServer
	seen    chan metadata.MD
	calls   atomic.Int32
	payload []byte
}

func (p *opaqueDestination) Route(stream grpc.BidiStreamingServer[pb.Request, pb.Response]) error {
	p.calls.Add(1)
	md, _ := metadata.FromIncomingContext(stream.Context())
	p.seen <- md
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if !bytes.Equal(request.Payload, p.payload) {
			return status.Error(codes.Internal, "relay interpreted payload")
		}
		response := &pb.Response{Id: request.Id, Payload: []byte("opaque response")}
		if err := stream.Send(response); err != nil {
			return err
		}
		end := &pb.Response{Id: request.Id, End: true}
		if err := stream.Send(end); err != nil {
			return err
		}
	}
}
func TestRouteTransparentRelayAndHopLimit(t *testing.T) {
	fixture := &opaqueDestination{seen: make(chan metadata.MD, 4), payload: []byte{0xff, 0, 0xff}}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	downstream := grpc.NewServer()
	pb.RegisterWeirServer(downstream, fixture)
	go func() { _ = downstream.Serve(listener) }()
	t.Cleanup(func() { downstream.Stop(); _ = listener.Close() })
	remote := testRemote(t, listener.Addr().String())
	service := Service{RemoteWeir: remote}
	options := peerServerOptions{routes: map[string]Service{"records": service}, budget: 4}
	srv, address := startPeerServer(t, options)
	_, client := peerClient(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := client.Route(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request := &pb.Request{Id: 7, Destination: "records", Payload: fixture.payload}
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	_ = stream.CloseSend()
	response, err := stream.Recv()
	if err != nil || string(response.Payload) != "opaque response" {
		t.Fatal(response, err)
	}
	response, err = stream.Recv()
	if err != nil || !response.End {
		t.Fatal(response, err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	headers := <-fixture.seen
	if got := headers.Get(HopMetadata); len(got) != 1 || got[0] != "3" {
		t.Fatal("hop not decremented", got)
	}
	waitPeerIdle(t, srv)
	options.budget = 0
	blocked, address := startPeerServer(t, options)
	_, client = peerClient(t, address)
	stream, err = client.Route(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(request)
	_ = stream.CloseSend()
	if _, err := stream.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("exhausted hops", err)
	}
	waitPeerIdle(t, blocked)
	if fixture.calls.Load() != 1 {
		t.Fatal("zero hops forwarded")
	}
}

func TestRoutePeerMetadataDeadlineAndCancellation(t *testing.T) {
	adapter, rt := peerLocal(t, "records")
	release := make(chan struct{})
	adapter.block = release
	local := Service{LocalStore: rt}
	options := peerServerOptions{routes: map[string]Service{"records": local}, peer: true}
	backend, address := startPeerServer(t, options)
	_, trusted := peerClient(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := routeRead(trusted, ctx, testRequest()); status.Code(err) != codes.InvalidArgument {
		t.Fatal("peer accepted missing hop metadata", err)
	}
	remote := testRemote(t, address)
	forward := Service{RemoteWeir: remote}
	options = peerServerOptions{routes: map[string]Service{"records": forward}, budget: 2}
	entry, address := startPeerServer(t, options)
	_, client := peerClient(t, address)
	requestCtx, stop := context.WithTimeout(ctx, 700*time.Millisecond)
	original, _ := requestCtx.Deadline()
	done := make(chan error, 1)
	go func() { _, err := routeMutate(client, requestCtx, testMutation("value")); done <- err }()
	select {
	case observed := <-adapter.seen:
		deadline, ok := observed.Deadline()
		if !ok || deadline.After(original.Add(10*time.Millisecond)) {
			t.Fatal("deadline reset", deadline, original)
		}
	case <-ctx.Done():
		t.Fatal("execution not reached")
	}
	stop()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatal("cancel not propagated", err)
	}
	close(release)
	waitPeerIdle(t, entry)
	waitPeerIdle(t, backend)
	until := time.Now().Add(time.Second)
	for rt.Snapshot().Retained != 0 {
		if time.Now().After(until) {
			t.Fatal("cancel retained tickets", rt.Snapshot())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRouteSlowConsumerBackpressureAndShutdown(t *testing.T) {
	adapter, rt := peerLocal(t, "records")
	adapter.documents[testRequest().Resource] = &pb.Document{MediaType: "application/octet-stream", Data: bytes.Repeat([]byte("x"), protocol.MaxDocument)}
	local := Service{LocalStore: rt}
	limits := DefaultLimits()
	limits.Stall = 3 * time.Second
	options := peerServerOptions{routes: map[string]Service{"records": local}, limits: limits}
	srv, address := startPeerServer(t, options)
	_, client := peerClient(t, address)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Route(ctx)
	if err != nil {
		t.Fatal(err)
	}
	read := testRequest()
	read.AdapterOptions = &pb.Document{MediaType: "application/octet-stream", Data: bytes.Repeat([]byte("p"), 128<<10)}
	value := &pb.Call_Read{Read: read}
	call := &pb.Call{Version: 1, Operation: value}
	payload, err := proto.Marshal(call)
	if err != nil {
		t.Fatal(err)
	}
	var sent atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for id := uint64(1); ; id++ {
			request := &pb.Request{Id: id, Destination: "records", Payload: payload}
			if stream.Send(request) != nil {
				return
			}
			sent.Add(1)
		}
	}()
	if _, err := stream.Header(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	stable := time.Now()
	last := sent.Load()
	for time.Since(stable) < 100*time.Millisecond {
		if time.Now().After(deadline) {
			t.Fatal("producer never blocked", sent.Load())
		}
		time.Sleep(10 * time.Millisecond)
		now := sent.Load()
		if now != last {
			last = now
			stable = time.Now()
		}
	}
	select {
	case <-done:
		t.Fatal("cancellation does not prove flow control")
	default:
	}
	time.Sleep(100 * time.Millisecond)
	if sent.Load() != last {
		t.Fatal("unbounded input read-ahead", last, sent.Load())
	}
	snapshot := srv.Snapshot()
	if snapshot.Outstanding > protocol.RouteOutstanding || snapshot.OutstandingBytes > protocol.RouteBytes || rt.Snapshot().Retained > protocol.RouteOutstanding {
		t.Fatal("bounded ledgers exceeded", snapshot, rt.Snapshot())
	}
	drain, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	started := time.Now()
	if err := srv.Shutdown(drain); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("shutdown blocked")
	}
	cancel()
	<-done
	waitPeerIdle(t, srv)
	t.Logf("blocked producer plateau=%d; router=%+v; runtime=%+v", last, snapshot, rt.Snapshot())
}

func TestRouteGracefulDrainPreservesAdmittedExecution(t *testing.T) {
	adapter, rt := peerLocal(t, "records")
	release := make(chan struct{})
	adapter.block = release
	local := Service{LocalStore: rt}
	options := peerServerOptions{routes: map[string]Service{"records": local}}
	srv, address := startPeerServer(t, options)
	_, client := peerClient(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resultDone := make(chan error, 1)
	go func() {
		result, err := routeMutate(client, ctx, testMutation("applied"))
		if err == nil && result.Outcome != pb.MutationOutcome_APPLIED {
			err = io.ErrUnexpectedEOF
		}
		resultDone <- err
	}()
	var backendCtx context.Context
	select {
	case backendCtx = <-adapter.seen:
	case <-ctx.Done():
		t.Fatal("backend not reached")
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- srv.Shutdown(ctx) }()
	time.Sleep(50 * time.Millisecond)
	if backendCtx.Err() != nil {
		t.Fatal("graceful drain canceled admitted mutation", backendCtx.Err())
	}
	close(release)
	if err := <-resultDone; err != nil {
		t.Fatal("admitted result lost", err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	waitPeerIdle(t, srv)
}

func TestRouteAppliedWriteWithReplicaFailurePreservesItemEvidence(t *testing.T) {
	adapter, rt := peerLocal(t, "records")
	adapter.ackFailureKey = "data/s:ack"
	adapter.ackFailure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "write acknowledged but replica acknowledgement failed")
	local := Service{LocalStore: rt}
	options := peerServerOptions{routes: map[string]Service{"records": local}, peer: true}
	backend, address := startPeerServer(t, options)
	servers := []*Server{backend}
	for hop := 0; hop < 2; hop++ {
		remote := testRemote(t, address)
		service := Service{RemoteWeir: remote}
		options = peerServerOptions{routes: map[string]Service{"records": service}, budget: 4, peer: hop == 0}
		relay, nextAddress := startPeerServer(t, options)
		servers = append(servers, relay)
		address = nextAddress
	}
	_, client := peerClient(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var produced int
	results := make(map[uint64]*pb.MutationResult)
	completed := make(map[uint64]bool)
	opts := routeclient.Options{Destination: "records"}
	opts.Produce = func(context.Context) (*pb.Call, error) {
		if produced == 2 {
			return nil, io.EOF
		}
		request := testMutation("persisted")
		if produced == 0 {
			request.Resource = adapter.ackFailureKey
		} else {
			request.Resource = "data/s:other"
		}
		produced++
		value := &pb.Call_Mutate{Mutate: request}
		call := &pb.Call{Version: 1, Operation: value}
		return call, nil
	}
	opts.Consume = func(_ context.Context, id uint64, event *pb.Event) error {
		result := event.GetResult()
		if result == nil || result.GetMutation() == nil {
			return errors.New("missing mutation evidence")
		}
		results[id] = result.GetMutation()
		return nil
	}
	opts.Complete = func(_ context.Context, id uint64) error { completed[id] = true; return nil }
	if err := routeclient.Run(ctx, client, opts); err != nil {
		t.Fatal("valid acknowledgement truncated Route", err)
	}
	first := results[1]
	if first == nil || first.Outcome != pb.MutationOutcome_APPLIED || !proto.Equal(first.Failure, adapter.ackFailure) {
		t.Fatal("positive primary acknowledgement or replica failure lost", first)
	}
	second := results[2]
	if second == nil || second.Outcome != pb.MutationOutcome_APPLIED || second.Failure != nil {
		t.Fatal("peer item contaminated", second)
	}
	if len(completed) != 2 || !completed[1] || !completed[2] || adapter.commands.Load() != 2 {
		t.Fatal("item completion or execution count", completed, adapter.commands.Load())
	}
	for _, key := range []string{adapter.ackFailureKey, "data/s:other"} {
		request := &pb.ReadRequest{Resource: key}
		result, err := routeRead(client, ctx, request)
		if err != nil || string(result.GetDocument().GetData()) != "persisted" {
			t.Fatal("applied record absent", key, result, err)
		}
	}
	for _, server := range servers {
		waitPeerIdle(t, server)
	}
}
