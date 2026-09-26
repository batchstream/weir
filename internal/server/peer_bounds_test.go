package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/overload"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/testpeer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type terminalPeer struct {
	pb.UnimplementedWeirServer
	mode  string
	calls atomic.Int32
}

func (p *terminalPeer) Scan(_ *pb.ScanRequest, stream grpc.ServerStreamingServer[pb.ScanResponseFrame]) error {
	p.calls.Add(1)
	doc := &pb.Document{MediaType: "application/octet-stream", Data: []byte("opaque")}
	variant := &pb.ScanResponseFrame_Document{Document: doc}
	frame := &pb.ScanResponseFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		return err
	}
	if p.mode == "missing" {
		return nil
	}
	end := &pb.ScanEnd{DocumentCount: 1}
	if p.mode == "count" {
		end.DocumentCount = 2
	}
	endVariant := &pb.ScanResponseFrame_End{End: end}
	terminal := &pb.ScanResponseFrame{Frame: endVariant}
	if err := stream.Send(terminal); err != nil {
		return err
	}
	if p.mode == "extra" {
		return stream.Send(frame)
	}
	return status.Error(codes.Unavailable, "failure after End")
}
func (p *terminalPeer) Native(stream grpc.BidiStreamingServer[pb.NativeRequestFrame, pb.NativeResponseFrame]) error {
	p.calls.Add(1)
	if _, err := stream.Recv(); err != nil {
		return err
	}
	head := &pb.NativeHead{BodyMediaType: "application/octet-stream"}
	variant := &pb.NativeResponseFrame_Head{Head: head}
	frame := &pb.NativeResponseFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		return err
	}
	if p.mode == "missing" {
		return nil
	}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	endVariant := &pb.NativeResponseFrame_End{End: end}
	terminal := &pb.NativeResponseFrame{Frame: endVariant}
	if err := stream.Send(terminal); err != nil {
		return err
	}
	if p.mode == "extra" {
		return stream.Send(frame)
	}
	return status.Error(codes.Unavailable, "failure after End")
}
func (p *terminalPeer) Bulk(stream grpc.BidiStreamingServer[pb.BulkRequestFrame, pb.BulkResponseFrame]) error {
	p.calls.Add(1)
	if _, err := stream.Recv(); err != nil {
		return err
	}
	var operations []*pb.BulkOperation
	for range 3 {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		operations = append(operations, frame.GetOperation())
	}
	// Finite fault fixture deliberately completes these three independent keys in reverse order.
	for i := len(operations) - 1; i >= 0; i-- {
		result := protocol.ResultError(operations[i], pb.MutationOutcome_NOT_STARTED, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "fixture"))
		variant := &pb.BulkResponseFrame_Result{Result: result}
		frame := &pb.BulkResponseFrame{Frame: variant}
		if err := stream.Send(frame); err != nil {
			return err
		}
	}
	if p.mode == "missing" {
		return nil
	}
	count := uint64(len(operations))
	end := &pb.BulkEnd{ReceivedCount: count, ResultCount: count}
	if p.mode == "count" {
		end.ResultCount++
	}
	variant := &pb.BulkResponseFrame_End{End: end}
	frame := &pb.BulkResponseFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		return err
	}
	if p.mode == "extra" {
		return stream.Send(frame)
	}
	if p.mode == "valid" {
		return nil
	}
	return status.Error(codes.Unavailable, "failure after End")
}
func TestPeerRejectsMissingOrNonOKTerminal(t *testing.T) {
	for _, method := range []string{"Scan", "Native", "Bulk"} {
		for _, mode := range []string{"missing", "status", "extra", "count", "valid"} {
			if mode == "valid" && method != "Bulk" {
				continue
			}
			t.Run(method+"/"+mode, func(t *testing.T) {
				ca := testpeer.NewCA(t)
				identity, _, _ := ca.Identity(t, "node.weir.test")
				identity.ServerName = "node.weir.test"
				fixture := &terminalPeer{mode: mode}
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				peer := grpc.NewServer(grpc.Creds(credentials.NewTLS(identity)), grpc.MaxRecvMsgSize(protocol.MaxFrame), grpc.MaxSendMsgSize(protocol.MaxFrame))
				pb.RegisterWeirServer(peer, fixture)
				go func() { _ = peer.Serve(listener) }()
				t.Cleanup(func() { peer.Stop(); _ = listener.Close() })
				remote := testRemote(t, listener.Addr().String(), identity)
				service := Service{RemoteWeir: remote}
				opts := peerServerOptions{routes: map[string]Service{"records": service}, budget: 4}
				srv, address := startPeerServer(t, opts)
				_, client := peerClient(t, address, nil)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				var terminal bool
				var final error
				switch method {
				case "Scan":
					request := &pb.ScanRequest{Resource: "weir://records/data"}
					stream, err := client.Scan(ctx, request)
					if err != nil {
						t.Fatal(err)
					}
					for {
						frame, err := stream.Recv()
						if err != nil {
							final = err
							break
						}
						terminal = terminal || frame.GetEnd() != nil
					}
				case "Native":
					stream, err := client.Native(ctx)
					if err != nil {
						t.Fatal(err)
					}
					descriptor := &pb.Document{MediaType: "application/octet-stream"}
					open := &pb.NativeOpen{Resource: "weir://records/data", Descriptor_: descriptor}
					variant := &pb.NativeRequestFrame_Open{Open: open}
					frame := &pb.NativeRequestFrame{Frame: variant}
					_ = stream.Send(frame)
					for {
						frame, err := stream.Recv()
						if err != nil {
							final = err
							break
						}
						terminal = terminal || frame.GetEnd() != nil
					}
				case "Bulk":
					stream, err := client.Bulk(ctx)
					if err != nil {
						t.Fatal(err)
					}
					open := &pb.BulkOpen{Store: "weir://records"}
					variant := &pb.BulkRequestFrame_Open{Open: open}
					frame := &pb.BulkRequestFrame{Frame: variant}
					_ = stream.Send(frame)
					for i := uint64(0); i < 3; i++ {
						read := testRequest()
						read.Resource += fmt.Sprint(i)
						operation := &pb.BulkOperation_Read{Read: read}
						op := &pb.BulkOperation{Index: i, Operation: operation}
						variant := &pb.BulkRequestFrame_Operation{Operation: op}
						frame := &pb.BulkRequestFrame{Frame: variant}
						_ = stream.Send(frame)
					}
					_ = stream.CloseSend()
					expected := uint64(2)
					for {
						frame, err := stream.Recv()
						if err != nil {
							final = err
							break
						}
						if result := frame.GetResult(); result != nil {
							if result.Index != expected {
								t.Fatal("out-of-order correlation", result.Index, expected)
							}
							expected--
						}
						terminal = terminal || frame.GetEnd() != nil
					}
				}
				if mode == "valid" {
					if !terminal || final != io.EOF {
						t.Fatal("valid reversed stream", terminal, final)
					}
				} else if final == io.EOF || terminal {
					t.Fatal("false complete terminal", terminal, final)
				}
				if fixture.calls.Load() != 1 {
					t.Fatal("stream restarted", fixture.calls.Load())
				}
				waitPeerIdle(t, srv)
			})
		}
	}
}
func TestPeerSlowConsumersAndRepeatedCancellation(t *testing.T) {
	limits := DefaultLimits()
	limits.Stall = 120 * time.Millisecond
	limits.UnaryLifetime = 250 * time.Millisecond
	f := newChainWithLimits(t, 2, limits)
	payload := bytes.Repeat([]byte("x"), 200<<10)
	f.adapter.documents[testRequest().Resource] = &pb.Document{MediaType: "application/octet-stream", Data: payload}
	baseline := runtime.NumGoroutine()
	for cycle := 0; cycle < 12; cycle++ {
		ctx, cancel := context.WithCancel(context.Background())
		request := &pb.ScanRequest{Resource: "weir://records/data"}
		stream, err := f.client.Scan(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Header(); err != nil {
			t.Fatal(err)
		}
		if cycle%3 == 0 {
			time.Sleep(200 * time.Millisecond)
		}
		cancel()
		for _, srv := range f.servers {
			waitPeerIdle(t, srv)
		}
		for _, remote := range f.remotes {
			if len(remote.slots) != 0 || len(remote.sockets) > 2 {
				t.Fatal("remote resource bounds", len(remote.slots), len(remote.sockets))
			}
		}
		if cycle%4 == 3 {
			runtime.GC()
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			t.Logf("cancel sample=%d heap=%d goroutines=%d baseline=%d runtime=%+v", cycle+1, stats.HeapAlloc, runtime.NumGoroutine(), baseline, f.runtime.Snapshot())
		}
	}
	// Unary data is larger than every fixed receiving window. No caller deadline.
	descriptor := &grpc.StreamDesc{ServerStreams: true}
	stream, err := f.conn.NewStream(context.Background(), descriptor, pb.Weir_Read_FullMethodName)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg(testRequest()); err != nil {
		t.Fatal(err)
	}
	_ = stream.CloseSend()
	if _, err := stream.Header(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(350 * time.Millisecond)
	reply := &pb.ReadResult{}
	err = stream.RecvMsg(reply)
	if err == nil {
		extra := &pb.ReadResult{}
		err = stream.RecvMsg(extra)
	}
	if err == io.EOF {
		t.Fatal("complete unary after delivery deadline")
	}
	for _, srv := range f.servers {
		waitPeerIdle(t, srv)
	}
	if runtime.NumGoroutine() > baseline+15 {
		t.Fatal("goroutine growth", runtime.NumGoroutine(), baseline)
	}
}
func TestPeerBulkBackpressureAndDrainEveryHop(t *testing.T) {
	for _, node := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(node), func(t *testing.T) {
			limits := DefaultLimits()
			limits.Stall = 500 * time.Millisecond
			f := newChainWithLimits(t, 2, limits)
			f.adapter.documents[testRequest().Resource] = &pb.Document{MediaType: "application/octet-stream", Data: bytes.Repeat([]byte("x"), 200<<10)}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := f.client.Bulk(ctx)
			if err != nil {
				t.Fatal(err)
			}
			open := &pb.BulkOpen{Store: "weir://records"}
			variant := &pb.BulkRequestFrame_Open{Open: open}
			frame := &pb.BulkRequestFrame{Frame: variant}
			_ = stream.Send(frame)
			var sent atomic.Int64
			done := make(chan struct{})
			go func() {
				defer close(done)
				for index := uint64(0); ; index++ {
					read := &pb.BulkOperation_Read{Read: testRequest()}
					op := &pb.BulkOperation{Index: index, Operation: read}
					variant := &pb.BulkRequestFrame_Operation{Operation: op}
					frame := &pb.BulkRequestFrame{Frame: variant}
					if stream.Send(frame) != nil {
						return
					}
					sent.Add(1)
				}
			}()
			if _, err := stream.Header(); err != nil {
				t.Fatal(err)
			}
			time.Sleep(80 * time.Millisecond)
			first := sent.Load()
			time.Sleep(80 * time.Millisecond)
			second := sent.Load()
			if second > first+20 || f.runtime.Snapshot().Retained > 8 {
				t.Fatal("unbounded input/results", first, second, f.runtime.Snapshot())
			}
			drain, stop := context.WithTimeout(context.Background(), 70*time.Millisecond)
			defer stop()
			start := time.Now()
			if err := f.servers[node].Shutdown(drain); err != nil {
				t.Fatal(err)
			}
			if time.Since(start) > time.Second {
				t.Fatal("drain waited for unlimited producer")
			}
			cancel()
			<-done
			for _, srv := range f.servers {
				waitPeerIdle(t, srv)
			}
			t.Logf("node=%d frames after 80/160ms=%d/%d, retained=%d", node, first, second, f.runtime.Snapshot().Retained)
		})
	}
}

func TestPeerNativeSlowDownloadAndBidirectionalBlock(t *testing.T) {
	for _, mode := range []string{"download", "upload", "both"} {
		t.Run(mode, func(t *testing.T) {
			limits := DefaultLimits()
			limits.Stall = 120 * time.Millisecond
			limits.NativeLifetime = 500 * time.Millisecond
			f := newChainWithLimits(t, 2, limits)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
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
			sent := make(chan error, 1)
			if mode == "download" {
				go func() {
					for range 6 {
						variant := &pb.NativeRequestFrame_Chunk{Chunk: bytes.Repeat([]byte("x"), protocol.NativeChunk)}
						frame := &pb.NativeRequestFrame{Frame: variant}
						if err := stream.Send(frame); err != nil {
							sent <- err
							return
						}
					}
					sent <- stream.CloseSend()
				}()
			} else {
				sent <- nil
			}
			if _, err := stream.Header(); err != nil {
				t.Fatal(err)
			}
			// No half-close in upload/both; no consumer progress in download/both.
			if mode != "upload" {
				time.Sleep(250 * time.Millisecond)
			}
			complete := false
			for {
				frame, err := stream.Recv()
				if err != nil {
					if err == io.EOF && complete {
						t.Fatal("stalled exchange completed")
					}
					break
				}
				if frame.GetEnd().GetCompletion() == pb.NativeCompletion_RESPONSE_COMPLETE {
					complete = true
				}
			}
			cancel()
			<-sent
			for _, srv := range f.servers {
				waitPeerIdle(t, srv)
			}
			snapshot := f.runtime.Snapshot()
			if snapshot.Active != 0 || snapshot.NativeBytes != 0 || snapshot.Retained != 0 {
				t.Fatal("Native retained after cleanup", snapshot)
			}
		})
	}
}
func TestPeerConnectionRebuildAndStoreIsolation(t *testing.T) {
	ca := testpeer.NewCA(t)
	identity, _, _ := ca.Identity(t, "node.weir.test")
	identity.ServerName = "node.weir.test"
	blocked, runtimeBlocked := peerLocal(t, "records")
	release := make(chan struct{})
	blocked.block = release
	_, runtimeLocal := peerLocal(t, "local")
	localService := Service{LocalStore: runtimeBlocked}
	options := peerServerOptions{routes: map[string]Service{"records": localService}, tls: identity, allow: map[string]map[string]Permission{"node.weir.test": {"records": AllPermissions}}}
	backend, address := startPeerServer(t, options)
	remote := testRemote(t, address, identity)
	remoteService := Service{RemoteWeir: remote}
	healthyService := Service{LocalStore: runtimeLocal}
	options = peerServerOptions{routes: map[string]Service{"records": remoteService, "local": healthyService}, budget: 4}
	entry, address := startPeerServer(t, options)
	_, client := peerClient(t, address, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { _, err := client.Read(ctx, testRequest()); finished <- err }()
	select {
	case <-blocked.seen:
	case <-ctx.Done():
		t.Fatal("blocked peer not reached")
	}
	request := &pb.ReadRequest{Resource: "weir://local/data/s:key"}
	if result, err := client.Read(ctx, request); err != nil || result.GetMissing() == nil {
		t.Fatal("other Store blocked", result, err)
	}
	backend.connections.Range(func(_, value any) bool { _ = value.(*limitedConn).Close(); return true })
	if err := <-finished; err == nil {
		t.Fatal("in-flight disconnect was hidden")
	}
	close(release)
	// Each iteration is a distinct new Read. Production never resumes the lost call.
	attempts := 0
	for {
		attempts++
		result, err := client.Read(ctx, testRequest())
		if err == nil {
			if result.GetMissing() == nil {
				t.Fatal(result)
			}
			break
		}
		if ctx.Err() != nil {
			t.Fatal("future connection did not recover", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	waitPeerIdle(t, entry)
	if len(remote.sockets) > 2 {
		t.Fatal("reconnect socket bound")
	}
	t.Log("future Read recovered after disconnect; distinct calls", attempts)
}
func TestPeerRelayAdmissionAndSharedListeners(t *testing.T) {
	ca := testpeer.NewCA(t)
	identity, _, _ := ca.Identity(t, "node.weir.test")
	identity.ServerName = "node.weir.test"
	adapter, local := peerLocal(t, "records")
	release := make(chan struct{})
	adapter.block = release
	service := Service{LocalStore: local}
	limits := DefaultLimits()
	limits.Sessions = 2
	limits.Connections = 2
	options := peerServerOptions{routes: map[string]Service{"records": service}, tls: identity, allow: map[string]map[string]Permission{"node.weir.test": {"records": AllPermissions}}, limits: limits}
	_, address := startPeerServer(t, options)
	config := RemoteConfig{Endpoint: address, TLS: identity, Relays: 1}
	remote, err := NewRemote(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = remote.Close() })
	service = Service{RemoteWeir: remote}
	admission, err := NewAdmission(limits)
	if err != nil {
		t.Fatal(err)
	}
	options = peerServerOptions{routes: map[string]Service{"records": service}, budget: 4, admission: admission, limits: limits}
	public, publicAddress := startPeerServer(t, options)
	_, client := peerClient(t, publicAddress, nil)
	options.tls = identity
	options.allow = map[string]map[string]Permission{"node.weir.test": {"records": AllPermissions}}
	peer, peerAddress := startPeerServer(t, options)
	_, trusted := peerClient(t, peerAddress, identity)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := client.Read(ctx, testRequest()); done <- err }()
	select {
	case <-adapter.seen:
	case <-ctx.Done():
		t.Fatal("backend not reached")
	}
	peerCtx, stop := peerContext("4")
	defer stop()
	if _, err := trusted.Read(peerCtx, testRequest()); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("relay capacity was not shared", err)
	}
	if len(remote.slots) != 1 || len(public.slots) != 1 || public.slots != peer.slots {
		t.Fatal("shared admission accounting")
	}
	if len(admission.connections) != 2 {
		t.Fatal("public and peer connection accounting", len(admission.connections))
	}
	extra, err := net.DialTimeout("tcp", publicAddress, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Close()
	_ = extra.SetReadDeadline(time.Now().Add(time.Second))
	var probe [1]byte
	if _, err := extra.Read(probe[:]); err != io.EOF {
		t.Fatal("shared listener connection capacity was exceeded", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	waitPeerIdle(t, public)
}

func TestPeerForwardOnlyProcessSampler(t *testing.T) {
	f := newChain(t, 1)
	entry := f.servers[1]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	targets := []overload.Target{entry.admission}
	// Real process sampling with an intentionally tiny threshold, not a dummy Runtime.
	go func() { defer close(done); overload.Guard(ctx, targets, 1) }()
	deadline := time.Now().Add(time.Second)
	for !entry.admission.overloaded.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	request, cancelRequest := context.WithTimeout(context.Background(), time.Second)
	defer cancelRequest()
	if _, err := f.client.Read(request, testRequest()); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("sampler did not stop forwarding-only admission", err)
	}
	cancel()
	<-done
}
