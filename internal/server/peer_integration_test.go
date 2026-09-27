//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	spb "github.com/batchstream/weir/api/weir/search/v1"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func forwardFixture(t *testing.T, f scanFixture, hops int) scanFixture {
	t.Helper()
	name, _, _ := protocolResource(f.root)
	service := Service{LocalStore: f.runtime}
	opts := peerServerOptions{routes: map[string]Service{name: service}, peer: true, limits: f.server.limits, admission: f.server.admission}
	_, address := startPeerServer(t, opts)
	for i := 0; i < hops; i++ {
		remote := testRemote(t, address)
		f.metricsRemotes = append(f.metricsRemotes, remote)
		route := Service{RemoteWeir: remote}
		opts := peerServerOptions{routes: map[string]Service{name: route}, limits: f.server.limits, budget: 4}
		if i < hops-1 {
			opts.peer = true
		}
		f.server, address = startPeerServer(t, opts)
	}
	f.address = address
	f.conn, f.client = peerClient(t, address)
	return f
}
func protocolResource(root string) (string, string, bool) {
	trimmed := strings.TrimPrefix(root, "weir://")
	return strings.Cut(trimmed, "/")
}
func realMutation(t *testing.T, f scanFixture, id string, n int) *pb.MutateRequest {
	t.Helper()
	var doc *pb.Document
	if f.backend == nil {
		value := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: n}}
		raw, err := bson.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		doc = &pb.Document{MediaType: "application/bson", Data: raw}
	} else {
		doc = &pb.Document{MediaType: "application/json", Data: []byte(fmt.Sprintf(`{"n":%d}`, n))}
	}
	action := &pb.MutateRequest_Put{Put: doc}
	req := &pb.MutateRequest{Resource: f.root + "/s:" + id, Action: action}
	return req
}
func TestPeerRealFiveRPCs(t *testing.T) {
	for _, kind := range []string{"mongo", "search"} {
		t.Run(kind, func(t *testing.T) {
			for _, hops := range []int{0, 1, 2} {
				t.Run(fmt.Sprint(hops), func(t *testing.T) {
					f := scanServer(t, kind, DefaultLimits())
					if hops > 0 {
						f = forwardReplicaFixture(t, f, hops)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					request := realMutation(t, f, "peer", 1)
					if result, err := f.client.Mutate(ctx, request); err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
						t.Fatal(result, err)
					}
					read := &pb.ReadRequest{Resource: request.Resource}
					result, err := f.client.Read(ctx, read)
					if err != nil || !bytes.Equal(result.GetDocument().Data, request.GetPut().Data) {
						t.Fatal("raw document changed", err)
					}
					for _, action := range []string{"create", "replace", "delete"} {
						request := realMutation(t, f, "crud", 2)
						document := request.GetPut()
						switch action {
						case "create":
							request.Action = &pb.MutateRequest_Create{Create: document}
						case "replace":
							request.Action = &pb.MutateRequest_Replace{Replace: document}
						case "delete":
							empty := &pb.Empty{}
							request.Action = &pb.MutateRequest_Delete{Delete: empty}
						}
						mutation, err := f.client.Mutate(ctx, request)
						if err != nil || mutation.GetOutcome() != pb.MutationOutcome_APPLIED {
							t.Fatal("real CRUD", action, mutation, err)
						}
						read := &pb.ReadRequest{Resource: request.Resource}
						observed, err := f.client.Read(ctx, read)
						if err != nil || action == "delete" && observed.GetMissing() == nil || action != "delete" && !bytes.Equal(observed.GetDocument().GetData(), document.Data) {
							t.Fatal("real CRUD read", action, observed, err)
						}
					}
					bulk, err := f.client.Bulk(ctx)
					if err != nil {
						t.Fatal(err)
					}
					name, _, _ := protocolResource(f.root)
					open := &pb.BulkOpen{Store: "weir://" + name}
					variant := &pb.BulkRequestFrame_Open{Open: open}
					frame := &pb.BulkRequestFrame{Frame: variant}
					if err := bulk.Send(frame); err != nil {
						t.Fatal(err)
					}
					sent := make(chan error, 1)
					go func() {
						for index := uint64(0); index < 32; index++ {
							op := &pb.BulkOperation{Index: index}
							if index%2 == 0 {
								op.Operation = &pb.BulkOperation_Mutate{Mutate: realMutation(t, f, "peer", int(index))}
							} else {
								op.Operation = &pb.BulkOperation_Read{Read: read}
							}
							variant := &pb.BulkRequestFrame_Operation{Operation: op}
							frame := &pb.BulkRequestFrame{Frame: variant}
							if err := bulk.Send(frame); err != nil {
								sent <- err
								return
							}
						}
						sent <- bulk.CloseSend()
					}()
					var count uint64
					for {
						frame, err := bulk.Recv()
						if err != nil {
							t.Fatal(err)
						}
						if end := frame.GetEnd(); end != nil {
							if count != 32 || end.ReceivedCount != 32 || end.ResultCount != 32 {
								t.Fatal(end, count)
							}
							break
						}
						result := frame.GetResult()
						if result == nil {
							t.Fatal(frame)
						}
						if result.Index%2 == 0 {
							if result.GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
								t.Fatal(result)
							}
						} else {
							expected := realMutation(t, f, "peer", int(result.Index-1))
							if !bytes.Equal(result.GetRead().GetDocument().Data, expected.GetPut().Data) {
								t.Fatal("same-key order", result.Index)
							}
						}
						count++
					}
					if _, err := bulk.Recv(); err != io.EOF {
						t.Fatal(err)
					}
					if err := <-sent; err != nil {
						t.Fatal(err)
					}
					if f.backend != nil {
						f.backend.Do(t, "POST", "/"+f.backend.Index+"/_refresh", "")
					}
					scanReq := &pb.ScanRequest{Resource: f.root, FetchItemsHint: 1}
					scan, err := f.client.Scan(ctx, scanReq)
					if err != nil {
						t.Fatal(err)
					}
					count = 0
					for {
						frame, err := scan.Recv()
						if err != nil {
							t.Fatal(err)
						}
						if end := frame.GetEnd(); end != nil {
							if end.Failure != nil || end.DocumentCount != count || count < 1 {
								t.Fatal(end, count)
							}
							break
						}
						count++
					}
					if _, err := scan.Recv(); err != io.EOF {
						t.Fatal(err)
					}
					openNative, body := nativeRequest(t, f, false)
					stream := startNative(t, f, ctx, openNative)
					go func() { sent <- uploadNative(stream, body) }()
					raw, end, err := receiveNative(stream)
					if err != io.EOF || end.GetCompletion() != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil || len(raw) == 0 {
						t.Fatal(end, err)
					}
					if err := <-sent; err != nil {
						t.Fatal(err)
					}
					waitScanReleased(t, f)
					if sumLocalMetric(t, f, "weir_store_records_total") != 40 {
						t.Fatal("real logical operation count")
					}
					recordCalls := sampleLocalMetric(t, f, "weir_store_executions_total", map[string]string{"kind": "record"})
					if recordCalls != 40 {
						t.Fatal("real physical calls duplicated")
					}
					nativeComplete := sampleLocalMetric(t, f, "weir_store_native_completions_total", map[string]string{"completion": "response_complete"})
					scanComplete := sampleLocalMetric(t, f, "weir_store_scan_terminations_total", map[string]string{"result": "exhausted"})
					if nativeComplete != 1 || scanComplete != 1 {
						t.Fatal("real stream metrics")
					}
					for _, remote := range f.metricsRemotes {
						forwarded := testmetrics.ScrapeCollector(t, remote)
						if testmetrics.Sum(forwarded, "weir_relay_terminations_total") != 11 || forwarded["weir_store_executions_total"] != nil {
							t.Fatal("real relay duplicated execution")
						}
					}
					t.Logf("metrics: %s hops=%d CRUD/Bulk records=40 physical record calls=40 Native complete=1 Scan exhausted=1; each relay=11", kind, hops)
				})
			}
		})
	}
}

func TestPeerRealNativeEarlyResponse(t *testing.T) {
	for _, continuous := range []bool{false, true} {
		t.Run(fmt.Sprint(continuous), func(t *testing.T) {
			f := forwardFixture(t, scanServer(t, "search", DefaultLimits()), 2)
			open, body := nativeRequest(t, f, true)
			descriptor := &spb.Request{Method: "POST", Path: "/_bulk"}
			for range 160 {
				header := &spb.Header{Name: "x-opaque-id", Values: []string{strings.Repeat("x", 128)}}
				descriptor.Headers = append(descriptor.Headers, header)
			}
			open.Descriptor_.Data, _ = proto.Marshal(descriptor)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			stream := startNative(t, f, ctx, open)
			variant := &pb.NativeRequestFrame_Chunk{Chunk: body}
			frame := &pb.NativeRequestFrame{Frame: variant}
			if err := stream.Send(frame); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			if continuous {
				go func() {
					defer close(done)
					for {
						if stream.Send(frame) != nil {
							return
						}
					}
				}()
			} else {
				close(done)
			}
			head, err := stream.Recv()
			if err != nil || head.GetHead() == nil {
				t.Fatal(head, err)
			}
			response := &spb.Response{}
			if err := proto.Unmarshal(head.GetHead().Metadata.Data, response); err != nil || response.StatusCode < 400 {
				t.Fatal(response, err)
			}
			_, end, err := receiveNative(stream)
			if err != io.EOF || end.GetCompletion() != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil {
				t.Fatal(end, err)
			}
			cancel()
			<-done
			waitScanReleased(t, f)
		})
	}
}

// A real RPC fault endpoint: it waits for an acknowledged downstream mutation,
// then closes the actual upstream socket without delivering its result.
// Hop metadata is preserved, never invented or reset by this test fault endpoint.
type lostReplyPeer struct {
	transport *Server
	pb.UnimplementedWeirServer
	next           pb.WeirClient
	calls          atomic.Int32
	applied        atomic.Int32
	nativeComplete atomic.Int32
	lossBudget     *atomic.Int32
}

func (p *lostReplyPeer) Mutate(ctx context.Context, req *pb.MutateRequest) (*pb.MutationResult, error) {
	p.calls.Add(1)
	incoming, _ := metadata.FromIncomingContext(ctx)
	forwarded := metadata.MD{HopMetadata: incoming.Get(HopMetadata)}
	ctx = metadata.NewOutgoingContext(ctx, forwarded)
	result, err := p.next.Mutate(ctx, req)
	if err != nil {
		return nil, err
	}
	if result.Outcome == pb.MutationOutcome_APPLIED {
		p.applied.Add(1)
	}
	if p.lossBudget == nil || p.lossBudget.CompareAndSwap(1, 0) {
		p.transport.abortPeer(ctx)
		return nil, status.Error(codes.Unavailable, "injected loss after real acknowledgement")
	}
	return result, nil
}
func (p *lostReplyPeer) Native(stream grpc.BidiStreamingServer[pb.NativeRequestFrame, pb.NativeResponseFrame]) error {
	p.calls.Add(1)
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	incoming, _ := metadata.FromIncomingContext(ctx)
	forwarded := metadata.MD{HopMetadata: incoming.Get(HopMetadata)}
	ctx = metadata.NewOutgoingContext(ctx, forwarded)
	downstream, err := p.next.Native(ctx)
	if err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			frame, err := stream.Recv()
			if err == io.EOF {
				_ = downstream.CloseSend()
				return
			}
			if err != nil {
				return
			}
			if downstream.Send(frame) != nil {
				return
			}
		}
	}()
	drop := p.lossBudget == nil || p.lossBudget.CompareAndSwap(1, 0)
	var complete bool
	for {
		frame, err := downstream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if frame.GetEnd().GetCompletion() == pb.NativeCompletion_RESPONSE_COMPLETE {
			complete = true
		}
		if !drop {
			if err := stream.Send(frame); err != nil {
				return err
			}
		}
	}
	<-done
	if complete {
		p.nativeComplete.Add(1)
	}
	if drop {
		p.transport.abortPeer(stream.Context())
		return status.Error(codes.Unavailable, "injected loss after native completion")
	}
	return nil
}
func faultPeer(t *testing.T, address string) (string, *lostReplyPeer) {
	t.Helper()
	_, next := peerClient(t, address)
	limits := DefaultLimits()
	limits.Connections = 8
	admission, err := NewAdmission(limits)
	if err != nil {
		t.Fatal(err)
	}
	transport := &Server{admission: admission, metrics: newTransportMetrics()}
	proxy := &lostReplyPeer{next: next, transport: transport}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(300<<10), grpc.MaxSendMsgSize(300<<10))
	pb.RegisterWeirServer(srv, proxy)
	bounded := &limitedListener{Listener: listener, slots: admission.connections, server: transport}
	go func() { _ = srv.Serve(bounded) }()
	t.Cleanup(func() { srv.Stop(); _ = listener.Close() })
	return listener.Addr().String(), proxy
}

type backendFault struct {
	fixture  scanFixture
	mongo    *testmongo.Proxy
	writes   *atomic.Int32
	endpoint string
}

func faultBackend(t *testing.T, kind string, drop bool, native bool) backendFault {
	t.Helper()
	f := scanFixture{}
	var adapter execution.Adapter
	var err error
	calls := new(atomic.Int32)
	result := backendFault{writes: calls}
	if kind == "mongo" {
		f.mongo = testmongo.Open(t)
		proxy := testmongo.StartProxy(t, f.mongo)
		result.mongo = proxy
		result.endpoint = proxy.URI()
		if drop {
			proxy.DropCommand = "update"
			if native {
				proxy.DropCommand = "findAndModify"
			}
			proxy.DropRemaining.Store(1)
		}
		cfg := mongodb.Config{Store: kind, URI: proxy.URI(), Database: f.mongo.DB, Collection: "records", Pool: 1}
		adapter, err = mongodb.Open(context.Background(), cfg)
		f.root = "weir://mongo/" + f.mongo.DB + "/records"
	} else {
		f.backend = testsearch.Open(t)
		backend := f.backend
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			request, err := http.NewRequestWithContext(r.Context(), r.Method, backend.URL+r.URL.RequestURI(), r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(502)
				return
			}
			request.Header = r.Header.Clone()
			request.GetBody = nil
			response, err := backend.Client.Do(request)
			if err != nil {
				t.Error(err)
				w.WriteHeader(502)
				return
			}
			defer response.Body.Close()
			raw, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
			if err != nil {
				t.Error(err)
				w.WriteHeader(502)
				return
			}
			if strings.HasSuffix(r.URL.Path, "/_bulk") || strings.Contains(r.URL.Path, "/_update/") {
				calls.Add(1)
				if response.StatusCode != 200 || (!bytes.Contains(raw, []byte(`"result":"created"`)) && !bytes.Contains(raw, []byte(`"result":"updated"`))) {
					t.Error("write not acknowledged before fault", response.StatusCode)
				}
				if drop {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close()
					return
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(response.StatusCode)
			_, _ = w.Write(raw)
		})
		proxy := httptest.NewServer(handler)
		t.Cleanup(proxy.Close)
		result.endpoint = proxy.URL
		cfg := search.Config{Store: kind, URL: proxy.URL, Index: backend.Index, Profile: backend.Profile, Pool: 1}
		adapter, err = search.Open(context.Background(), cfg)
		f.root = "weir://search/" + backend.Index
	}
	if err != nil {
		t.Fatal(err)
	}
	limits := store.DefaultLimits()
	limits.Concurrency = 1
	f.runtime, err = store.New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	routes := map[string]*store.Runtime{kind: f.runtime}
	f.server, err = newLocalServer(t, routes, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	result.fixture = f
	return result
}
func TestPeerRealAcknowledgedReplyLossNoReplay(t *testing.T) {
	for _, kind := range []string{"mongo", "search"} {
		for _, leg := range []string{"database", "peer", "application"} {
			for _, mode := range []string{"ordinary", "native", "expression"} {
				native := mode == "native"
				t.Run(fmt.Sprintf("%s/%s/%s", kind, leg, mode), func(t *testing.T) {
					backend := faultBackend(t, kind, leg == "database", native)
					f := backend.fixture
					replica := replicaRuntime(t, f, backend.endpoint)
					f.replicas = append(f.replicas, replica)
					if mode == "expression" {
						if f.backend == nil {
							seed := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: 0}}
							if _, err := f.mongo.Admin.Database(f.mongo.DB).Collection("records").InsertOne(context.Background(), seed); err != nil {
								t.Fatal(err)
							}
						} else {
							code, _ := f.backend.Do(t, "PUT", "/"+f.backend.Index+"/_doc/counter", `{"n":0}`)
							if code != 201 {
								t.Fatal(code)
							}
						}
					}

					service := Service{LocalStore: f.runtime}
					routes := map[string]Service{kind: service}
					options := peerServerOptions{routes: routes, peer: true}
					_, address := startPeerServer(t, options)
					secondService := Service{LocalStore: replica}
					secondOptions := peerServerOptions{routes: map[string]Service{kind: secondService}, peer: true}
					_, secondAddress := startPeerServer(t, secondOptions)
					var loss, secondLoss *lostReplyPeer
					if leg == "peer" {
						address, loss = faultPeer(t, address)
						secondAddress, secondLoss = faultPeer(t, secondAddress)
						budget := new(atomic.Int32)
						budget.Store(1)
						loss.lossBudget, secondLoss.lossBudget = budget, budget
					}
					remote := multipleRemote(t, []string{address, secondAddress})
					remoteService := Service{RemoteWeir: remote}
					options = peerServerOptions{routes: map[string]Service{kind: remoteService}, budget: 4}
					_, address = startPeerServer(t, options)
					if leg == "application" {
						address, loss = faultPeer(t, address)
					}
					_, f.client = peerClient(t, address)
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					id := "loss"
					if native {
						id = "native"
						open, body := nativeRequest(t, f, true)
						stream := startNative(t, f, ctx, open)
						sent := make(chan error, 1)
						go func() { sent <- uploadNative(stream, body) }()
						_, end, err := receiveNative(stream)
						if err == io.EOF && (end == nil || end.Completion != pb.NativeCompletion_RESPONSE_INCOMPLETE) {
							t.Fatal("false native evidence", end, err)
						}
						if err := <-sent; err != nil && status.Code(err) != codes.Unavailable && err != io.EOF {
							t.Fatal(err)
						}
					} else {
						request := realMutation(t, f, id, 1)
						if mode == "expression" {
							id = "counter"
							request = realExpression(t, f, 1)
						}
						result, err := f.client.Mutate(ctx, request)
						if err == nil && (result == nil || result.Outcome != pb.MutationOutcome_UNKNOWN) {
							t.Fatal("false mutation evidence", result, err)
						}
					}
					if loss != nil {
						calls, applied, complete := loss.calls.Load(), loss.applied.Load(), loss.nativeComplete.Load()
						if secondLoss != nil {
							calls += secondLoss.calls.Load()
							applied += secondLoss.applied.Load()
							complete += secondLoss.nativeComplete.Load()
						}
						if calls != 1 || !native && applied != 1 || native && complete != 1 {
							t.Fatal("fault did not follow one positive acknowledgement across both receivers", calls, applied, complete)
						}
					}
					var commands int
					if backend.mongo != nil {
						command := "update"
						if native {
							command = "findAndModify"
						}
						for _, event := range backend.mongo.Events() {
							if event.Command == command {
								commands++
								if !event.Acknowledged || leg == "database" && !event.Dropped {
									t.Fatal("not an acknowledged reply loss", event)
								}
							}
						}
						filter := bson.D{{Key: "_id", Value: id}}
						var observed struct{ N int }
						if err := f.mongo.Admin.Database(f.mongo.DB).Collection("records").FindOne(ctx, filter).Decode(&observed); err != nil || observed.N != 1 {
							t.Fatal("real effect", observed.N, err)
						}
					} else {
						commands = int(backend.writes.Load())
						code, raw := f.backend.Do(t, "GET", "/"+f.backend.Index+"/_doc/"+id, "")
						var observed struct {
							Version int `json:"_version"`
							Found   bool
						}
						expectedVersion := 1
						if mode == "expression" {
							expectedVersion = 2
						}
						if code != 200 || json.Unmarshal(raw, &observed) != nil || !observed.Found || observed.Version != expectedVersion {
							t.Fatal("real effect/version", code, string(raw))
						}
					}
					if commands != 1 {
						t.Fatal("backend replayed", commands)
					}
					if !native {
						outcome := "applied"
						if leg == "database" {
							outcome = "unknown"
						}
						records := sampleLocalMetric(t, f, "weir_store_records_total", map[string]string{"operation": "mutate", "outcome": outcome})
						if records != 1 || sumLocalMetric(t, f, "weir_store_executions_total") != 1 {
							t.Fatal("response loss changed execution evidence")
						}
						forwarded := testmetrics.ScrapeCollector(t, remote)
						status := "ok"
						if leg == "peer" {
							status = "non_ok"
						}
						if testmetrics.Sample(forwarded, "weir_relay_terminations_total", map[string]string{"method": "Mutate", "status": status}).GetCounter().GetValue() != 1 {
							t.Fatal("relay observation boundary", status, forwarded["weir_relay_terminations_total"])
						}
					}
					t.Logf("%s %s Native=%t: acknowledged backend commands=%d, effect independently verified", kind, leg, native, commands)
				})
			}
		}
	}
}

func TestPeerRealNativeSlowDownload(t *testing.T) {
	testNativeSlowDownload(t, 2)
}
func TestPeerRealScanPartialFailure(t *testing.T) {
	testScanGRPCCompletionAndPartialFailure(t, 2)
}
