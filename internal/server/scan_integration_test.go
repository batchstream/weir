//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/mongostore"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/searchstore"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testmongo"
	"github.com/batchstream/weir/internal/testsearch"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type scanFixture struct {
	fixture
	backend *testsearch.Backend
	root    string
}

func scanServer(t *testing.T, kind string, sl Limits) scanFixture {
	t.Helper()
	f := scanFixture{}
	var adapter execution.Adapter
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	if kind == "mongo" {
		f.native, f.db = testmongo.Open(t)
		cfg := mongostore.Config{URI: testmongo.URI, Store: "mongo", Database: f.db, Collection: "records", Pool: 1}
		adapter, err = mongostore.Open(ctx, cfg)
		f.root = "weir://mongo/" + f.db + "/records"
	} else {
		f.backend = testsearch.Open(t)
		cfg := searchstore.Config{Store: "search", URL: f.backend.URL, Index: f.backend.Index, Profile: f.backend.Profile, Pool: 1}
		adapter, err = searchstore.Open(ctx, cfg)
		f.root = "weir://search/" + f.backend.Index
	}
	if err != nil {
		t.Fatal(err)
	}
	limits := store.DefaultLimits()
	limits.Concurrency = 1
	limits.Collect = 0
	f.runtime, err = store.New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	routes := map[string]*store.Runtime{kind: f.runtime}
	f.server, err = New(routes, sl)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.address = listener.Addr().String()
	go func() { _ = f.server.Serve(listener) }()
	f.conn, err = grpc.NewClient(f.address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithStaticStreamWindowSize(65535), grpc.WithStaticConnWindowSize(65535))
	if err != nil {
		t.Fatal(err)
	}
	f.client = pb.NewWeirClient(f.conn)
	t.Cleanup(func() {
		_ = f.conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := f.server.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return f
}
func (f scanFixture) seed(t *testing.T, sizes []int) {
	t.Helper()
	for i, size := range sizes {
		pad := strings.Repeat("x", size)
		if f.backend == nil {
			doc := bson.D{{Key: "_id", Value: fmt.Sprint(i)}, {Key: "n", Value: int64(9223372036854775807)}, {Key: "pad", Value: pad}}
			if _, err := f.native.Database(f.db).Collection("records").InsertOne(context.Background(), doc); err != nil {
				t.Fatal(err)
			}
		} else {
			body := map[string]any{"n": int64(9223372036854775807), "pad": pad}
			raw, _ := json.Marshal(body)
			status, _ := f.backend.Do(t, "PUT", fmt.Sprintf("/%s/_doc/%d", f.backend.Index, i), string(raw))
			if status != 201 {
				t.Fatal(status)
			}
		}
	}
	if f.backend != nil {
		f.backend.Do(t, "POST", "/"+f.backend.Index+"/_refresh", "")
	}
}
func receiveScan(t *testing.T, stream grpc.ServerStreamingClient[pb.ScanResponseFrame]) (uint64, *pb.ScanEnd, error) {
	t.Helper()
	var count uint64
	for {
		frame, err := stream.Recv()
		if err != nil {
			return count, nil, err
		}
		if end := frame.GetEnd(); end != nil {
			if end.DocumentCount != count {
				t.Fatal("count mismatch", count, end)
			}
			_, err = stream.Recv()
			return count, end, err
		}
		if frame.GetDocument() == nil {
			t.Fatal("invalid Scan frame")
		}
		count++
	}
}
func waitScanReleased(t *testing.T, f scanFixture) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		snap := f.runtime.Snapshot()
		if snap.Active == 0 && snap.Retained == 0 && snap.Pending == 0 && snap.PendingBytes == 0 && snap.ResultBytes == 0 && snap.ScanSessions == 0 && len(f.server.slots) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Scan leaked", f.runtime.Snapshot(), len(f.server.slots))
}
func TestScanGRPCCompletionAndPartialFailure(t *testing.T) {
	for _, kind := range []string{"mongo", "search"} {
		t.Run(kind, func(t *testing.T) {
			for _, bad := range []bool{false, true} {
				sl := DefaultLimits()
				f := scanServer(t, kind, sl)
				sizes := []int{0, 1, 100}
				if bad {
					sizes[1] = protocol.MaxDocument + 1
				}
				f.seed(t, sizes)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				req := &pb.ScanRequest{Resource: f.root, FetchItemsHint: 1}
				stream, err := f.client.Scan(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				count, end, err := receiveScan(t, stream)
				cancel()
				if err != io.EOF || end == nil {
					t.Fatal("no final OK/End", end, err)
				}
				if bad {
					if count != 1 || end.Failure == nil || end.Failure.Code != pb.FailureCode_RESOURCE_EXHAUSTED {
						t.Fatal(count, end)
					}
				} else if count != 3 || end.Failure != nil {
					t.Fatal(count, end)
				}
				waitScanReleased(t, f)
			}
		})
	}
}
func TestScanSlowConsumerC1AndSessionLimit(t *testing.T) {
	for _, kind := range []string{"mongo", "search"} {
		t.Run(kind, func(t *testing.T) {
			sl := DefaultLimits()
			sl.Stall = time.Second
			f := scanServer(t, kind, sl)
			f.seed(t, []int{200 << 10, 200 << 10, 200 << 10, 200 << 10, 200 << 10, 200 << 10})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req := &pb.ScanRequest{Resource: f.root, FetchItemsHint: 2}
			stream, err := f.client.Scan(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Header(); err != nil {
				t.Fatal(err)
			}
			time.Sleep(40 * time.Millisecond)
			snap := f.runtime.Snapshot()
			if snap.Active != 0 || snap.ScanSessions != 1 || snap.ScanPages != 1 || snap.Pending != 1 || snap.Window != 1 {
				t.Fatal("scan holds execution permit", snap)
			}
			// A different connection removes connection-level receive flow control from
			// this scheduler test. It does not assert hard latency or resource isolation.
			conn, err := grpc.NewClient(f.address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			client := pb.NewWeirClient(conn)
			fast, stop := context.WithTimeout(context.Background(), 700*time.Millisecond)
			defer stop()
			read := &pb.ReadRequest{Resource: f.root + "/s:missing"}
			result, err := client.Read(fast, read)
			if err != nil || result.GetMissing() == nil {
				t.Fatal("read blocked by slow Scan", result, err)
			}
			doc := &pb.Document{MediaType: "application/json", Data: []byte(`{"n":1}`)}
			if kind == "mongo" {
				native := bson.D{{Key: "_id", Value: "short"}, {Key: "n", Value: int32(1)}}
				raw, err := bson.Marshal(native)
				if err != nil {
					t.Fatal(err)
				}
				doc = &pb.Document{MediaType: "application/bson", Data: raw}
			}
			action := &pb.MutateRequest_Create{Create: doc}
			mutation := &pb.MutateRequest{Resource: f.root + "/s:short", Action: action}
			written, err := client.Mutate(fast, mutation)
			if err != nil || written.GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal("mutation blocked by slow Scan", written, err)
			}
			other, err := client.Scan(fast, req)
			if err != nil {
				t.Fatal(err)
			}
			count, end, err := receiveScan(t, other)
			if err != io.EOF || count != 0 || end.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
				t.Fatal("session cap", end, err)
			}
			cancel()
			_, _, _ = receiveScan(t, stream)
			waitScanReleased(t, f)
		})
	}
}
func TestScanTransportStallLifetimeAndDrain(t *testing.T) {
	for _, kind := range []string{"mongo", "search"} {
		t.Run(kind, func(t *testing.T) {
			for _, mode := range []string{"stall", "lifetime", "drain"} {
				t.Run(mode, func(t *testing.T) {
					sl := DefaultLimits()
					sl.Stall = 100 * time.Millisecond
					sl.ScanLifetime = 2 * time.Second
					if mode == "lifetime" {
						sl.Stall = 2 * time.Second
						sl.ScanLifetime = 100 * time.Millisecond
					}
					f := scanServer(t, kind, sl)
					f.seed(t, []int{200 << 10, 200 << 10, 200 << 10, 200 << 10})
					req := &pb.ScanRequest{Resource: f.root, FetchItemsHint: 1}
					stream, err := f.client.Scan(context.Background(), req)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := stream.Header(); err != nil {
						t.Fatal(err)
					}
					if mode == "drain" {
						ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
						err = f.server.Shutdown(ctx)
						cancel()
						if err != nil {
							t.Fatal(err)
						}
					}
					time.Sleep(600 * time.Millisecond)
					_, end, err := receiveScan(t, stream)
					if end != nil && end.Failure == nil && err == io.EOF {
						t.Fatal("stalled stream became complete+OK")
					}
					if err == io.EOF {
						t.Fatal("expected transport truncation", end)
					}
					waitScanReleased(t, f)
				})
			}
		})
	}
}
func TestScanRepeatedCancelResourceSamples(t *testing.T) {
	for _, kind := range []string{"mongo", "search"} {
		t.Run(kind, func(t *testing.T) {
			sl := DefaultLimits()
			sl.Stall = 150 * time.Millisecond
			f := scanServer(t, kind, sl)
			f.seed(t, []int{200 << 10, 200 << 10, 200 << 10})
			baseline := runtime.NumGoroutine()
			for round := 0; round < 12; round++ {
				ctx, cancel := context.WithCancel(context.Background())
				req := &pb.ScanRequest{Resource: f.root, FetchItemsHint: 1}
				stream, err := f.client.Scan(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				if round%3 == 0 {
					_, err = stream.Recv()
					if err != nil {
						t.Fatal(err)
					}
				} else if round%3 == 1 {
					_, _ = stream.Header()
				}
				cancel()
				_, _, _ = receiveScan(t, stream)
				waitScanReleased(t, f)
				if round%4 == 3 {
					runtime.GC()
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					connections := 0
					f.server.connections.Range(func(_, _ any) bool { connections++; return true })
					t.Logf("round=%d HeapAlloc=%d Sys-HeapReleased=%d goroutines=%d baseline=%d connections=%d snapshot=%+v", round+1, m.HeapAlloc, m.Sys-m.HeapReleased, runtime.NumGoroutine(), baseline, connections, f.runtime.Snapshot())
					if connections > 1 {
						t.Fatal("connection growth", connections)
					}
				}
			}
			if runtime.NumGoroutine() > baseline+20 {
				t.Fatal("goroutine growth", baseline, runtime.NumGoroutine())
			}
		})
	}
}

func TestScanWireInputAndBlockedTerminal(t *testing.T) {
	for _, mode := range []string{"no_data", "partial_prefix", "partial_body", "terminal_zero_window"} {
		t.Run(mode, func(t *testing.T) {
			sl := DefaultLimits()
			sl.ScanLifetime = 2 * time.Second
			sl.Stall = 100 * time.Millisecond
			f := scanServer(t, "mongo", sl)
			conn, err := net.DialTimeout("tcp", f.address, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
				t.Fatal(err)
			}
			framer := http2.NewFramer(conn, conn)
			framer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
			window := uint32(65535)
			if mode == "terminal_zero_window" {
				window = 0
			}
			setting := http2.Setting{ID: http2.SettingInitialWindowSize, Val: window}
			if err := framer.WriteSettings(setting); err != nil {
				t.Fatal(err)
			}
			var block bytes.Buffer
			encoder := hpack.NewEncoder(&block)
			fields := []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"}, {Name: ":authority", Value: f.address}, {Name: ":path", Value: pb.Weir_Scan_FullMethodName}, {Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"}}
			for _, field := range fields {
				if err := encoder.WriteField(field); err != nil {
					t.Fatal(err)
				}
			}
			header := http2.HeadersFrameParam{StreamID: 1, BlockFragment: block.Bytes(), EndHeaders: true}
			if err := framer.WriteHeaders(header); err != nil {
				t.Fatal(err)
			}
			req := &pb.ScanRequest{Resource: f.root}
			payload, _ := proto.Marshal(req)
			framed := make([]byte, len(payload)+5)
			binary.BigEndian.PutUint32(framed[1:5], uint32(len(payload)))
			copy(framed[5:], payload)
			switch mode {
			case "partial_prefix":
				framed = framed[:2]
			case "partial_body":
				framed = framed[:len(framed)-1]
			case "no_data":
				framed = nil
			}
			if framed != nil {
				if err := framer.WriteData(1, mode == "terminal_zero_window", framed); err != nil {
					t.Fatal(err)
				}
			}
			wire := &unaryWire{conn: conn, framer: framer}
			start := time.Now()
			reply := wire.receive(t)
			if !reply.reset && reply.status == "0" {
				t.Fatal("incomplete Scan transport succeeded", reply)
			}
			if time.Since(start) > 500*time.Millisecond {
				t.Fatal("stall did not protect input/terminal", time.Since(start))
			}
			waitScanReleased(t, f)
			t.Logf("%s exited in %s with reset=%v status=%s", mode, time.Since(start), reply.reset, reply.status)
		})
	}
}

func TestScanDualStoreIsolation(t *testing.T) {
	b := testsearch.Open(t)
	sl := DefaultLimits()
	sl.Stall = time.Second
	f := dualServer(t, b, b.URL, sl)
	mongo := scanFixture{fixture: f.fixture, root: "weir://mongo/" + f.db + "/records"}
	searchBase := f.fixture
	searchBase.runtime = f.search
	search := scanFixture{fixture: searchBase, backend: b, root: "weir://search/" + b.Index}
	mongo.seed(t, []int{200 << 10, 200 << 10})
	search.seed(t, []int{200 << 10, 200 << 10})
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	reqA := &pb.ScanRequest{Resource: mongo.root, FetchItemsHint: 1}
	reqB := &pb.ScanRequest{Resource: search.root, FetchItemsHint: 1}
	a, err := f.client.Scan(ctxA, reqA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Header(); err != nil {
		t.Fatal(err)
	}
	streamB, err := f.client.Scan(ctxB, reqB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := streamB.Header(); err != nil {
		t.Fatal(err)
	}
	if f.runtime.Snapshot().ScanSessions != 1 || f.search.Snapshot().ScanSessions != 1 {
		t.Fatal("stores share Scan session budget")
	}
	cancelA()
	_, _, _ = receiveScan(t, a)
	until := time.Now().Add(500 * time.Millisecond)
	for f.runtime.Snapshot().ScanSessions != 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if f.runtime.Snapshot().ScanSessions != 0 || f.search.Snapshot().ScanSessions != 1 {
		t.Fatal("one store cancellation poisoned other", f.runtime.Snapshot(), f.search.Snapshot())
	}
	cancelB()
	_, _, _ = receiveScan(t, streamB)
	waitScanReleased(t, search)
}
