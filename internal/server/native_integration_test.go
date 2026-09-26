//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	spb "github.com/batchstream/weir/api/weir/search/v1"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/mongostore"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/searchstore"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func nativeRequest(t *testing.T, f scanFixture, write bool) (*pb.NativeOpen, []byte) {
	t.Helper()
	open := &pb.NativeOpen{Resource: f.root}
	var body []byte
	if f.backend == nil {
		open.Descriptor_ = &pb.Document{MediaType: mongostore.NativeDescriptor}
		open.BodyMediaType = "application/bson"
		command := bson.D{{Key: "count", Value: "records"}}
		if write {
			query := bson.D{{Key: "_id", Value: "native"}}
			change := bson.D{{Key: "n", Value: 1}}
			update := bson.D{{Key: "$inc", Value: change}}
			command = bson.D{{Key: "findAndModify", Value: "records"}, {Key: "query", Value: query}, {Key: "update", Value: update}, {Key: "upsert", Value: true}, {Key: "new", Value: true}}
		}
		body, _ = bson.Marshal(command)
	} else {
		descriptor := &spb.Request{Method: "GET", Path: "/_doc/0"}
		if write {
			descriptor.Method = "POST"
			descriptor.Path = "/_bulk"
			open.BodyMediaType = "application/x-ndjson"
			body = []byte("{\"create\":{\"_id\":\"native\"}}\n{\"n\":1}\n")
		}
		raw, _ := proto.Marshal(descriptor)
		open.Descriptor_ = &pb.Document{MediaType: searchstore.NativeDescriptor, Data: raw}
	}
	return open, body
}
func startNative(t *testing.T, f scanFixture, ctx context.Context, open *pb.NativeOpen) grpc.BidiStreamingClient[pb.NativeRequestFrame, pb.NativeResponseFrame] {
	t.Helper()
	stream, err := f.client.Native(ctx)
	if err != nil {
		t.Fatal(err)
	}
	variant := &pb.NativeRequestFrame_Open{Open: open}
	frame := &pb.NativeRequestFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		t.Fatal(err)
	}
	return stream
}
func uploadNative(stream grpc.BidiStreamingClient[pb.NativeRequestFrame, pb.NativeResponseFrame], body []byte) error {
	for len(body) > 0 {
		n := min(protocol.NativeChunk, len(body))
		variant := &pb.NativeRequestFrame_Chunk{Chunk: body[:n]}
		frame := &pb.NativeRequestFrame{Frame: variant}
		if err := stream.Send(frame); err != nil {
			return err
		}
		body = body[n:]
	}
	return stream.CloseSend()
}
func receiveNative(stream grpc.BidiStreamingClient[pb.NativeRequestFrame, pb.NativeResponseFrame]) ([]byte, *pb.NativeEnd, error) {
	var body bytes.Buffer
	headSeen := false
	for {
		frame, err := stream.Recv()
		if err != nil {
			return body.Bytes(), nil, err
		}
		if end := frame.GetEnd(); end != nil {
			_, err = stream.Recv()
			return body.Bytes(), end, err
		}
		if frame.GetHead() != nil {
			if headSeen || body.Len() != 0 {
				return nil, nil, fmt.Errorf("invalid Head order")
			}
			headSeen = true
			continue
		}
		chunk, ok := frame.Frame.(*pb.NativeResponseFrame_Chunk)
		if !ok || len(chunk.Chunk) == 0 || len(chunk.Chunk) > protocol.NativeChunk {
			return nil, nil, fmt.Errorf("invalid chunk")
		}
		body.Write(chunk.Chunk)
	}
}
func TestNativeGRPCNormalAndNativeErrors(t *testing.T) {
	for _, kind := range []string{"mongo", "search"} {
		t.Run(kind, func(t *testing.T) {
			sl := DefaultLimits()
			f := scanServer(t, kind, sl)
			f.seed(t, []int{150 << 10})
			for _, write := range []bool{false, true, true} {
				open, body := nativeRequest(t, f, write)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				stream := startNative(t, f, ctx, open)
				sent := make(chan error, 1)
				go func() { sent <- uploadNative(stream, body) }()
				raw, end, err := receiveNative(stream)
				cancel()
				if err != io.EOF || end == nil || end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil {
					t.Fatal(end, err)
				}
				if err := <-sent; err != nil {
					t.Fatal(err)
				}
				if len(raw) == 0 {
					t.Fatal("empty native reply")
				}
				if f.backend != nil && write && strings.Contains(string(raw), "version_conflict") {
					t.Log("complete native mixed/error response retained")
				}
				waitScanReleased(t, f)
			}
		})
	}
}
func TestNativeEarlyResponseBeforeHalfClose(t *testing.T) {
	for _, continuous := range []bool{false, true} {
		t.Run(fmt.Sprint(continuous), func(t *testing.T) {
			sl := DefaultLimits()
			sl.Stall = 500 * time.Millisecond
			f := scanServer(t, "search", sl)
			open, body := nativeRequest(t, f, true)
			// Exceed the fixed backend's native HTTP header bound without ending the
			// chunked body. Unlike REST validation this happens before body aggregation.
			header := &spb.Header{Name: "x-opaque-id"}
			descriptor := &spb.Request{Method: "POST", Path: "/_bulk"}
			for range 160 {
				h := &spb.Header{Name: header.Name, Values: []string{strings.Repeat("x", 128)}}
				descriptor.Headers = append(descriptor.Headers, h)
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
			var sending chan error
			if continuous {
				sending = make(chan error, 1)
				go func() {
					for {
						if err := stream.Send(frame); err != nil {
							sending <- err
							return
						}
					}
				}()
			}
			first, err := stream.Recv()
			if err != nil || first.GetHead() == nil {
				t.Fatal(first, err)
			}
			metadata := &spb.Response{}
			if err := proto.Unmarshal(first.GetHead().Metadata.Data, metadata); err != nil || metadata.StatusCode < 400 {
				t.Fatal(metadata, err)
			}
			raw, end, err := receiveNative(stream)
			if err != io.EOF || end == nil || end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil {
				t.Fatal(end, err, string(raw))
			}
			t.Log("real backend header rejection before upload half-close", metadata.StatusCode, string(raw))
			cancel()
			if sending != nil {
				select {
				case <-sending:
				case <-time.After(time.Second):
					t.Fatal("early response left sender blocked")
				}
			}
			waitScanReleased(t, f)
		})
	}
}

func TestNativeStallCancelDrainAndSharedSession(t *testing.T) {
	for _, kind := range []string{"mongo", "search"} {
		for _, mode := range []string{"stall", "lifetime", "cancel", "drain"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				sl := DefaultLimits()
				sl.Stall = 120 * time.Millisecond
				if mode == "lifetime" {
					sl.Stall = time.Second
					sl.NativeLifetime = 120 * time.Millisecond
				}
				f := scanServer(t, kind, sl)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				open, _ := nativeRequest(t, f, true)
				stream := startNative(t, f, ctx, open)
				until := time.Now().Add(time.Second)
				for f.runtime.Snapshot().Active == 0 && time.Now().Before(until) {
					time.Sleep(time.Millisecond)
				}
				if f.runtime.Snapshot().NativeSessions != 1 || f.runtime.Snapshot().Active != 1 {
					t.Fatal(f.runtime.Snapshot())
				}
				req := &pb.ScanRequest{Resource: f.root}
				scan, err := f.client.Scan(context.Background(), req)
				if err != nil {
					t.Fatal(err)
				}
				frame, err := scan.Recv()
				if err != nil || frame.GetEnd().GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
					t.Fatal(frame, err)
				}
				second := startNative(t, f, context.Background(), open)
				_, end, err := receiveNative(second)
				if err != io.EOF || end == nil || end.Completion != pb.NativeCompletion_NATIVE_NOT_STARTED {
					t.Fatal(end, err)
				}
				if mode == "cancel" {
					cancel()
				}
				if mode == "drain" {
					drain, done := context.WithTimeout(context.Background(), 20*time.Millisecond)
					defer done()
					go f.server.Shutdown(drain)
				}
				_, end, err = receiveNative(stream)
				if err == io.EOF && (end == nil || end.Completion == pb.NativeCompletion_RESPONSE_COMPLETE) {
					t.Fatal("stalled request completed", end)
				}
				waitScanReleased(t, f)
				if mode != "drain" {
					req := &pb.ReadRequest{Resource: f.root + "/s:absent"}
					if _, err := f.client.Read(context.Background(), req); err != nil {
						t.Fatal("ordinary operation did not recover", err)
					}
				}
			})
		}
	}
}
func TestNativeSlowDownload(t *testing.T) {
	for _, kind := range []string{"mongo", "search"} {
		for _, mode := range []string{"stall", "lifetime"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				sl := DefaultLimits()
				sl.Stall = 100 * time.Millisecond
				sl.NativeLifetime = 2 * time.Second
				if mode == "lifetime" {
					sl.Stall = time.Second
					sl.NativeLifetime = 100 * time.Millisecond
				}
				f := scanServer(t, kind, sl)
				f.seed(t, []int{200 << 10})
				open, body := nativeRequest(t, f, false)
				if kind == "mongo" {
					query := bson.D{{Key: "_id", Value: "0"}}
					change := bson.D{{Key: "n", Value: int64(1)}}
					update := bson.D{{Key: "$set", Value: change}}
					command := bson.D{{Key: "findAndModify", Value: "records"}, {Key: "query", Value: query}, {Key: "update", Value: update}}
					body, _ = bson.Marshal(command)
				}
				stream := startNative(t, f, context.Background(), open)
				go uploadNative(stream, body)
				if _, err := stream.Header(); err != nil {
					t.Fatal(err)
				}
				time.Sleep(600 * time.Millisecond)
				_, end, err := receiveNative(stream)
				if err == io.EOF && end != nil && end.Completion == pb.NativeCompletion_RESPONSE_COMPLETE {
					t.Fatal("completed after output deadline")
				}
				waitScanReleased(t, f)
			})
		}
	}
}

func TestNativeWireInputAndBlockedTerminal(t *testing.T) {
	for _, mode := range []string{"no_data", "partial_prefix", "partial_body", "terminal_zero_window"} {
		t.Run(mode, func(t *testing.T) {
			sl := DefaultLimits()
			sl.NativeLifetime = 2 * time.Second
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
			fields := []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"}, {Name: ":authority", Value: f.address}, {Name: ":path", Value: pb.Weir_Native_FullMethodName}, {Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"}}
			for _, field := range fields {
				if err := encoder.WriteField(field); err != nil {
					t.Fatal(err)
				}
			}
			header := http2.HeadersFrameParam{StreamID: 1, BlockFragment: block.Bytes(), EndHeaders: true}
			if err := framer.WriteHeaders(header); err != nil {
				t.Fatal(err)
			}
			descriptor := &pb.Document{MediaType: "unsupported"}
			open := &pb.NativeOpen{Resource: f.root, Descriptor_: descriptor}
			variant := &pb.NativeRequestFrame_Open{Open: open}
			req := &pb.NativeRequestFrame{Frame: variant}
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
				t.Fatal("incomplete Native transport succeeded", reply)
			}
			if time.Since(start) > 500*time.Millisecond {
				t.Fatal("stall did not protect input/terminal", time.Since(start))
			}
			waitScanReleased(t, f)
			t.Logf("%s exited in %s with reset=%v status=%s", mode, time.Since(start), reply.reset, reply.status)
		})
	}
}

func TestNativeRepeatedCancelResourceSamples(t *testing.T) {
	for _, kind := range []string{"mongo", "search"} {
		t.Run(kind, func(t *testing.T) {
			sl := DefaultLimits()
			f := scanServer(t, kind, sl)
			baseline := runtime.NumGoroutine()
			for round := 0; round < 12; round++ {
				open, _ := nativeRequest(t, f, true)
				ctx, cancel := context.WithCancel(context.Background())
				stream := startNative(t, f, ctx, open)
				until := time.Now().Add(time.Second)
				for f.runtime.Snapshot().Active == 0 && time.Now().Before(until) {
					time.Sleep(time.Millisecond)
				}
				cancel()
				receiveNative(stream)
				waitScanReleased(t, f)
				if round%4 == 3 {
					runtime.GC()
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					t.Logf("round=%d HeapAlloc=%d goroutines=%d baseline=%d snapshot=%+v", round+1, m.HeapAlloc, runtime.NumGoroutine(), baseline, f.runtime.Snapshot())
				}
			}
			if runtime.NumGoroutine() > baseline+12 {
				t.Fatal("goroutine growth", baseline, runtime.NumGoroutine())
			}
		})
	}
}

func TestNativeGRPCMultiframeUploadAndInvalidFrames(t *testing.T) {
	for _, kind := range []string{"mongo", "search"} {
		t.Run(kind, func(t *testing.T) {
			sl := DefaultLimits()
			f := scanServer(t, kind, sl)
			for _, mode := range []string{"multiframe", "empty", "oversized", "second_open"} {
				open, body := nativeRequest(t, f, true)
				if mode == "multiframe" {
					if kind == "mongo" {
						query := bson.D{{Key: "_id", Value: "large"}}
						change := bson.D{{Key: "pad", Value: strings.Repeat("x", 150<<10)}}
						update := bson.D{{Key: "$set", Value: change}}
						command := bson.D{{Key: "findAndModify", Value: "records"}, {Key: "query", Value: query}, {Key: "update", Value: update}, {Key: "upsert", Value: true}, {Key: "new", Value: true}}
						body, _ = bson.Marshal(command)
					} else {
						body = []byte("{\"create\":{\"_id\":\"large\"}}\n{\"pad\":\"" + strings.Repeat("x", 150<<10) + "\"}\n")
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				stream := startNative(t, f, ctx, open)
				sent := make(chan error, 1)
				go func() {
					if mode == "multiframe" {
						sent <- uploadNative(stream, body)
						return
					}
					variant := &pb.NativeRequestFrame_Chunk{}
					if mode == "oversized" {
						variant.Chunk = make([]byte, protocol.NativeChunk+1)
					}
					frame := &pb.NativeRequestFrame{Frame: variant}
					if mode == "second_open" {
						frame.Frame = &pb.NativeRequestFrame_Open{Open: open}
					}
					err := stream.Send(frame)
					if err == nil {
						err = stream.CloseSend()
					}
					sent <- err
				}()
				_, end, err := receiveNative(stream)
				cancel()
				<-sent
				expected := pb.NativeCompletion_NATIVE_NOT_STARTED
				if mode == "multiframe" {
					expected = pb.NativeCompletion_RESPONSE_COMPLETE
				}
				if err != io.EOF || end == nil || end.Completion != expected {
					t.Fatal(mode, end, err)
				}
				waitScanReleased(t, f)
			}
		})
	}
}
