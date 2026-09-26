//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestUnaryAppliedMutationLosesBlockedReply(t *testing.T) {
	limits := DefaultLimits()
	limits.UnaryLifetime = 100 * time.Millisecond
	limits.Stall = 100 * time.Millisecond
	f := setupWithLimits(t, true, limits)
	conn, err := net.DialTimeout("tcp", f.address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatal(err)
	}
	framer := http2.NewFramer(conn, conn)
	framer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	// Unlike grpc-go's minimum 64 KiB window, a raw HTTP/2 zero window can also
	// stop a tiny mutation acknowledgement. The request itself is still delivered.
	setting := http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0}
	if err := framer.WriteSettings(setting); err != nil {
		t.Fatal(err)
	}
	var headers bytes.Buffer
	encoder := hpack.NewEncoder(&headers)
	fields := []hpack.HeaderField{
		{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"},
		{Name: ":authority", Value: f.address}, {Name: ":path", Value: pb.Weir_Mutate_FullMethodName},
		{Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"},
	}
	for _, field := range fields {
		if err := encoder.WriteField(field); err != nil {
			t.Fatal(err)
		}
	}
	params := http2.HeadersFrameParam{StreamID: 1, BlockFragment: headers.Bytes(), EndHeaders: true}
	if err := framer.WriteHeaders(params); err != nil {
		t.Fatal(err)
	}
	request := mutation(f, "applied-without-reply", 1)
	request.Action = &pb.MutateRequest_Create{Create: request.GetPut()}
	message, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	envelope := make([]byte, 5+len(message))
	binary.BigEndian.PutUint32(envelope[1:5], uint32(len(message)))
	copy(envelope[5:], message)
	start := time.Now()
	if err := framer.WriteData(1, true, envelope); err != nil {
		t.Fatal(err)
	}
	reset := false
	for !reset {
		frame, err := framer.ReadFrame()
		if err != nil {
			t.Fatal("server failed to reset an expired reply", err)
		}
		switch frame := frame.(type) {
		case *http2.SettingsFrame:
			if !frame.IsAck() {
				if err := framer.WriteSettingsAck(); err != nil {
					t.Fatal(err)
				}
			}
		case *http2.PingFrame:
			if !frame.IsAck() {
				if err := framer.WritePing(true, frame.Data); err != nil {
					t.Fatal(err)
				}
			}
		case *http2.DataFrame:
			if frame.StreamID == 1 && len(frame.Data()) != 0 {
				t.Fatal("response bypassed zero receive window")
			}
		case *http2.MetaHeadersFrame:
			for _, field := range frame.Fields {
				if field.Name == "grpc-status" && field.Value == "0" {
					t.Fatal("undelivered mutation reported gRPC OK")
				}
			}
		case *http2.RSTStreamFrame:
			if frame.StreamID == 1 {
				reset = true
			}
		}
	}
	if time.Since(start) > 400*time.Millisecond {
		t.Fatal("server-side unary timeout was not bounded", time.Since(start))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	filter := bson.D{{Key: "_id", Value: "applied-without-reply"}}
	raw, err := f.native.Database(f.db).Collection("records").FindOne(ctx, filter).Raw()
	if err != nil || raw.Lookup("n").AsInt64() != 1 {
		t.Fatal("test must confirm the real mutation applied despite its missing reply", err, raw)
	}
	for i := 0; i < 100 && (len(f.server.slots) != 0 || f.runtime.Snapshot().Retained != 0); i++ {
		time.Sleep(time.Millisecond)
	}
	if len(f.server.slots) != 0 || f.runtime.Snapshot().Retained != 0 {
		t.Fatal("lost reply retained credits", len(f.server.slots), f.runtime.Snapshot())
	}
	t.Log("real mutation applied; zero-window reply reset; client outcome remains UNKNOWN")
}

func TestUnaryResultHandoffAfterTransportClosed(t *testing.T) {
	f := setup(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req := &pb.ReadRequest{Resource: resource(f, "missing")}
	variant := &pb.BulkOperation_Read{Read: req}
	op := &pb.BulkOperation{Operation: variant}
	plan, failure := f.runtime.Prepare(op)
	if failure != nil {
		t.Fatal(failure)
	}
	ticket, failure, _ := f.runtime.Submit(ctx, plan, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	if _, err := ticket.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	state := &delivery{}
	state.finish()
	state.retain(ticket)
	if f.runtime.Snapshot().Retained != 0 {
		t.Fatal("late result handoff leaked credit", f.runtime.Snapshot())
	}
}

func TestUnaryStalledStreamDoesNotBlockOtherStreams(t *testing.T) {
	limits := DefaultLimits()
	limits.UnaryLifetime = time.Second
	limits.Stall = 100 * time.Millisecond
	f := setupWithLimits(t, true, limits)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	doc := bson.D{{Key: "_id", Value: "large"}, {Key: "data", Value: make([]byte, 200<<10)}}
	if _, err := f.native.Database(f.db).Collection("records").InsertOne(ctx, doc); err != nil {
		t.Fatal(err)
	}
	stream := startPausedUnaryRead(t, f, ctx)
	var connection any
	f.server.connections.Range(func(_, v any) bool { connection = v; return false })
	req := &pb.ReadRequest{Resource: resource(f, "independent")}
	if result, err := f.client.Read(ctx, req); err != nil || result.GetMissing() == nil {
		t.Fatal("stalled stream blocked an independent unary", err, result)
	}
	time.Sleep(250 * time.Millisecond)
	present := false
	f.server.connections.Range(func(_, v any) bool { present = present || v == connection; return true })
	if !present {
		t.Fatal("stream write timeout unnecessarily killed its connection")
	}
	if result, err := f.client.Read(ctx, req); err != nil || result.GetMissing() == nil {
		t.Fatal("healthy stream failed after another stream timed out", err, result)
	}
	var result pb.ReadResult
	if err := stream.RecvMsg(&result); err == nil {
		t.Fatal("stalled unary was successful")
	}
}

func TestUnaryRejectedFramesReleaseDeliverySlot(t *testing.T) {
	f := setup(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request := &pb.ReadRequest{Resource: string(bytes.Repeat([]byte("x"), 310<<10))}
	if _, err := f.client.Read(ctx, request); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("oversized unary must be rejected", err)
	}
	empty := &pb.Empty{}
	var response pb.Empty
	for _, method := range []string{"/weir.v1.Weir/Unregistered", "/weir.v1.Weir/Read?alias=1", "/weir.v1.Weir/%52ead"} {
		err := f.conn.Invoke(ctx, method, empty, &response)
		if status.Code(err) != codes.Unimplemented {
			t.Fatal("unregistered or noncanonical method", method, err)
		}
	}
	for i := 0; i < 100 && len(f.server.slots) != 0; i++ {
		time.Sleep(time.Millisecond)
	}
	if len(f.server.slots) != 0 || f.runtime.Snapshot().Retained != 0 {
		t.Fatal("rejection leaked delivery resources", len(f.server.slots), f.runtime.Snapshot())
	}
}

func TestUnaryLifetimeIncludesHandlerTime(t *testing.T) {
	limits := DefaultLimits()
	limits.UnaryLifetime = 200 * time.Millisecond
	limits.Stall = 2 * time.Second
	f := setupWithLimits(t, true, limits)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	doc := bson.D{{Key: "_id", Value: "large"}, {Key: "data", Value: make([]byte, 200<<10)}}
	if _, err := f.native.Database(f.db).Collection("records").InsertOne(ctx, doc); err != nil {
		t.Fatal(err)
	}
	data := bson.D{{Key: "failCommands", Value: bson.A{"find"}}, {Key: "appName", Value: "weir:" + f.db}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 140}}
	testmongo.FailCommand(t, f.native, data, 1)
	start := time.Now()
	stream := startPausedUnaryRead(t, f, ctx)
	if time.Since(start) < 130*time.Millisecond {
		t.Fatal("backend delay not exercised")
	}
	for time.Since(start) < 280*time.Millisecond && (len(f.server.slots) != 0 || f.runtime.Snapshot().Retained != 0) {
		time.Sleep(time.Millisecond)
	}
	if len(f.server.slots) != 0 || f.runtime.Snapshot().Retained != 0 {
		t.Fatal("handler return restarted the unary lifetime", time.Since(start), f.runtime.Snapshot())
	}
	var result pb.ReadResult
	if err := stream.RecvMsg(&result); err == nil {
		t.Fatal("expired response became successful")
	}
	t.Log("backend work and blocked response share original lifetime:", time.Since(start))
}
