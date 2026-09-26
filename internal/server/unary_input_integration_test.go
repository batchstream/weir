//go:build integration

package server

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"
)

type unaryWire struct {
	conn   net.Conn
	framer *http2.Framer
}
type unaryWireReply struct {
	data   []byte
	status string
	reset  bool
}

func openUnaryWire(t *testing.T, f fixture) *unaryWire {
	t.Helper()
	conn, err := net.DialTimeout("tcp", f.address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatal(err)
	}
	framer := http2.NewFramer(conn, conn)
	framer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	setting := http2.Setting{ID: http2.SettingInitialWindowSize, Val: 65535}
	if err := framer.WriteSettings(setting); err != nil {
		t.Fatal(err)
	}
	var block bytes.Buffer
	encoder := hpack.NewEncoder(&block)
	fields := []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"}, {Name: ":authority", Value: f.address}, {Name: ":path", Value: pb.Weir_Read_FullMethodName}, {Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"}}
	for _, field := range fields {
		if err := encoder.WriteField(field); err != nil {
			t.Fatal(err)
		}
	}
	header := http2.HeadersFrameParam{StreamID: 1, BlockFragment: block.Bytes(), EndHeaders: true}
	if err := framer.WriteHeaders(header); err != nil {
		t.Fatal(err)
	}
	wire := &unaryWire{conn: conn, framer: framer}
	return wire
}

func (w *unaryWire) receive(t *testing.T) unaryWireReply {
	t.Helper()
	reply := unaryWireReply{}
	for {
		frame, err := w.framer.ReadFrame()
		if err != nil {
			t.Fatal("missing terminal HTTP/2 frame", err)
		}
		switch frame := frame.(type) {
		case *http2.SettingsFrame:
			if !frame.IsAck() {
				if err := w.framer.WriteSettingsAck(); err != nil {
					t.Fatal(err)
				}
			}
		case *http2.PingFrame:
			if !frame.IsAck() {
				if err := w.framer.WritePing(true, frame.Data); err != nil {
					t.Fatal(err)
				}
			}
		case *http2.DataFrame:
			if frame.StreamID == 1 {
				reply.data = append(reply.data, frame.Data()...)
				if len(reply.data) > protocol.MaxFrame+5 {
					t.Fatal("response exceeded frame bound")
				}
				if frame.StreamEnded() {
					return reply
				}
			}
		case *http2.MetaHeadersFrame:
			if frame.StreamID == 1 {
				for _, field := range frame.Fields {
					if field.Name == "grpc-status" {
						reply.status = field.Value
					}
				}
				if frame.StreamEnded() {
					return reply
				}
			}
		case *http2.RSTStreamFrame:
			if frame.StreamID == 1 {
				reply.reset = true
				return reply
			}
		}
	}
}

func unaryRequestBytes(t *testing.T, f fixture) []byte {
	t.Helper()
	request := &pb.ReadRequest{Resource: resource(f, "missing")}
	payload, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	framed := make([]byte, 5+len(payload))
	binary.BigEndian.PutUint32(framed[1:5], uint32(len(payload)))
	copy(framed[5:], payload)
	return framed
}

func TestUnaryInputStall(t *testing.T) {
	for _, mode := range []string{"no_data", "partial_prefix", "partial_body"} {
		t.Run(mode, func(t *testing.T) {
			limits := DefaultLimits()
			limits.UnaryLifetime = 2 * time.Second
			limits.Stall = 100 * time.Millisecond
			f := setupWithLimits(t, true, limits)
			wire := openUnaryWire(t, f)
			request := unaryRequestBytes(t, f)
			switch mode {
			case "partial_prefix":
				if err := wire.framer.WriteData(1, false, request[:2]); err != nil {
					t.Fatal(err)
				}
			case "partial_body":
				if err := wire.framer.WriteData(1, false, request[:len(request)-1]); err != nil {
					t.Fatal(err)
				}
			}
			start := time.Now()
			if err := wire.conn.SetReadDeadline(start.Add(500 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			reply := wire.receive(t)
			if !reply.reset && reply.status == "0" {
				t.Fatal("incomplete unary request succeeded")
			}
			if time.Since(start) > 400*time.Millisecond {
				t.Fatal("input stall was not bounded independently of total lifetime")
			}
			assertUnaryResourcesReleased(t, f)
			t.Logf("%s: input terminated in %s reset=%v status=%s", mode, time.Since(start), reply.reset, reply.status)
		})
	}
}

func TestUnaryInputProgressRenewsOnlyStallBudget(t *testing.T) {
	limits := DefaultLimits()
	limits.UnaryLifetime = time.Second
	limits.Stall = 150 * time.Millisecond
	f := setupWithLimits(t, true, limits)
	wire := openUnaryWire(t, f)
	request := unaryRequestBytes(t, f)
	start := time.Now()
	for offset := 0; offset < len(request); {
		end := min(offset+8, len(request))
		last := end == len(request)
		if err := wire.framer.WriteData(1, last, request[offset:end]); err != nil {
			t.Fatal(err)
		}
		offset = end
		if !last {
			time.Sleep(60 * time.Millisecond)
		}
	}
	reply := wire.receive(t)
	if reply.reset || reply.status != "0" {
		t.Fatal("input making progress was treated as stalled", reply)
	}
	if time.Since(start) < limits.Stall {
		t.Fatal("test did not span multiple stall intervals")
	}
	var result pb.ReadResult
	if len(reply.data) < 5 || int(binary.BigEndian.Uint32(reply.data[1:5])) != len(reply.data)-5 {
		t.Fatal("invalid unary frame")
	}
	if err := proto.Unmarshal(reply.data[5:], &result); err != nil || result.GetMissing() == nil {
		t.Fatal(err, &result)
	}
	assertUnaryResourcesReleased(t, f)
}

func TestUnaryDecodedInputDoesNotTimeoutBackend(t *testing.T) {
	limits := DefaultLimits()
	limits.UnaryLifetime = time.Second
	limits.Stall = 100 * time.Millisecond
	f := setupWithLimits(t, true, limits)
	data := bson.D{{Key: "failCommands", Value: bson.A{"find"}}, {Key: "appName", Value: "weir:" + f.db}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 250}}
	testmongo.FailCommand(t, f.native, data, 1)
	wire := openUnaryWire(t, f)
	request := unaryRequestBytes(t, f)
	start := time.Now()
	// Deliberately do not half-close. A decoded unary frame is enough to end
	// input-stall accounting; the backend still consumes the original lifetime.
	if err := wire.framer.WriteData(1, false, request); err != nil {
		t.Fatal(err)
	}
	reply := wire.receive(t)
	if reply.reset || reply.status != "0" {
		t.Fatal("input timer cancelled already-decoded work", reply)
	}
	if time.Since(start) < 240*time.Millisecond {
		t.Fatal("backend delay not exercised")
	}
	assertUnaryResourcesReleased(t, f)
}
