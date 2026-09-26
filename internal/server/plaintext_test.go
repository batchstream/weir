package server

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// Exercise bytes before and after HTTP/2 dispatch. Handler-only deadlines cannot
// bound connections that have not supplied complete prefaces and headers.
func TestPlaintextPrefaceAndHeaderLifetime(t *testing.T) {
	for _, peer := range []bool{false, true} {
		for _, mode := range []string{"silent", "preface", "settings", "frame-header", "header-body", "continuations", "rpc-no-data", "rpc-prefix", "rpc-body"} {
			t.Run(fmt.Sprintf("peer=%t/%s", peer, mode), func(t *testing.T) {
				adapter, runtime := peerLocal(t, "records")
				local := Service{LocalStore: runtime}
				limits := DefaultLimits()
				limits.Stall = 75 * time.Millisecond
				opts := peerServerOptions{routes: map[string]Service{"records": local}, peer: peer, limits: limits}
				srv, address := startPeerServer(t, opts)
				conn, err := net.DialTimeout("tcp", address, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				started := time.Now()
				if mode == "preface" {
					_, err = io.WriteString(conn, http2.ClientPreface[:14])
				} else if mode != "silent" {
					_, err = io.WriteString(conn, http2.ClientPreface)
				}
				if err != nil {
					t.Fatal(err)
				}
				framer := http2.NewFramer(conn, conn)
				if mode != "silent" && mode != "preface" && mode != "settings" {
					if err := framer.WriteSettings(); err != nil {
						t.Fatal(err)
					}
					var block, encoded bytes.Buffer
					encoder := hpack.NewEncoder(&block)
					fields := []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"}, {Name: ":authority", Value: address}, {Name: ":path", Value: pb.Weir_Read_FullMethodName}, {Name: "content-type", Value: "application/grpc"}}
					hop := hpack.HeaderField{Name: HopMetadata, Value: "0"}
					if peer {
						fields = append(fields, hop)
					}
					for _, field := range fields {
						if err := encoder.WriteField(field); err != nil {
							t.Fatal(err)
						}
					}
					writer := http2.NewFramer(&encoded, nil)
					params := http2.HeadersFrameParam{StreamID: 1, BlockFragment: block.Bytes(), EndHeaders: mode != "continuations"}
					if err := writer.WriteHeaders(params); err != nil {
						t.Fatal(err)
					}
					raw := encoded.Bytes()
					switch mode {
					case "frame-header":
						raw = raw[:4]
					case "header-body":
						raw = raw[:len(raw)-1]
					}
					if _, err := conn.Write(raw); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "rpc-prefix" || mode == "rpc-body" {
					data := []byte{0, 0, 0}
					if mode == "rpc-body" {
						data = []byte{0, 0, 0, 0, 20, 10}
					}
					err := framer.WriteData(1, false, data)
					if err != nil {
						t.Fatal(err)
					}
				}
				producer := make(chan struct{})
				go func() {
					defer close(producer)
					if mode == "continuations" {
						// Progress without END_HEADERS must not buy unbounded lifetime.
						for range 50 {
							time.Sleep(10 * time.Millisecond)
							if err := framer.WriteContinuation(1, false, nil); err != nil {
								return
							}
						}
					}
				}()
				var readErr error
				if strings.HasPrefix(mode, "rpc-") {
					ended := false
					for !ended {
						frame, err := framer.ReadFrame()
						if err != nil {
							t.Fatal("partial body did not receive a bounded stream termination", err)
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
						case *http2.RSTStreamFrame:
							ended = frame.StreamID == 1
						case *http2.HeadersFrame:
							ended = frame.StreamID == 1 && frame.StreamEnded()
						}
					}
				} else {
					_, readErr = io.Copy(io.Discard, conn)
				}
				_ = conn.Close()
				<-producer
				if timeout, ok := readErr.(net.Error); ok && timeout.Timeout() {
					t.Fatal("only the client deadline closed the stalled connection", readErr)
				}
				deadline := time.Now().Add(time.Second)
				for len(srv.admission.connections) != 0 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				if time.Since(started) > 750*time.Millisecond || len(srv.admission.connections) != 0 || len(srv.slots) != 0 || len(adapter.seen) != 0 || adapter.commands.Load() != 0 {
					t.Fatal("stalled transport leaked resources or executed", time.Since(started), len(srv.admission.connections), len(srv.slots))
				}
			})
		}
	}
}
