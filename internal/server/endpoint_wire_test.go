package server

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

type retryWirePeer struct {
	mode              string
	headers, received atomic.Int32
	mu                sync.Mutex
	closed            bool
	conns             map[net.Conn]bool
	group             sync.WaitGroup
}

func startRetryWirePeer(t *testing.T, mode string) (*retryWirePeer, string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &retryWirePeer{mode: mode, conns: make(map[net.Conn]bool)}
	p.group.Go(func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.closed || len(p.conns) >= 4 {
				p.mu.Unlock()
				_ = conn.Close()
				continue
			}
			p.conns[conn] = true
			p.mu.Unlock()
			p.group.Go(func() {
				defer func() { _ = conn.Close(); p.mu.Lock(); delete(p.conns, conn); p.mu.Unlock() }()
				p.serve(conn)
			})
		}
	})
	t.Cleanup(func() {
		_ = l.Close()
		p.mu.Lock()
		p.closed = true
		for c := range p.conns {
			_ = c.Close()
		}
		p.mu.Unlock()
		p.group.Wait()
	})
	return p, l.Addr().String()
}

func (p *retryWirePeer) serve(conn net.Conn) {
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(conn, preface); err != nil {
		return
	}
	if string(preface) != http2.ClientPreface {
		return
	}
	f := http2.NewFramer(conn, conn)
	setting := http2.Setting{ID: http2.SettingMaxConcurrentStreams, Val: 8}
	if f.WriteSettings(setting) != nil {
		return
	}
	for {
		frame, err := f.ReadFrame()
		if err != nil {
			return
		}
		switch frame := frame.(type) {
		case *http2.SettingsFrame:
			if !frame.IsAck() {
				if f.WriteSettingsAck() != nil {
					return
				}
			}
		case *http2.PingFrame:
			if !frame.IsAck() {
				if f.WritePing(true, frame.Data) != nil {
					return
				}
			}
		case *http2.HeadersFrame:
			p.headers.Add(1)
			if p.mode == "refused" {
				if f.WriteRSTStream(frame.StreamID, http2.ErrCodeRefusedStream) != nil {
					return
				}
			}
			if p.mode == "unreceived-goaway" {
				_ = f.WriteGoAway(0, http2.ErrCodeNo, nil)
				return
			}
		case *http2.DataFrame:
			if p.mode == "refused" {
				continue
			}
			if !frame.StreamEnded() {
				continue
			}
			p.received.Add(1)
			switch p.mode {
			case "received-goaway":
				_ = f.WriteGoAway(frame.StreamID, http2.ErrCodeNo, nil)
				return
			case "received-loss":
				return
			case "received-unavailable":
				var block bytes.Buffer
				encoder := hpack.NewEncoder(&block)
				for _, pair := range [][2]string{{":status", "200"}, {"content-type", "application/grpc"}, {"grpc-status", "14"}} {
					field := hpack.HeaderField{Name: pair[0], Value: pair[1]}
					if encoder.WriteField(field) != nil {
						return
					}
				}
				params := http2.HeadersFrameParam{StreamID: frame.StreamID, BlockFragment: block.Bytes(), EndHeaders: true, EndStream: true}
				if f.WriteHeaders(params) != nil {
					return
				}
			}
		}
	}
}

func TestEndpointWireUnreceivedAndReceivedNoReplay(t *testing.T) {
	for _, mode := range []string{"refused", "unreceived-goaway", "received-loss", "received-goaway", "received-unavailable"} {
		t.Run(mode, func(t *testing.T) {
			one, a := startRetryWirePeer(t, mode)
			two, b := startRetryWirePeer(t, mode)
			r := multipleRemote(t, []string{a, b})
			service := Service{RemoteWeir: r}
			opts := peerServerOptions{routes: map[string]Service{"records": service}, budget: 4}
			entry, address := startPeerServer(t, opts)
			_, client := peerClient(t, address)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if result, err := client.Mutate(ctx, testMutation("wire")); err == nil {
				t.Fatal("wire failure became success", result)
			}
			waitPeerIdle(t, entry)
			awaitEndpoint(t, func() bool { return one.headers.Load()+two.headers.Load() > 0 })
			want := int32(1)
			if mode == "refused" || mode == "unreceived-goaway" {
				want = 0
			}
			if received := one.received.Load() + two.received.Load(); received != want {
				t.Fatal("received request replayed", received, want)
			}
			if one.headers.Load() > 0 && two.headers.Load() > 0 {
				t.Fatal("application switched configured endpoint")
			}
			if one.headers.Load()+two.headers.Load() > 2 {
				t.Fatal("unbounded transparent wire attempts")
			}
			t.Logf("real HTTP/2 mode=%s headers across both endpoints=%d fully received semantic requests=%d", mode, one.headers.Load()+two.headers.Load(), want)
		})
	}
}
