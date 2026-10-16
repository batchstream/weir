package transport

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
)

type receiveTestMemory struct {
	connections   atomic.Int32
	releases      atomic.Int32
	headerBytes   int
	rejectHeaders bool
	last          *receiveTestStream
}

func (memory *receiveTestMemory) NewConnection() (func(), error) {
	memory.connections.Add(1)
	var once sync.Once
	release := func() { once.Do(func() { memory.connections.Add(-1); memory.releases.Add(1) }) }
	return release, nil
}
func (memory *receiveTestMemory) NewStream(_ context.Context, bytes int) (mem.ReceiveStream, error) {
	memory.headerBytes = bytes
	if memory.rejectHeaders {
		return nil, status.Error(codes.ResourceExhausted, "metadata memory")
	}
	owner := &receiveTestStream{}
	memory.last = owner
	return owner, nil
}

type receiveTestStream struct{ closed, retired atomic.Int32 }

func (*receiveTestStream) ReserveMessage(int, bool) error          { return nil }
func (*receiveTestStream) Buffer(int) (func(), error)              { release := func() {}; return release, nil }
func (*receiveTestStream) BeforeDecode(mem.BufferSlice, any) error { return nil }
func (*receiveTestStream) Decoded(any)                             {}
func (*receiveTestStream) ReleaseMessage()                         {}
func (owner *receiveTestStream) Retire()                           { owner.retired.Add(1) }
func (owner *receiveTestStream) Close()                            { owner.closed.Add(1) }
func receiveTestConfig(memory *receiveTestMemory) *ServerConfig {
	maxHeader := uint32(64 << 10)
	parameters := keepalive.ServerParameters{Time: 2 * time.Hour, Timeout: 20 * time.Second, MaxConnectionAge: time.Duration(math.MaxInt64), MaxConnectionIdle: time.Duration(math.MaxInt64), MaxConnectionAgeGrace: time.Duration(math.MaxInt64)}
	config := &ServerConfig{MaxStreams: math.MaxUint32, ReadBufferSize: 32 << 10, WriteBufferSize: 32 << 10, BufferPool: mem.DefaultBufferPool(), ReceiveMemory: memory, MaxHeaderListSize: &maxHeader, KeepaliveParams: parameters}
	return config
}
func TestReceiveConnectionConstructorFailureReleasesBeforeReturning(t *testing.T) {
	memory := &receiveTestMemory{}
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		framer := http2.NewFramer(client, client)
		_, _ = framer.ReadFrame()
		client.Close()
	}()
	config := receiveTestConfig(memory)
	transport, err := NewServerTransport(server, config)
	<-joined
	if transport != nil || err == nil || memory.connections.Load() != 0 || memory.releases.Load() != 1 {
		t.Fatal("failed preface retained connection memory", transport, err, memory.connections.Load(), memory.releases.Load())
	}
}
func TestReceiveUnservedTransportReleasesAfterWriterJoined(t *testing.T) {
	memory := &receiveTestMemory{}
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })
	readDone := make(chan struct{})
	go func() { defer close(readDone); _, _ = io.Copy(io.Discard, client) }()
	writeDone := make(chan error, 1)
	go func() {
		_, err := client.Write(clientPreface)
		if err == nil {
			framer := http2.NewFramer(client, client)
			err = framer.WriteSettings()
		}
		writeDone <- err
	}()
	config := receiveTestConfig(memory)
	transport, err := NewServerTransport(server, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	native := transport.(*http2Server)
	if memory.connections.Load() != 1 {
		t.Fatal("constructed transport lost its owner")
	}
	transport.Close(errors.New("server stopped before addConn"))
	DiscardUnservedTransport(transport)
	<-readDone
	if memory.connections.Load() != 0 || memory.releases.Load() != 1 || native.framer != nil || native.loopy != nil {
		t.Fatal("unserved transport retained native workspaces", memory.connections.Load(), memory.releases.Load(), native.framer, native.loopy)
	}
}
func TestReceiveFinishStreamDrainsChannelBacklogAndReaderTail(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	owner := &receiveTestStream{}
	stream := &ServerStream{cancel: cancel, receiveMemory: owner}
	stream.ctx = ctx
	stream.buf.init(mem.DefaultBufferPool())
	stream.buf.accounted = true
	stream.trReader.reader.ctx = ctx
	stream.trReader.reader.ctxDone = ctx.Done()
	stream.trReader.reader.recv = &stream.buf
	var live atomic.Int32
	buffer := func() mem.Buffer {
		live.Add(1)
		source := mem.SliceBuffer(make([]byte, 37))
		release := func() { live.Add(-1) }
		tracked := mem.TrackReceiveBuffer(source, release)
		return tracked
	}
	first := recvMsg{buffer: buffer()}
	second := recvMsg{buffer: buffer()}
	stream.buf.put(first)
	stream.buf.put(second)
	stream.trReader.reader.last = buffer()
	done := make(chan struct{})
	native := &http2Server{controlBuf: newControlBuffer(done)}
	header := &serverHeaders{}
	native.finishStream(stream, false, http2.ErrCodeNo, header, true)
	if live.Load() != 0 || stream.trReader.reader.last != nil || len(stream.buf.backlog) != 0 || len(stream.buf.c) != 0 {
		t.Fatal("terminal status retained unread native receive owners", live.Load())
	}
	late := recvMsg{buffer: buffer()}
	stream.buf.put(late)
	if live.Load() != 0 {
		t.Fatal("late DATA became reachable after stream finish", live.Load())
	}
	stream.closeReceiveBuffers()
	if live.Load() != 0 {
		t.Fatal("repeated cancellation changed owner accounting")
	}
}

func (*receiveTestStream) SetContext(context.Context) {}

func TestReceiveMetadataReservedBeforeTapAndEarlyAbortRetires(t *testing.T) {
	for _, reject := range []bool{false, true} {
		memory := &receiveTestMemory{rejectHeaders: reject}
		done := make(chan struct{})
		native := &http2Server{receiveMemory: memory, bufferPool: mem.DefaultBufferPool(), controlBuf: newControlBuffer(done)}
		var tapped atomic.Int32
		native.inTapHandle = func(ctx context.Context, _ *tap.Info) (context.Context, error) { tapped.Add(1); return ctx, nil }
		fields := []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"}, {Name: ":authority", Value: "local"}, {Name: ":path", Value: "/receive/Unary"}, {Name: "content-type", Value: "text/plain"}}
		if reject {
			fields[4].Value = "application/grpc"
		}
		for range 1200 {
			field := hpack.HeaderField{Name: "x-field", Value: "a"}
			fields = append(fields, field)
		}
		want := 4096
		for _, field := range fields {
			want += 2*(len(field.Name)+len(field.Value)) + 512
		}
		header := http2.FrameHeader{StreamID: 1, Type: http2.FrameHeaders}
		raw := &http2.HeadersFrame{FrameHeader: header}
		frame := &http2.MetaHeadersFrame{HeadersFrame: raw, Fields: fields}
		dispatched := false
		handle := func(*ServerStream) { dispatched = true }
		if err := native.operateHeaders(t.Context(), frame, handle); err != nil {
			t.Fatal(err)
		}
		if memory.headerBytes != want || tapped.Load() != 0 || dispatched {
			t.Fatal("metadata allocation/tap preceded budget or invalid request dispatched", reject, memory.headerBytes, want, tapped.Load(), dispatched)
		}
		if !reject && (memory.last == nil || memory.last.closed.Load() != 1 || memory.last.retired.Load() != 1) {
			t.Fatal("header early abort retained its stream account")
		}
	}
}
