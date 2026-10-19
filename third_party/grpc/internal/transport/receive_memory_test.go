package transport

import (
	"bytes"
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
	"google.golang.org/grpc/internal/channelz"
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

// cancelOnLenBuffer injects cancellation after write's stream-state check but
// before it acquires the queued DATA reference, without a scheduler-dependent race.
type cancelOnLenBuffer struct {
	mem.Buffer
	cancel func()
}

func (buffer *cancelOnLenBuffer) Len() int {
	if buffer.cancel != nil {
		cancel := buffer.cancel
		buffer.cancel = nil
		cancel()
	}
	return buffer.Buffer.Len()
}

func TestReceiveCanceledStreamLateDataReleasesOnlyTransportReference(t *testing.T) {
	for _, freeBeforeLoopy := range []bool{false, true} {
		name := "caller-frees-after-loopy"
		if freeBeforeLoopy {
			name = "caller-frees-before-loopy"
		}
		t.Run(name, func(t *testing.T) {
			done := make(chan struct{})
			native := &http2Server{controlBuf: newControlBuffer(done), activeStreams: make(map[uint32]*ServerStream), channelz: &channelz.Socket{}}
			defer native.controlBuf.finish()
			var wire bytes.Buffer
			writer := newBufWriter(&wire, 0, nil)
			framer := &framer{writer: writer, fr: http2.NewFramer(&wire, &wire)}
			loopy := &loopyWriter{estdStreams: make(map[uint32]*outStream), activeStreams: newOutStreamList(), sendQuota: defaultWindowSize, oiws: defaultWindowSize, framer: framer}
			streams := make([]*ServerStream, 0, 2)
			for _, id := range []uint32{1, 3} {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				stream := &ServerStream{cancel: cancel}
				stream.id, stream.ctx = id, ctx
				stream.headerSent.Store(true)
				stream.wq.init(defaultWriteQuota, ctx.Done())
				native.activeStreams[id] = stream
				registration := &registerStream{streamID: id, wq: &stream.wq}
				loopy.registerStreamHandler(registration)
				streams = append(streams, stream)
			}
			var expiredReleases, siblingReleases atomic.Int32
			siblingBuffer := mem.TrackReceiveBuffer(mem.SliceBuffer([]byte("confirmed")), func() { siblingReleases.Add(1) })
			siblingData := mem.BufferSlice{siblingBuffer}
			err := native.write(streams[1], nil, siblingData, nil)
			if err != nil {
				t.Fatal(err)
			}
			siblingData.Free() // Only the native queued reference remains.
			handleNext := func() {
				item, err := native.controlBuf.get(false)
				if err != nil || item == nil {
					t.Fatal("missing queued transport item", item, err)
				}
				if err := loopy.handle(item); err != nil {
					t.Fatal(err)
				}
			}
			handleNext() // Keep the sibling's DATA owned by its live stream.

			expiredBuffer := mem.TrackReceiveBuffer(mem.SliceBuffer(make([]byte, 1025)), func() { expiredReleases.Add(1) })
			lateBuffer := &cancelOnLenBuffer{Buffer: expiredBuffer, cancel: func() {
				native.closeStream(streams[0], false, http2.ErrCodeCancel, false)
			}}
			lateData := mem.BufferSlice{lateBuffer}
			err = native.write(streams[0], nil, lateData, nil)
			if err != nil || streams[0].ctx.Err() != context.Canceled {
				t.Fatal("cancellation did not interleave with successful DATA enqueue", err, streams[0].ctx.Err())
			}
			if freeBeforeLoopy {
				lateData.Free()
			}
			handleNext() // RST/deadline cleanup removes stream 1 first.
			handleNext() // Its concurrently enqueued DATA must be discarded and freed.
			if !freeBeforeLoopy {
				if expiredReleases.Load() != 0 || len(expiredBuffer.ReadOnlyData()) != 1025 {
					t.Fatal("discarding DATA released the caller's reference")
				}
				lateData.Free()
			}
			if expiredReleases.Load() != 1 {
				t.Fatal("late DATA leaked its transport reference after RPC cancellation", expiredReleases.Load())
			}
			if siblingReleases.Load() != 0 || len(native.activeStreams) != 1 || native.activeStreams[3] != streams[1] {
				t.Fatal("canceling one RPC retired its sibling's ownership", siblingReleases.Load())
			}
			if _, err := loopy.processData(); err != nil {
				t.Fatal(err)
			}
			reader := http2.NewFramer(nil, &wire)
			frame, err := reader.ReadFrame()
			if err != nil {
				t.Fatal(err)
			}
			data, ok := frame.(*http2.DataFrame)
			if !ok || data.StreamID != 3 || string(data.Data()) != "confirmed" || siblingReleases.Load() != 1 {
				t.Fatal("sibling DATA lost its payload or final native release", frame, siblingReleases.Load())
			}
			// Closing the control buffer after a discarded frame must not free it twice.
			native.controlBuf.finish()
			if expiredReleases.Load() != 1 || siblingReleases.Load() != 1 {
				t.Fatal("transport shutdown repeated final buffer release")
			}
		})
	}
}

func TestReceiveRejectedDataEnqueueReleasesOnlyTransportReference(t *testing.T) {
	done := make(chan struct{})
	native := &http2Server{controlBuf: newControlBuffer(done)}
	native.controlBuf.finish()
	stream := &ServerStream{}
	stream.headerSent.Store(true)
	stream.wq.init(defaultWriteQuota, done)
	var releases atomic.Int32
	buffer := mem.TrackReceiveBuffer(mem.SliceBuffer(make([]byte, 1025)), func() { releases.Add(1) })
	data := mem.BufferSlice{buffer}
	// BufferSlice copies alias the same ref-counted buffers. A failed enqueue
	// returns only write's Ref, leaving SendMsg's deferred Free valid.
	callerData := data
	err := native.write(stream, nil, data, nil)
	if !errors.Is(err, ErrConnClosing) || releases.Load() != 0 || len(buffer.ReadOnlyData()) != 1025 {
		t.Fatal("failed enqueue consumed the caller's buffer reference", err, releases.Load())
	}
	callerData.Free()
	native.controlBuf.finish()
	if releases.Load() != 1 {
		t.Fatal("failed enqueue or repeated close leaked its final reference", releases.Load())
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
