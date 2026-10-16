package transport

import (
	"io"

	"google.golang.org/grpc/mem"
)

func (s *ServerStream) ReserveReceiveMessage(length int, compressed bool) error {
	if s.receiveMemory == nil {
		return nil
	}
	return s.receiveMemory.ReserveMessage(length, compressed)
}
func (s *ServerStream) DecodedReceiveMessage(value any) {
	if s.receiveMemory != nil {
		s.receiveMemory.Decoded(value)
	}
}
func (s *ServerStream) ReleaseReceiveMessage() {
	if s.receiveMemory != nil {
		s.receiveMemory.ReleaseMessage()
	}
}
func (s *ServerStream) CloseReceiveMemory() {
	if s.receiveMemory != nil {
		s.receiveMemory.Close()
	}
}
func (s *ServerStream) closeReceiveBuffers() {
	if s.receiveMemory == nil {
		return
	}
	if s.receiveReady != nil {
		<-s.receiveReady
	}
	s.buf.close()
	reader := &s.trReader.reader
	reader.mu.Lock()
	if reader.last != nil {
		reader.last.Free()
		reader.last = nil
	}
	reader.err = io.EOF
	reader.mu.Unlock()
}

// close releases queued owners while the RPC reader retains sole access to
// any buffer already removed from the channel. Future DATA is discarded.
func (b *recvBuffer) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.err = io.EOF
	select {
	case message := <-b.c:
		if message.buffer != nil {
			message.buffer.Free()
		}
	default:
	}
	for i := range b.backlog {
		if b.backlog[i].buffer != nil {
			b.backlog[i].buffer.Free()
		}
		b.backlog[i] = recvMsg{}
	}
	b.backlog = nil
	b.uncompactedSuffixLen, b.uncompactedBytes = 0, 0
}

func (s *ServerStream) BeforeDecodeReceiveMessage(data mem.BufferSlice, value any) error {
	if s.receiveMemory == nil {
		return nil
	}
	return s.receiveMemory.BeforeDecode(data, value)
}

// DiscardUnservedTransport is used only when a constructed native transport is
// rejected before HandleStreams starts. No reader owns readerDone in this path.
func DiscardUnservedTransport(server ServerTransport) {
	native, ok := server.(*http2Server)
	if !ok || native.receiveConnection == nil {
		return
	}
	close(native.readerDone)
	<-native.loopyWriterDone
	native.finishReceiveConnection()
}

func (t *http2Server) finishReceiveConnection() {
	if t.receiveConnection == nil {
		return
	}
	t.receiveConnectionOnce.Do(func() {
		// The native reader and writer have both stopped. RPC contexts may retain
		// the transport, so drop its now-unused framer/HPACK and writer workspaces.
		t.framer = nil
		t.loopy = nil
		t.receiveConnection()
	})
}

func (s *ServerStream) RetireReceiveMemory() {
	if s.receiveMemory != nil {
		s.receiveMemory.Retire()
	}
}
