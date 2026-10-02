package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// Model a blocked HTTP/2 stream independently of its shared physical connection.
// Only the stream write deadline releases Send; closing the connection is
// observable separately and would also interrupt unrelated RPCs.
type interruptedResponseWriter struct {
	header      http.Header
	interrupted chan struct{}
	once        sync.Once
}

func (w *interruptedResponseWriter) Header() http.Header { return w.header }
func (w *interruptedResponseWriter) WriteHeader(int)     {}
func (w *interruptedResponseWriter) Write(data []byte) (int, error) {
	return len(data), nil
}
func (w *interruptedResponseWriter) SetReadDeadline(time.Time) error { return nil }
func (w *interruptedResponseWriter) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		w.once.Do(func() { close(w.interrupted) })
	}
	return nil
}

type interruptedSendStream struct {
	grpc.ServerStream
	ctx         context.Context
	started     chan struct{}
	interrupted <-chan struct{}
}

func (s *interruptedSendStream) Context() context.Context { return s.ctx }
func (s *interruptedSendStream) Send(*pb.ExecuteResponse) error {
	close(s.started)
	<-s.interrupted
	return context.DeadlineExceeded
}
func (s *interruptedSendStream) Recv() (*pb.ExecuteRequest, error) { return nil, nil }

func TestCanceledSendInterruptsOnlyItsHTTP2Stream(t *testing.T) {
	server := &Server{metrics: newTransportMetrics()}
	server.limits.Stall = time.Second
	connection, sibling := net.Pipe()
	defer connection.Close()
	defer sibling.Close()
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	limited := &limitedConn{server: server, Conn: connection, slots: slots}
	server.connections.Store(connection.RemoteAddr().String(), limited)
	writer := &interruptedResponseWriter{header: make(http.Header), interrupted: make(chan struct{})}
	defer writer.once.Do(func() { close(writer.interrupted) })
	deadline := time.Now().Add(time.Second)
	state := &delivery{controller: http.NewResponseController(writer), deadline: deadline, readDeadline: deadline}
	remote := &peer.Peer{Addr: connection.RemoteAddr()}
	streamContext := peer.NewContext(context.Background(), remote)
	streamContext = context.WithValue(streamContext, deliveryKey, state)
	ctx, cancel := context.WithCancel(streamContext)
	defer cancel()
	stream := &interruptedSendStream{ctx: streamContext, started: make(chan struct{}), interrupted: writer.interrupted}
	response := &pb.ExecuteResponse{RequestId: 1, RequestComplete: true}
	done := make(chan error, 1)
	go func() { done <- server.sendExecutionResponse(ctx, stream, response) }()
	<-stream.started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("blocked send returned unexpected error", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("cancel failed to interrupt the individual stream; shared connection closed:", limited.closed.Load())
	}
	if limited.closed.Load() || len(slots) != 1 {
		t.Fatal("one canceled RPC closed its shared connection")
	}
}

func TestStalledSendStillClosesAnUnresponsivePeer(t *testing.T) {
	server := &Server{metrics: newTransportMetrics()}
	server.limits.Stall = 20 * time.Millisecond
	connection, sibling := net.Pipe()
	defer connection.Close()
	defer sibling.Close()
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	limited := &limitedConn{server: server, Conn: connection, slots: slots}
	server.connections.Store(connection.RemoteAddr().String(), limited)
	remote := &peer.Peer{Addr: connection.RemoteAddr()}
	ctx := peer.NewContext(context.Background(), remote)
	writer := &interruptedResponseWriter{interrupted: make(chan struct{})}
	defer writer.once.Do(func() { close(writer.interrupted) })
	stream := &interruptedSendStream{ctx: ctx, started: make(chan struct{}), interrupted: writer.interrupted}
	response := &pb.ExecuteResponse{RequestId: 1, RequestComplete: true}
	done := make(chan error, 1)
	go func() { done <- server.sendExecutionResponse(ctx, stream, response) }()
	<-stream.started
	if err := sibling.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1)
	_, err := sibling.Read(buffer)
	if err == nil || !limited.closed.Load() {
		t.Fatal("stall watchdog failed to close and release the shared connection", err)
	}
	select {
	case slots <- struct{}{}:
		<-slots
	case <-time.After(time.Second):
		t.Fatal("closed connection retained its admission slot")
	}
	writer.once.Do(func() { close(writer.interrupted) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stalled send did not stop")
	}
}
