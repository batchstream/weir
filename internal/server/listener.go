package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/peer"
)

func (s *Server) Serving() <-chan struct{} { return s.serving }
func (s *Server) Serve(listener net.Listener) error {
	close(s.serving)
	bounded := &limitedListener{Listener: listener, slots: s.connectionSlots, server: s}
	err := s.http.Serve(bounded)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func (s *Server) Shutdown(ctx context.Context) error {
	s.once.Do(func() {
		drain, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		s.admission.BeginDrain()
		stopped := make(chan struct{})
		go func() {
			if err := s.http.Shutdown(drain); err != nil {
				s.metrics.forced.WithLabelValues("drain").Inc()
				_ = s.http.Close()
			}
			// ServeHTTP has no gRPC Drain. net/http owns HTTP/2 graceful shutdown;
			// Stop joins the remaining bounded handlers after transport cancellation.
			s.grpc.Stop()
			s.controlGRPC.Stop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-drain.Done():
			_ = s.http.Close()
			<-stopped
		}
	})
	return nil
}

type limitedListener struct {
	server *Server
	net.Listener
	slots chan struct{}
}
type limitedConn struct {
	server *Server
	net.Conn
	slots  chan struct{}
	closed atomic.Bool
}

func (l *limitedListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.slots <- struct{}{}:
			c := &limitedConn{Conn: conn, slots: l.slots, server: l.server}
			l.server.connections.Store(conn.RemoteAddr().String(), c)
			return c, nil
		default:
			l.server.admission.rejections.WithLabelValues("connections").Inc()
			_ = conn.Close()
		}
	}
}
func (c *limitedConn) Close() error { return c.close("") }
func (c *limitedConn) close(reason string) error {
	if c.closed.CompareAndSwap(false, true) {
		if reason != "" {
			c.server.metrics.forced.WithLabelValues(reason).Inc()
		}
		defer func() { <-c.slots }()
		c.server.connections.CompareAndDelete(c.RemoteAddr().String(), c)
		return c.Conn.Close()
	}
	return nil
}

// gRPC queues trailers behind already-buffered DATA. A peer that never reads can
// prevent even an error trailer/RST from progressing. Close that bounded connection
// on a send/input stall; co-resident RPCs lose their responses, never their evidence.
func (s *Server) abortPeer(ctx context.Context) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return
	}
	if conn, ok := s.connections.Load(p.Addr.String()); ok {
		_ = conn.(*limitedConn).close("abort")
	}
}
