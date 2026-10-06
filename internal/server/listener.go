package server

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

func (s *Server) Serving() <-chan struct{} { return s.serving }
func (s *Server) Serve(listener net.Listener) error {
	close(s.serving)
	bounded := &limitedListener{Listener: listener, slots: s.connectionSlots, server: s}
	err := s.grpc.Serve(bounded)
	if errors.Is(err, grpc.ErrServerStopped) {
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
			s.grpc.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-drain.Done():
			s.metrics.forced.WithLabelValues("drain").Inc()
			s.grpc.Stop()
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
	slots   chan struct{}
	closed  atomic.Bool
	opening atomic.Pointer[time.Timer]
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
			if l.server.limits.Stall > 0 {
				timer := time.AfterFunc(min(5*time.Second, l.server.limits.Stall), func() { _ = c.close("open") })
				c.opening.Store(timer)
			}
			return c, nil
		default:
			l.server.admission.rejections.WithLabelValues("connections").Inc()
			_ = conn.Close()
		}
	}
}
func (c *limitedConn) Write(data []byte) (int, error) {
	if c.server.limits.Stall > 0 {
		if err := c.Conn.SetWriteDeadline(time.Now().Add(c.server.limits.Stall)); err != nil {
			return 0, err
		}
	}
	return c.Conn.Write(data)
}

func (c *limitedConn) Close() error { return c.close("") }
func (c *limitedConn) close(reason string) error {
	if c.closed.CompareAndSwap(false, true) {
		if timer := c.opening.Load(); timer != nil {
			timer.Stop()
		}
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
// on an output stall; co-resident RPCs lose their responses, never their evidence.
func (s *Server) abortPeer(ctx context.Context) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return
	}
	if conn, ok := s.connections.Load(p.Addr.String()); ok {
		_ = conn.(*limitedConn).close("abort")
	}
}
