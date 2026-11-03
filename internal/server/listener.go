package server

import (
	"context"
	"errors"
	"net"

	"google.golang.org/grpc"
)

func (s *Server) Serve(listener net.Listener) error {
	err := s.grpc.Serve(listener)
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

// Shutdown stops admitting RPCs and waits for running handlers until ctx ends,
// then cancels them.
func (s *Server) Shutdown(ctx context.Context) {
	s.BeginDrain()
	stopped := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-ctx.Done():
		s.grpc.Stop()
		<-stopped
	}
}
