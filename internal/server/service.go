package server

import (
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) resolve(destination string) (*store.Runtime, error) {
	runtime, ok := s.stores[destination]
	if !ok {
		s.admission.rejections.WithLabelValues("route").Inc()
		return nil, status.Error(codes.Unavailable, "Store is not hosted here; Resolve before sending business requests")
	}
	return runtime, nil
}
