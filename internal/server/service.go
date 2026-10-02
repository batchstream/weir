package server

import (
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) hostedStore(storeName string) (*store.Runtime, error) {
	runtime, ok := s.stores[storeName]
	if !ok {
		s.admission.rejections.WithLabelValues("execute").Inc()
		return nil, status.Error(codes.Unavailable, "Store is not hosted here; ResolveStore before sending business requests")
	}
	return runtime, nil
}
