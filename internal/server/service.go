package server

import (
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service is a closed choice between the two static execution destinations.
// Assembly owns both; listeners only borrow references.
type Service struct {
	LocalStore *store.Runtime
	RemoteWeir *RemoteWeir
}

func (s *Server) resolve(destination string) (Service, error) {
	service, ok := s.routes[destination]
	if !ok {
		s.admission.rejections.WithLabelValues("route").Inc()
		empty := Service{}
		return empty, status.Error(codes.InvalidArgument, "unknown destination")
	}
	return service, nil
}
