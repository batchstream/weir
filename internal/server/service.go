package server

import (
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/store"
)

// Service is a closed choice between the two static execution destinations.
// Assembly owns both; listeners only borrow references.
type Service struct {
	LocalStore *store.Runtime
	RemoteWeir *RemoteWeir
}

func (s *Server) resolve(resource string, root bool) (Service, string, *pb.Failure) {
	empty := Service{}
	name, segments, err := protocol.ParseResource(resource)
	if err != nil || root && len(segments) != 0 {
		s.admission.rejections.WithLabelValues("route").Inc()
		return empty, "", protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid resource")
	}
	service, ok := s.routes[name]
	if !ok {
		s.admission.rejections.WithLabelValues("route").Inc()
		return empty, name, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "unknown store")
	}
	return service, name, nil
}
