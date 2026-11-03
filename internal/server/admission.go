package server

import (
	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
)

// A stream turned away before any of it was read carries a Failure as a status
// detail. UNAVAILABLE alone also reports a lost connection, after which records
// may have taken effect; the detail tells a client that none did.
var (
	errDraining   = refusedStream("draining")
	errOverloaded = refusedStream("process overloaded")
)

func refusedStream(message string) error {
	failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, message)
	refused, err := status.New(codes.Unavailable, message).WithDetails(protoadapt.MessageV1Of(failure))
	if err != nil {
		panic(err)
	}
	return refused.Err()
}

func (s *Server) SetOverloaded(value bool) { s.overloaded.Store(value) }

// BeginDrain refuses new RPCs; Shutdown then waits for those running.
func (s *Server) BeginDrain() { s.drain.Do(func() { close(s.draining) }) }

func (s *Server) admit() error {
	select {
	case <-s.draining:
		s.metrics.rejections.WithLabelValues("draining").Inc()
		return errDraining
	default:
	}
	if s.overloaded.Load() {
		s.metrics.rejections.WithLabelValues("overload").Inc()
		return errOverloaded
	}
	return nil
}
