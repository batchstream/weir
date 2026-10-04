package server

import (
	"context"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ExecutionSnapshot struct {
	ActiveRPCs int64
}

func (s *Server) Snapshot() ExecutionSnapshot {
	snapshot := ExecutionSnapshot{ActiveRPCs: int64(len(s.slots))}
	return snapshot
}

// Execute runs one Scan or Native command. Records use the unary Read and
// Mutate batch RPCs and never enter this event publisher.
func (s *Server) Execute(request *pb.ExecuteRequest, stream grpc.ServerStreamingServer[pb.ExecuteResponse]) error {
	if err := protocol.ValidateExecuteRequest(request); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	runtime, err := s.hostedStore(request.StoreName)
	if err != nil {
		return err
	}
	plan, failure := runtime.PrepareCommand(1, request.Command)
	if failure != nil {
		return s.sendEvent(stream, failedCommand(request.Command, failure))
	}
	session := runtime.NewSession()
	defer session.Close()
	ctx := stream.Context()
	for {
		_, failure, changed := runtime.Submit(ctx, plan, session)
		if failure == nil {
			break
		}
		if failure.Code != pb.FailureCode_RESOURCE_EXHAUSTED || runtime.Snapshot().Overloaded {
			return s.sendEvent(stream, failedCommand(request.Command, failure))
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-s.draining:
			return status.Error(codes.Unavailable, "draining")
		}
	}
	for {
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case emission := <-session.Events:
			if emission == nil || emission.Ticket == nil {
				return status.Error(codes.Internal, "missing streaming emission")
			}
			if emission.End {
				emission.Release()
				emission.Ticket.Ack()
				return nil
			}
			if err := s.sendEvent(stream, emission.Event); err != nil {
				return err
			}
			emission.Release()
		}
	}
}

func failedCommand(call *pb.Command, failure *pb.Failure) *pb.Event {
	event := &pb.Event{}
	if call.GetScan() != nil {
		end := &pb.ScanEnd{Failure: failure}
		event.Value = &pb.Event_ScanEnd{ScanEnd: end}
	} else {
		end := protocol.NativeFailure(false, failure)
		event.Value = &pb.Event_NativeEnd{NativeEnd: end}
	}
	return event
}

func (s *Server) sendEvent(stream grpc.ServerStreamingServer[pb.ExecuteResponse], event *pb.Event) error {
	response := &pb.ExecuteResponse{Event: event}
	if err := protocol.ValidateExecuteResponse(response); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	timer := time.AfterFunc(s.limits.Stall, func() {
		s.metrics.watchdogs.WithLabelValues("output").Inc()
		s.abortPeer(stream.Context())
	})
	defer timer.Stop()
	return stream.Send(response)
}

func (s *Server) submitBatch(ctx context.Context, runtime *store.Runtime, prepared *store.PreparedBatch) (*store.Ticket, error) {
	for {
		ticket, failure, changed := runtime.SubmitBatch(ctx, prepared)
		if failure == nil {
			return ticket, nil
		}
		if failure.Code != pb.FailureCode_RESOURCE_EXHAUSTED || runtime.Snapshot().Overloaded {
			return nil, failureStatus(failure)
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-s.draining:
			return nil, status.Error(codes.Unavailable, "draining")
		}
	}
}
