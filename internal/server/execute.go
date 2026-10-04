package server

import (
	"context"
	"io"
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

// Execute processes one Store and operation kind in consecutive bounded frames.
// Only the current frame and database window are retained. Publication waits for
// native transport ownership to end before admitting more record work.
func (s *Server) Execute(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]) error {
	request, err := s.receiveFrame(stream)
	if err == io.EOF {
		return status.Error(codes.InvalidArgument, "empty execution stream")
	}
	if err != nil {
		return err
	}
	if err := protocol.ValidateExecuteRequest(request); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	runtime, err := s.hostedStore(request.StoreName)
	if err != nil {
		return err
	}
	kind := commandKind(request.Command)
	if kind == "scan" || kind == "native" {
		_, err = s.receiveFrame(stream)
		if err != io.EOF {
			if err != nil {
				return err
			}
			return status.Error(codes.InvalidArgument, "Scan and Native require exactly one frame")
		}
		return s.executeCommand(stream, runtime, request.Command)
	}
	storeName, next := request.StoreName, uint64(1)
	for {
		if request.StoreName != storeName || commandKind(request.Command) != kind || request.Index != next {
			return status.Error(codes.InvalidArgument, "execution frames must preserve Store, kind and consecutive ordinals")
		}
		count := recordCount(request.Command)
		for offset := 0; offset < count; {
			prepared, size, failure := runtime.PrepareWindow(storeName, request.Command, offset)
			if failure != nil {
				return failureStatus(failure)
			}
			if err := s.executeWindow(stream, runtime, prepared, request.Index+uint64(offset)); err != nil {
				return err
			}
			offset += size
		}
		next += uint64(count)
		request, err = s.receiveFrame(stream)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := protocol.ValidateExecuteRequest(request); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
	}
}

func commandKind(command *pb.Command) string {
	switch command.Operation.(type) {
	case *pb.Command_Read:
		return "read"
	case *pb.Command_Mutate:
		return "mutate"
	case *pb.Command_Scan:
		return "scan"
	default:
		return "native"
	}
}

func recordCount(command *pb.Command) int {
	if read := command.GetRead(); read != nil {
		return len(read.Requests)
	}
	return len(command.GetMutate().Requests)
}

func (s *Server) receiveFrame(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]) (*pb.ExecuteRequest, error) {
	timer := time.AfterFunc(s.limits.Stall, func() {
		s.metrics.watchdogs.WithLabelValues("input_or_result").Inc()
		s.abortPeer(stream.Context())
	})
	defer timer.Stop()
	return stream.Recv()
}

func (s *Server) executeWindow(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse], runtime *store.Runtime, prepared *store.PreparedBatch, first uint64) error {
	ctx := stream.Context()
	ticket, err := s.submitBatch(ctx, runtime, prepared)
	if err != nil {
		return err
	}
	defer ticket.Ack()
	results, err := ticket.WaitBatch(ctx)
	if err != nil {
		return status.FromContextError(err).Err()
	}
	for position, result := range results {
		if result == nil {
			return status.Error(codes.Internal, "missing record result")
		}
		event := &pb.Event{}
		if result.Read != nil {
			event.Value = &pb.Event_ReadResult{ReadResult: result.Read}
		} else {
			event.Value = &pb.Event_MutationResult{MutationResult: result.Mutation}
		}
		if err := s.sendEvent(stream, event, first+uint64(position)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) executeCommand(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse], runtime *store.Runtime, command *pb.Command) error {
	plan, failure := runtime.PrepareCommand(1, command)
	if failure != nil {
		return s.sendEvent(stream, failedCommand(command, failure), 1)
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
			return s.sendEvent(stream, failedCommand(command, failure), 1)
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
			err := s.sendEvent(stream, emission.Event, 1)
			emission.Release()
			if err != nil {
				return err
			}
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

func (s *Server) sendEvent(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse], event *pb.Event, index uint64) error {
	response := &pb.ExecuteResponse{Index: index, Event: event}
	if err := protocol.ValidateExecuteResponse(response); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	timer := time.AfterFunc(s.limits.Stall, func() {
		s.metrics.watchdogs.WithLabelValues("output").Inc()
		s.abortPeer(stream.Context())
	})
	defer timer.Stop()
	err := stream.Send(response)
	timer.Stop()
	if value, ok := s.admission.responses.Load(response); ok {
		owner := value.(*responseBufferOwner)
		if err != nil {
			owner.detachMessage()
			return err
		}
		select {
		case <-owner.done:
		case <-stream.Context().Done():
			owner.detachMessage()
			return status.FromContextError(stream.Context().Err()).Err()
		}
	}
	return err
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
