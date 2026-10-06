package server

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ExecutionSnapshot struct {
	ActiveRPCs int64
}

func (s *Server) Snapshot() ExecutionSnapshot {
	snapshot := ExecutionSnapshot{ActiveRPCs: int64(len(s.admission.slots))}
	return snapshot
}

// RecordStreamItems bounds admitted and publishing records per stream. Store
// byte reservations independently bound their retained input and result data.
const RecordStreamItems = 32

// Execute consumes single requests while the shared Store scheduler aggregates
// database work. Results are published in input order without waiting for EOF.
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
			return status.Error(codes.InvalidArgument, "Scan and Native require exactly one request")
		}
		return s.executeCommand(stream, runtime, request.Command)
	}
	ctx, cancel := context.WithCancel(stream.Context())
	session := runtime.NewSession()
	tickets := make(chan *store.Ticket, RecordStreamItems)
	credits := make(chan struct{}, RecordStreamItems)
	for range RecordStreamItems {
		credits <- struct{}{}
	}
	received := make(chan error, 1)
	receiverDone := make(chan struct{})
	input := &recordInput{stream: stream, runtime: runtime, session: session, first: request, tickets: tickets, credits: credits}
	go func() {
		defer close(tickets)
		defer close(receiverDone)
		received <- s.receiveRecords(ctx, input)
	}()
	defer func() {
		cancel()
		session.Close()
		input.mu.Lock()
		receiving := input.waiting
		input.mu.Unlock()
		// A canceled native RPC already unblocks Recv. An output error with a live
		// RPC needs transport cancellation only when its receiver is inside Recv.
		if receiving && stream.Context().Err() == nil {
			s.abortPeer(stream.Context())
		}
		<-receiverDone
	}()
	index := uint64(1)
	for ticket := range tickets {
		if err := s.publishRecord(stream, ticket, index); err != nil {
			return err
		}
		input.acknowledge(s)
		index++
	}
	return <-received
}

type recordInput struct {
	stream       grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]
	runtime      *store.Runtime
	session      *store.Session
	first        *pb.ExecuteRequest
	tickets      chan<- *store.Ticket
	credits      chan struct{}
	mu           sync.Mutex
	waiting      bool
	idleDeadline time.Time
	timer        *time.Timer
}

// Only idle input has a stall deadline: a caller may wait for the first
// response before producing its next request, including slow database work.
func (input *recordInput) armIdleLocked(server *Server) {
	if !input.waiting || len(input.credits) != RecordStreamItems-1 {
		return
	}
	input.idleDeadline = time.Now().Add(server.limits.Stall)
	input.timer = time.AfterFunc(server.limits.Stall, func() {
		input.mu.Lock()
		stalled := input.waiting && len(input.credits) == RecordStreamItems-1 && !time.Now().Before(input.idleDeadline)
		input.mu.Unlock()
		if stalled {
			server.metrics.watchdogs.WithLabelValues("input_or_result").Inc()
			server.abortPeer(input.stream.Context())
		}
	})
}
func (input *recordInput) receive(ctx context.Context, server *Server) (*pb.ExecuteRequest, error) {
	input.mu.Lock()
	if ctx.Err() != nil {
		input.mu.Unlock()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	input.waiting = true
	input.armIdleLocked(server)
	input.mu.Unlock()
	request, err := input.stream.Recv()
	input.mu.Lock()
	input.waiting = false
	if input.timer != nil {
		input.timer.Stop()
	}
	input.mu.Unlock()
	return request, err
}
func (input *recordInput) acknowledge(server *Server) {
	input.mu.Lock()
	input.credits <- struct{}{}
	input.armIdleLocked(server)
	input.mu.Unlock()
}

func (s *Server) receiveRecords(ctx context.Context, input *recordInput) error {
	storeName, kind := input.first.StoreName, commandKind(input.first.Command)
	request := input.first
	input.first = nil
	for index := uint64(1); ; index++ {
		select {
		case <-input.credits:
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		}
		if request == nil {
			var err error
			request, err = input.receive(ctx, s)
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
		if request.StoreName != storeName || commandKind(request.Command) != kind || request.Index != index {
			return status.Error(codes.InvalidArgument, "requests must preserve Store, kind and consecutive indices")
		}
		record, err := execution.NewRecord(storeName, index, request.Command)
		if err != nil {
			return status.Error(codes.Internal, "validated resource could not be decoded")
		}
		plan, failure := input.runtime.PrepareRecord(record)
		if failure != nil {
			return failureStatus(failure)
		}
		ticket, failure := s.submitPlan(ctx, input.runtime, plan, input.session)
		if failure != nil {
			return failureStatus(failure)
		}
		select {
		case input.tickets <- ticket:
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		}
		request = nil
	}
}

func (s *Server) publishRecord(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse], ticket *store.Ticket, index uint64) error {
	defer ticket.Ack()
	event, err := ticket.Wait(stream.Context())
	if err != nil {
		return status.FromContextError(err).Err()
	}
	if event == nil {
		return status.Error(codes.Internal, "missing record result")
	}
	return s.sendEvent(stream, event, index)
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

func (s *Server) receiveFrame(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]) (*pb.ExecuteRequest, error) {
	timer := time.AfterFunc(s.limits.Stall, func() {
		s.metrics.watchdogs.WithLabelValues("input_or_result").Inc()
		s.abortPeer(stream.Context())
	})
	defer timer.Stop()
	return stream.Recv()
}

func (s *Server) executeCommand(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse], runtime *store.Runtime, command *pb.Command) error {
	plan, failure := runtime.PrepareCommand(1, command)
	if failure != nil {
		return s.sendEvent(stream, failedCommand(command, failure), 1)
	}
	session := runtime.NewSession()
	defer session.Close()
	ctx := stream.Context()
	ticket, failure := s.submitPlan(ctx, runtime, plan, session)
	if failure != nil {
		return s.sendEvent(stream, failedCommand(command, failure), 1)
	}
	for {
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case emission := <-session.Events:
			if emission == nil {
				return status.Error(codes.Internal, "missing streaming emission")
			}
			if emission.Event == nil {
				emission.Release()
				ticket.Ack()
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

func (s *Server) submitPlan(ctx context.Context, runtime *store.Runtime, plan *execution.Plan, session *store.Session) (*store.Ticket, *pb.Failure) {
	for {
		ticket, failure, changed := runtime.Submit(ctx, plan, session)
		if failure == nil {
			return ticket, nil
		}
		if failure.Code != pb.FailureCode_RESOURCE_EXHAUSTED || runtime.Snapshot().Overloaded {
			return nil, failure
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, protocol.ContextFailure(ctx)
		case <-s.admission.draining:
			return nil, protocol.Fail(pb.FailureCode_UNAVAILABLE, "draining")
		}
	}
}
