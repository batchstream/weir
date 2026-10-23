package server

import (
	"context"
	"errors"
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

type executeStream = grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]

var errNativeOutputClosed = errors.New("Native response no longer deliverable")

// Execute serves one record stream or one Native command. Records are queued
// as they arrive and their results are sent in input order without waiting
// for the end of the stream.
func (s *Server) Execute(stream executeStream) error {
	request, err := s.receive(stream)
	if err == io.EOF {
		return status.Error(codes.InvalidArgument, "empty execution stream")
	}
	if err != nil {
		return err
	}
	if err := protocol.ValidateExecuteRequest(request); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	runtime, ok := s.stores[request.StoreName]
	if !ok {
		s.admission.rejections.WithLabelValues("execute").Inc()
		return status.Error(codes.Unavailable, "Store is not hosted here; ResolveStore before sending business requests")
	}
	if commandKind(request.Command) == "native" {
		_, err = s.receive(stream)
		if err != io.EOF {
			if err != nil {
				return err
			}
			return status.Error(codes.InvalidArgument, "Native requires exactly one request")
		}
		return s.executeNative(stream, runtime, request.Command)
	}
	return s.executeRecords(stream, runtime, request)
}

// receive reads one request; a peer that sends nothing within Stall loses the RPC.
func (s *Server) receive(stream executeStream) (*pb.ExecuteRequest, error) {
	timer := time.AfterFunc(s.limits.Stall, func() { s.cancelRPC(stream.Context(), "input") })
	defer timer.Stop()
	return stream.Recv()
}

func (s *Server) executeRecords(stream executeStream, runtime *store.Runtime, first *pb.ExecuteRequest) error {
	ctx, cancel := context.WithCancel(stream.Context())
	window := s.limits.MaxPendingRecords
	records := &recordStream{
		server:  s,
		stream:  stream,
		runtime: runtime,
		session: runtime.NewSession(),
		credits: make(chan struct{}, window),
		tickets: make(chan *store.Ticket, window),
	}
	received := make(chan error, 1)
	receiverDone := make(chan struct{})
	go func() {
		defer close(receiverDone)
		defer close(records.tickets)
		received <- records.receive(ctx, first)
	}()
	defer func() {
		cancel()
		select {
		case <-receiverDone:
		default:
			// Only the RPC's own cancellation ends a blocked Recv.
			endRPC(stream.Context())
			<-receiverDone
		}
		records.stopWatchdog()
	}()
	index := uint64(1)
	for ticket := range records.tickets {
		event, err := ticket.Wait(stream.Context())
		if err != nil {
			return status.FromContextError(err).Err()
		}
		if err := s.send(stream, index, event); err != nil {
			return err
		}
		records.delivered()
		index++
	}
	return <-received
}

// recordStream bounds the records one stream has queued or awaiting delivery.
// A peer may take any time while results are pending, but an idle stream that
// sends nothing for Stall is canceled.
type recordStream struct {
	server  *Server
	stream  executeStream
	runtime *store.Runtime
	session *store.Session
	credits chan struct{}
	tickets chan *store.Ticket

	mu         sync.Mutex
	waiting    bool
	pending    int
	generation uint64
	timer      *time.Timer
}

func (r *recordStream) receive(ctx context.Context, first *pb.ExecuteRequest) error {
	storeName, kind := first.StoreName, commandKind(first.Command)
	request := first
	for index := uint64(1); ; index++ {
		select {
		case r.credits <- struct{}{}:
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		}
		if request == nil {
			var err error
			request, err = r.next()
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
		plan, failure := r.runtime.PrepareRecord(ctx, record)
		if failure != nil {
			return failureStatus(failure)
		}
		ticket, failure := r.runtime.Submit(ctx, plan, r.session)
		if failure != nil {
			return failureStatus(failure)
		}
		r.mu.Lock()
		r.pending++
		r.watchLocked()
		r.mu.Unlock()
		select {
		case r.tickets <- ticket:
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		}
		request = nil
	}
}

func (r *recordStream) next() (*pb.ExecuteRequest, error) {
	r.mu.Lock()
	r.waiting = true
	r.watchLocked()
	r.mu.Unlock()
	request, err := r.stream.Recv()
	r.mu.Lock()
	r.waiting = false
	r.watchLocked()
	r.mu.Unlock()
	return request, err
}

func (r *recordStream) delivered() {
	<-r.credits
	r.mu.Lock()
	r.pending--
	r.watchLocked()
	r.mu.Unlock()
}

// watchLocked restarts the idle watchdog after every state change; a timer
// from an earlier state never fires.
func (r *recordStream) watchLocked() {
	r.generation++
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
	if !r.waiting || r.pending != 0 {
		return
	}
	generation := r.generation
	r.timer = time.AfterFunc(r.server.limits.Stall, func() {
		r.mu.Lock()
		idle := r.generation == generation
		r.mu.Unlock()
		if idle {
			r.server.cancelRPC(r.stream.Context(), "input")
		}
	})
}

func (r *recordStream) stopWatchdog() {
	r.mu.Lock()
	r.waiting = false
	r.watchLocked()
	r.mu.Unlock()
}

func commandKind(command *pb.Command) string {
	switch command.Operation.(type) {
	case *pb.Command_Read:
		return "read"
	case *pb.Command_Mutate:
		return "mutate"
	default:
		return "native"
	}
}

func (s *Server) executeNative(stream executeStream, runtime *store.Runtime, command *pb.Command) error {
	ctx := stream.Context()
	plan, failure := runtime.PrepareNative(ctx, command)
	if failure != nil {
		return s.send(stream, 1, nativeFailure(failure))
	}
	output := &nativeOutput{server: s, stream: stream}
	ticket, failure := runtime.SubmitNative(ctx, plan, output.emit)
	if failure != nil {
		return s.send(stream, 1, nativeFailure(failure))
	}
	event, err := ticket.Wait(ctx)
	output.close()
	if err != nil {
		return status.FromContextError(err).Err()
	}
	return s.send(stream, 1, event)
}

// nativeOutput lets the Store executor send response events until the handler
// stops waiting; nothing may be sent after the handler returns.
type nativeOutput struct {
	server *Server
	stream executeStream
	mu     sync.Mutex
	closed bool
}

func (o *nativeOutput) emit(event *pb.Event) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return errNativeOutputClosed
	}
	return o.server.send(o.stream, 1, event)
}

func (o *nativeOutput) close() {
	o.mu.Lock()
	o.closed = true
	o.mu.Unlock()
}

func nativeFailure(failure *pb.Failure) *pb.Event {
	end := protocol.NativeFailure(false, failure)
	value := &pb.Event_NativeEnd{NativeEnd: end}
	event := &pb.Event{Value: value}
	return event
}

// send delivers one response; a peer that stops reading for Stall loses the RPC.
func (s *Server) send(stream executeStream, index uint64, event *pb.Event) error {
	response := &pb.ExecuteResponse{Index: index, Event: event}
	if err := protocol.ValidateExecuteResponse(response); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	timer := time.AfterFunc(s.limits.Stall, func() { s.cancelRPC(stream.Context(), "output") })
	defer timer.Stop()
	if err := stream.Send(response); err != nil {
		return err
	}
	if err := stream.Context().Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}
