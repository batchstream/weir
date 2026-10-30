package server

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type executeStream = grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]

var errNativeOutputClosed = errors.New("Native response no longer deliverable")

// Execute serves one record stream or one Native command. Records are queued
// as they arrive and each result is sent as soon as its record completes.
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
	if request.CancelIndex != 0 {
		return status.Error(codes.InvalidArgument, "an execution stream starts with a record")
	}
	runtime, ok := s.stores[request.StoreName]
	if !ok {
		s.admission.rejections.WithLabelValues("execute").Inc()
		return status.Error(codes.NotFound, "Store is not hosted here")
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

// receive reads one request; a client that sends nothing within Stall loses the RPC.
func (s *Server) receive(stream executeStream) (*pb.ExecuteRequest, error) {
	timer := time.AfterFunc(s.limits.Stall, func() { s.cancelRPC(stream.Context(), "input") })
	defer timer.Stop()
	return stream.Recv()
}

func (s *Server) executeRecords(stream executeStream, runtime *store.Runtime, first *pb.ExecuteRequest) error {
	ctx, cancel := context.WithCancel(stream.Context())
	window := s.limits.recordWindow
	// Each credit reserves room for one record's result until it is sent.
	results := make(chan store.Result, window)
	records := &recordStream{
		server:     s,
		stream:     stream,
		runtime:    runtime,
		session:    runtime.NewStreamSession(results),
		credits:    make(chan struct{}, window),
		admissions: make(chan *admission, window),
		results:    results,
		records:    make(map[uint64]*admission),
	}
	received := make(chan receiveEnd, 1)
	receiverDone := make(chan struct{})
	admitterDone := make(chan struct{})
	go func() {
		defer close(receiverDone)
		end := records.receive(ctx, first)
		close(records.admissions)
		received <- end
	}()
	go func() {
		defer close(admitterDone)
		records.admit()
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
		<-admitterDone
		records.stopWatchdog()
		if records.output != nil {
			records.output.Stop()
		}
	}()
	// Admitted records keep their results even when invalid input ends the stream.
	var sent uint64
	end := receiveEnd{admitted: math.MaxUint64}
	for sent != end.admitted {
		select {
		case result := <-results:
			if err := records.deliver(result); err != nil {
				return err
			}
			records.delivered(result.Index)
			sent++
		case end = <-received:
			received = nil
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		}
	}
	return end.err
}

// receiveEnd reports how many records the stream admitted before its input
// ended and the error, if any, that ends the stream.
type receiveEnd struct {
	admitted uint64
	err      error
}

// recordStream bounds the records one stream has queued or awaiting delivery.
// A client may take any time while results are pending, but an idle stream that
// sends nothing for Stall is canceled. One goroutine reads requests while
// another queues records in stream order, so a full Store queue never stops
// cancel requests from being read.
type recordStream struct {
	server     *Server
	stream     executeStream
	runtime    *store.Runtime
	session    *store.Session
	credits    chan struct{}
	admissions chan *admission
	results    chan store.Result
	// output cancels the RPC when one Send blocks for Stall; only the
	// delivering goroutine arms it.
	output *time.Timer

	mu         sync.Mutex
	waiting    bool
	pending    int
	generation uint64
	timer      *time.Timer
	// records holds each read record until its result is sent.
	records map[uint64]*admission
}

// admission is one read record and its context.
type admission struct {
	ctx    context.Context
	cancel context.CancelFunc
	record *execution.Record
	// claimed is set by whichever of the admitter and a cancel request
	// handles the record first.
	claimed atomic.Bool
}

// receive reads requests until input ends. Each record takes a credit when it
// is read, so a client within the window never waits to send a cancel request.
func (r *recordStream) receive(ctx context.Context, first *pb.ExecuteRequest) receiveEnd {
	storeName, kind := first.StoreName, commandKind(first.Command)
	request := first
	var index uint64
	for {
		end := receiveEnd{admitted: index}
		if request == nil {
			var err error
			request, err = r.next()
			if err == io.EOF {
				return end
			}
			if err == nil {
				err = protocol.ValidateExecuteRequest(request)
				if err != nil {
					err = status.Error(codes.InvalidArgument, err.Error())
				}
			}
			if err != nil {
				end.err = err
				return end
			}
		}
		if request.CancelIndex != 0 {
			if request.StoreName != storeName || request.CancelIndex > index {
				end.err = status.Error(codes.InvalidArgument, "cancel requests must name an earlier record of the stream")
				return end
			}
			r.cancelRecord(request.CancelIndex)
			request = nil
			continue
		}
		index++
		if request.StoreName != storeName || commandKind(request.Command) != kind || request.Index != index {
			end.err = status.Error(codes.InvalidArgument, "requests must preserve Store, kind and consecutive indices")
			return end
		}
		record, err := execution.NewRecord(storeName, index, request.Command)
		if err != nil {
			end.err = status.Error(codes.Internal, "validated resource could not be decoded")
			return end
		}
		next := &admission{record: record}
		next.ctx, next.cancel = recordContext(ctx, request.TimeoutMicros)
		select {
		case r.credits <- struct{}{}:
		case <-ctx.Done():
			next.cancel()
			end.err = status.FromContextError(ctx.Err()).Err()
			return end
		}
		r.mu.Lock()
		r.pending++
		r.records[index] = next
		r.watchLocked()
		r.mu.Unlock()
		r.admissions <- next
		request = nil
	}
}

// recordContext bounds one record by its timeout, counted from when the server
// reads the record.
func recordContext(ctx context.Context, timeoutMicros uint64) (context.Context, context.CancelFunc) {
	if timeoutMicros == 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, protocol.RecordTimeout(timeoutMicros))
}

// admit queues records in stream order. A record that cannot be queued
// receives its own NOT_STARTED result instead of ending the stream.
func (r *recordStream) admit() {
	for next := range r.admissions {
		if !next.claimed.CompareAndSwap(false, true) {
			continue
		}
		plan, failure := r.runtime.PrepareRecord(next.ctx, next.record)
		if failure == nil {
			_, failure = r.runtime.Submit(next.ctx, plan, r.session)
		}
		if failure != nil {
			event := execution.FailedEvent(next.record.Command(), pb.MutationOutcome_NOT_STARTED, failure)
			r.results <- store.Result{Index: next.record.Index(), Event: event}
		}
	}
}

// cancelRecord cancels a record that has not yet been reported. A record still
// waiting for the admitter reports at once instead of when its turn comes.
func (r *recordStream) cancelRecord(index uint64) {
	r.mu.Lock()
	canceled := r.records[index]
	r.mu.Unlock()
	if canceled == nil {
		return
	}
	canceled.cancel()
	if canceled.claimed.CompareAndSwap(false, true) {
		failure := protocol.ContextFailure(canceled.ctx)
		event := execution.FailedEvent(canceled.record.Command(), pb.MutationOutcome_NOT_STARTED, failure)
		r.results <- store.Result{Index: index, Event: event}
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

// delivered releases a sent record's credit and context.
func (r *recordStream) delivered(index uint64) {
	<-r.credits
	r.mu.Lock()
	sent := r.records[index]
	delete(r.records, index)
	r.pending--
	r.watchLocked()
	r.mu.Unlock()
	sent.cancel()
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

// oversized replaces a record result that does not fit one message with a
// failure. A mutation keeps its outcome evidence.
func oversized(event *pb.Event) *pb.Event {
	failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "result exceeds the message size limit")
	fitted := &pb.Event{}
	switch {
	case event.GetReadResult() != nil:
		fitted.Value = &pb.Event_ReadResult{ReadResult: protocol.ReadFailure(failure)}
	case event.GetMutationResult() != nil:
		outcome := event.GetMutationResult().GetOutcome()
		fitted.Value = &pb.Event_MutationResult{MutationResult: protocol.Mutation(outcome, failure)}
	default:
		return event
	}
	return fitted
}

func nativeFailure(failure *pb.Failure) *pb.Event {
	end := protocol.NativeFailure(false, failure)
	value := &pb.Event_NativeEnd{NativeEnd: end}
	event := &pb.Event{Value: value}
	return event
}

// send delivers one response; a client that stops reading for Stall loses the RPC.
func (s *Server) send(stream executeStream, index uint64, event *pb.Event) error {
	response, err := executeResponse(index, event)
	if err != nil {
		return err
	}
	timer := time.AfterFunc(s.limits.Stall, func() { s.cancelRPC(stream.Context(), "output") })
	defer timer.Stop()
	return sendResponse(stream, response)
}

// deliver sends one record result with the stream's reusable stall timer.
func (r *recordStream) deliver(result store.Result) error {
	response, err := executeResponse(result.Index, result.Event)
	if err != nil {
		return err
	}
	if r.output == nil {
		r.output = time.AfterFunc(r.server.limits.Stall, func() { r.server.cancelRPC(r.stream.Context(), "output") })
	} else {
		r.output.Reset(r.server.limits.Stall)
	}
	err = sendResponse(r.stream, response)
	r.output.Stop()
	return err
}

func executeResponse(index uint64, event *pb.Event) (*pb.ExecuteResponse, error) {
	response := &pb.ExecuteResponse{Index: index, Event: event}
	if proto.Size(response) > MaxMessageBytes {
		response.Event = oversized(event)
	}
	if err := protocol.ValidateExecuteResponse(response); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return response, nil
}

func sendResponse(stream executeStream, response *pb.ExecuteResponse) error {
	if err := stream.Send(response); err != nil {
		return err
	}
	if err := stream.Context().Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}
