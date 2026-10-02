package server

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type ExecutionSnapshot struct {
	ActiveRPCs, Outstanding, OutstandingBytes int64
	PeakOutstanding, PeakOutstandingBytes     int64
}

type executionCounters struct {
	outstanding, bytes, peakOutstanding, peakBytes atomic.Int64
}

func (s *Server) Snapshot() ExecutionSnapshot {
	snapshot := ExecutionSnapshot{
		ActiveRPCs:           int64(len(s.slots)),
		Outstanding:          s.executionStats.outstanding.Load(),
		OutstandingBytes:     s.executionStats.bytes.Load(),
		PeakOutstanding:      s.executionStats.peakOutstanding.Load(),
		PeakOutstandingBytes: s.executionStats.peakBytes.Load(),
	}
	return snapshot
}

func raisePeak(peak *atomic.Int64, value int64) {
	for previous := peak.Load(); value > previous; previous = peak.Load() {
		if peak.CompareAndSwap(previous, value) {
			return
		}
	}
}

// The ledger retains only unfinished IDs and byte charges. It never holds a
// request payload or historical ID set. A single uploader owns lastID.
type executionLedger struct {
	mu           sync.Mutex
	server       *Server
	ids          map[uint64]int
	bytes        int
	lastID       uint64
	closed       bool
	clientClosed bool
	stopped      bool
	cause        error
	draining     bool
	changed      chan struct{}
}

func newExecutionLedger(s *Server) *executionLedger {
	ledger := &executionLedger{server: s, ids: make(map[uint64]int), changed: make(chan struct{})}
	return ledger
}

func (l *executionLedger) notify() { close(l.changed); l.changed = make(chan struct{}) }

// Reserve room for a worst-case frame before calling Recv. This also accounts
// for one decoded request waiting for scheduler admission or downstream Send.
func (l *executionLedger) wait(ctx context.Context) error {
	for {
		l.mu.Lock()
		draining := l.draining
		ready := len(l.ids) < protocol.RouteOutstanding && l.bytes+protocol.MaxFrame <= protocol.RouteBytes
		changed := l.changed
		l.mu.Unlock()
		if draining {
			return status.Error(codes.Unavailable, "draining")
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-changed:
		}
	}
}

func (l *executionLedger) add(request *pb.ExecuteRequest) bool {
	size := proto.Size(request)
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return false
	}
	l.ids[request.RequestId] = size
	l.bytes += size
	l.lastID = request.RequestId
	l.notify()
	count := l.server.executionStats.outstanding.Add(1)
	bytes := l.server.executionStats.bytes.Add(int64(size))
	raisePeak(&l.server.executionStats.peakOutstanding, count)
	raisePeak(&l.server.executionStats.peakBytes, bytes)
	l.mu.Unlock()
	return true
}

func (l *executionLedger) contains(id uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.ids[id]
	return ok
}

func (l *executionLedger) release(id uint64) bool {
	l.mu.Lock()
	size, ok := l.ids[id]
	if ok {
		delete(l.ids, id)
		l.bytes -= size
		l.notify()
		l.server.executionStats.outstanding.Add(-1)
		l.server.executionStats.bytes.Add(-int64(size))
	}
	l.mu.Unlock()
	return ok
}

func (l *executionLedger) finishInput() {
	l.mu.Lock()
	l.closed = true
	l.clientClosed = true
	l.notify()
	l.mu.Unlock()
}
func (l *executionLedger) finishDrain() {
	l.mu.Lock()
	l.closed = true
	l.draining = true
	l.notify()
	l.mu.Unlock()
}
func (l *executionLedger) truncated() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.draining && !l.clientClosed
}
func (l *executionLedger) complete() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed && len(l.ids) == 0
}
func (l *executionLedger) clear() {
	l.mu.Lock()
	l.stopped = true
	count, bytes := len(l.ids), l.bytes
	clear(l.ids)
	l.bytes = 0
	l.notify()
	l.server.executionStats.outstanding.Add(-int64(count))
	l.server.executionStats.bytes.Add(-int64(bytes))
	l.mu.Unlock()
}

func (l *executionLedger) startDrain()      { l.mu.Lock(); l.draining = true; l.notify(); l.mu.Unlock() }
func (l *executionLedger) isDraining() bool { l.mu.Lock(); defer l.mu.Unlock(); return l.draining }
func (l *executionLedger) empty() bool      { l.mu.Lock(); defer l.mu.Unlock(); return len(l.ids) == 0 }

func (l *executionLedger) fail(err error) { l.mu.Lock(); l.cause = err; l.mu.Unlock() }
func (l *executionLedger) failure(err error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cause != nil {
		return l.cause
	}
	return err
}

func (s *Server) Execute(stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	delivery, ok := ctx.Value(deliveryKey).(*delivery)
	if !ok {
		return status.Error(codes.Internal, "missing HTTP/2 delivery lifetime")
	}
	delivery.inputRead(true)
	first, err := stream.Recv()
	delivery.inputRead(false)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := protocol.ValidateExecuteRequest(first, "", 0); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	runtime, err := s.hostedStore(first.StoreName)
	if err != nil {
		return err
	}
	ledger := newExecutionLedger(s)
	defer func() { cancel(); ledger.clear() }()
	ledger.add(first)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-s.draining:
			ledger.startDrain()
			delivery.stopInput()
		case <-ctx.Done():
		}
	}()
	defer func() { cancel(); <-watchDone }()
	args := localExecutionArgs{ctx: ctx, cancel: cancel, stream: stream, first: first, runtime: runtime, ledger: ledger, delivery: delivery}
	return s.localExecution(args)
}

type executionFailure struct {
	id    uint64
	event *pb.Event
}
type localExecutionArgs struct {
	ctx      context.Context
	cancel   context.CancelFunc
	stream   grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse]
	first    *pb.ExecuteRequest
	runtime  *store.Runtime
	ledger   *executionLedger
	delivery *delivery
}

func (s *Server) localExecution(args localExecutionArgs) error {
	session := args.runtime.NewSession()
	defer session.Close()
	invalid := make(chan executionFailure, 1)
	uploaded := make(chan error, 1)
	uploadDone := make(chan struct{})
	args.delivery.setPump(uploadDone)
	go func(upload localExecutionArgs) {
		defer close(uploadDone)
		err := s.uploadLocal(upload, session, invalid)
		select {
		case uploaded <- err:
		case <-args.ctx.Done():
		}
	}(args)
	args.first = nil
	idle := time.NewTimer(s.limits.Stall)
	defer idle.Stop()
	inputFinished := false
	draining := s.draining
	drainMode := false
	for {
		if inputFinished && args.ledger.complete() {
			if drainMode && args.ledger.truncated() {
				return status.Error(codes.Unavailable, "draining; incomplete writes are indeterminate")
			}
			return nil
		}
		if drainMode && args.ledger.empty() {
			return status.Error(codes.Unavailable, "draining")
		}
		select {
		case <-args.ctx.Done():
			return status.FromContextError(args.ctx.Err()).Err()
		case <-draining:
			drainMode = true
			draining = nil
			args.ledger.startDrain()
			args.delivery.stopInput()
			continue
		case <-idle.C:
			s.metrics.watchdogs.WithLabelValues("input_or_result").Inc()
			s.abortPeer(args.stream.Context())
			return status.Error(codes.DeadlineExceeded, "input/result stalled")
		case err := <-uploaded:
			if err != nil && !drainMode {
				select {
				case <-s.draining:
					drainMode = true
					draining = nil
					args.ledger.startDrain()
				default:
					return err
				}
			}
			inputFinished = true
			idle.Reset(s.limits.Stall)
		case rejected := <-invalid:
			if err := s.sendEvent(args.ctx, args.stream, rejected.id, rejected.event); err != nil {
				return err
			}
			end := &pb.ExecuteResponse{RequestId: rejected.id, RequestComplete: true}
			if err := s.sendExecutionResponse(args.ctx, args.stream, end); err != nil {
				return err
			}
			args.ledger.release(rejected.id)
			idle.Reset(s.limits.Stall)
		case emission := <-session.Events:
			if emission == nil || emission.Ticket == nil {
				return status.Error(codes.Internal, "invalid execution emission")
			}
			id := emission.Ticket.ID()
			if !args.ledger.contains(id) {
				return status.Error(codes.Internal, "uncorrelated execution emission")
			}
			if emission.End {
				end := &pb.ExecuteResponse{RequestId: id, RequestComplete: true}
				if err := s.sendExecutionResponse(args.ctx, args.stream, end); err != nil {
					return err
				}
				emission.Release()
				emission.Ticket.Ack()
				args.ledger.release(id)
			} else if err := s.sendEvent(args.ctx, args.stream, id, emission.Event); err != nil {
				return err
			} else {
				emission.Release()
			}
			idle.Reset(s.limits.Stall)
		}
	}
}

func (s *Server) uploadLocal(args localExecutionArgs, session *store.Session, invalid chan<- executionFailure) error {
	request := args.first
	storeName := request.StoreName
	args.first = nil
	for {
		if err := s.admission.check(); err != nil {
			return err
		}
		call, err := protocol.DecodeCall(request.CallPayload)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		plan, failure := args.runtime.PrepareCall(request.RequestId, call)
		if failure == nil {
			for {
				var changed <-chan struct{}
				_, failure, changed = args.runtime.Submit(args.ctx, plan, session)
				if failure == nil || failure.Code != pb.FailureCode_RESOURCE_EXHAUSTED || args.runtime.Snapshot().Overloaded {
					break
				}
				select {
				case <-args.ctx.Done():
					return status.FromContextError(args.ctx.Err()).Err()
				case <-s.draining:
					return status.Error(codes.Unavailable, "draining")
				case <-changed:
				}
			}
		}
		if failure != nil {
			event := failedCall(request.RequestId, call, failure)
			rejected := executionFailure{id: request.RequestId, event: event}
			select {
			case invalid <- rejected:
			case <-args.ctx.Done():
				return status.FromContextError(args.ctx.Err()).Err()
			}
		}
		// Drop the decoded Call and complete envelope before waiting or receiving.
		call = nil
		plan = nil
		request = nil
		if err := args.ledger.wait(args.ctx); err != nil {
			return err
		}
		args.delivery.inputRead(true)
		request, err = args.stream.Recv()
		args.delivery.inputRead(false)
		if errors.Is(err, io.EOF) {
			args.ledger.finishInput()
			return nil
		}
		if err != nil {
			return err
		}
		if err := protocol.ValidateExecuteRequest(request, storeName, args.ledger.lastID); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if err := s.admission.check(); err != nil {
			return err
		}
		if args.ctx.Err() != nil {
			return status.FromContextError(args.ctx.Err()).Err()
		}
		if !args.ledger.add(request) {
			return status.Error(codes.Canceled, "execution closed")
		}
	}
}

func failedCall(id uint64, call *pb.Call, failure *pb.Failure) *pb.Event {
	event := &pb.Event{Version: 1}
	switch operation := call.Operation.(type) {
	case *pb.Call_Read:
		read := protocol.ReadFailure(failure)
		value := &pb.Result_Read{Read: read}
		result := &pb.Result{Index: id, Result: value}
		event.Value = &pb.Event_Result{Result: result}
	case *pb.Call_Mutate:
		mutation := protocol.Mutation(pb.MutationOutcome_NOT_STARTED, failure)
		value := &pb.Result_Mutation{Mutation: mutation}
		result := &pb.Result{Index: id, Result: value}
		event.Value = &pb.Event_Result{Result: result}
	case *pb.Call_Scan:
		end := &pb.ScanEnd{Failure: failure}
		event.Value = &pb.Event_ScanEnd{ScanEnd: end}
	case *pb.Call_Native:
		end := protocol.NativeFailure(false, failure)
		event.Value = &pb.Event_NativeEnd{NativeEnd: end}
	default:
		_ = operation
	}
	return event
}

func (s *Server) sendEvent(ctx context.Context, stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse], id uint64, event *pb.Event) error {
	data, err := protocol.MarshalEvent(event)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	for len(data) > 0 {
		count := min(len(data), protocol.NativeChunk)
		response := &pb.ExecuteResponse{RequestId: id, EventFragment: data[:count]}
		if err := s.sendExecutionResponse(ctx, stream, response); err != nil {
			return err
		}
		data = data[count:]
	}
	return nil
}

func (s *Server) sendExecutionResponse(ctx context.Context, stream grpc.BidiStreamingServer[pb.ExecuteRequest, pb.ExecuteResponse], frame *pb.ExecuteResponse) error {
	timer := time.AfterFunc(s.limits.Stall, func() { s.metrics.watchdogs.WithLabelValues("output").Inc(); s.abortPeer(stream.Context()) })
	stop := context.AfterFunc(ctx, func() {
		if stream.Context().Err() == nil {
			// Cancellation belongs to this RPC. Keep other streams on the
			// same connection alive; the separate stall watchdog still closes
			// a peer whose transport cannot make progress.
			if state, ok := ctx.Value(deliveryKey).(*delivery); ok {
				state.shorten(time.Now())
			}
		}
	})
	err := stream.Send(frame)
	timer.Stop()
	stop()
	return err
}

func (d *delivery) inputRead(waiting bool) {
	d.mu.Lock()
	d.inputWaiting = waiting
	if !d.finished && !d.inputStopped {
		deadline := d.deadline
		if waiting {
			deadline = minTime(deadline, time.Now().Add(d.stall))
		}
		d.readDeadline = deadline
		_ = d.controller.SetReadDeadline(deadline)
	}
	d.mu.Unlock()
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func (d *delivery) setPump(done <-chan struct{}) { d.mu.Lock(); d.inputPump = done; d.mu.Unlock() }

func (d *delivery) stopInput() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.finished {
		d.inputStopped = true
		d.readDeadline = time.Now()
		_ = d.controller.SetReadDeadline(d.readDeadline)
	}
}
