package server

import (
	"context"
	"errors"
	"io"
	"math"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type receiveEnd struct {
	count uint64
	err   error
}

func (s *Server) Bulk(stream grpc.BidiStreamingServer[pb.BulkRequestFrame, pb.BulkResponseFrame]) error {
	ctx, cancel := context.WithTimeout(stream.Context(), s.limits.BulkLifetime)
	defer cancel()
	opening := make(chan *pb.BulkRequestFrame, 1)
	openError := make(chan error, 1)
	go func() {
		first, err := stream.Recv()
		if err != nil {
			openError <- err
			return
		}
		opening <- first
	}()
	timer := time.NewTimer(s.limits.Stall)
	defer timer.Stop()
	var first *pb.BulkRequestFrame
	select {
	case first = <-opening:
	case <-openError:
		return status.Error(codes.InvalidArgument, "Bulk requires Open")
	case <-timer.C:
		s.metrics.watchdogs.WithLabelValues("open").Inc()
		s.abortPeer(ctx)
		return status.Error(codes.DeadlineExceeded, "Bulk Open stalled")
	case <-ctx.Done():
		s.abortPeer(ctx)
		return status.FromContextError(ctx.Err()).Err()
	case <-s.draining:
		s.abortPeer(ctx)
		return status.Error(codes.Unavailable, "draining")
	}
	if first.GetOpen() == nil {
		return status.Error(codes.InvalidArgument, "Bulk requires Open")
	}
	service, name, failure := s.resolve(first.GetOpen().Store, true)
	if failure != nil {
		return status.Error(codes.InvalidArgument, "invalid Bulk Open")
	}
	if service.RemoteWeir != nil {
		return s.remoteBulk(stream, first, service.RemoteWeir, name)
	}
	runtime := service.LocalStore
	session := runtime.NewSession()
	defer session.Close()
	invalid := make(chan *pb.BulkResult, 1)
	ended := make(chan receiveEnd, 1)
	activity := make(chan struct{}, 1)
	args := receiveArgs{
		name:     name,
		runtime:  runtime,
		ctx:      ctx,
		stream:   stream,
		session:  session,
		invalid:  invalid,
		ended:    ended,
		activity: activity,
	}
	go s.receive(args)
	idle := time.NewTimer(s.limits.Stall)
	defer idle.Stop()
	var count, delivered uint64
	finished := false
	draining := s.draining
	drainMode := false
	terminalErr := status.Error(codes.Unavailable, "draining; unreported operations are indeterminate")
	for {
		if drainMode && session.Outstanding() == 0 {
			return terminalErr
		}
		if finished && delivered == count {
			end := &pb.BulkEnd{ReceivedCount: count, ResultCount: delivered}
			variant := &pb.BulkResponseFrame_End{End: end}
			frame := &pb.BulkResponseFrame{Frame: variant}
			return s.send(ctx, stream, frame)
		}
		var result *pb.BulkResult
		var ticket *store.Ticket
		select {
		case <-ctx.Done():
			if stream.Context().Err() == nil {
				s.abortPeer(stream.Context())
			}
			return status.FromContextError(ctx.Err()).Err()
		case <-draining:
			drainMode = true
			draining = nil
			continue
		case <-idle.C:
			s.metrics.watchdogs.WithLabelValues("input_or_result").Inc()
			s.abortPeer(stream.Context())
			return status.Error(codes.DeadlineExceeded, "input/result stall")
		case <-activity:
			idle.Reset(s.limits.Stall)
			continue
		case end := <-ended:
			if end.err != nil {
				// Overload closes input, not the already admitted result ledger.
				// Keep sending those results before returning the rejection.
				if status.Code(end.err) == codes.ResourceExhausted {
					drainMode = true
					terminalErr = end.err
					continue
				}
				select {
				case <-s.draining:
					drainMode = true
					draining = nil
					continue
				default:
				}
				return end.err
			}
			count = end.count
			finished = true
			continue
		case result = <-invalid:
		case ticket = <-session.Results:
			result = ticket.Result()
		}
		variant := &pb.BulkResponseFrame_Result{Result: result}
		frame := &pb.BulkResponseFrame{Frame: variant}
		err := s.send(ctx, stream, frame)
		if ticket != nil {
			ticket.Ack()
		}
		if err != nil {
			return err
		}
		delivered++
		idle.Reset(s.limits.Stall)
	}
}

type receiveArgs struct {
	name     string
	runtime  *store.Runtime
	ctx      context.Context
	stream   grpc.BidiStreamingServer[pb.BulkRequestFrame, pb.BulkResponseFrame]
	session  *store.Session
	invalid  chan<- *pb.BulkResult
	ended    chan<- receiveEnd
	activity chan<- struct{}
}

func (s *Server) receive(args receiveArgs) {
	ctx, stream, session, invalid, ended, activity := args.ctx, args.stream, args.session, args.invalid, args.ended, args.activity
	var count uint64
	finish := func(err error) {
		end := receiveEnd{count: count, err: err}
		ended <- end
	}
	for {
		select {
		case <-ctx.Done():
			finish(status.FromContextError(ctx.Err()).Err())
			return
		case <-s.draining:
			finish(status.Error(codes.Unavailable, "draining"))
			return
		default:
		}
		if err := s.admission.check(); err != nil {
			finish(err)
			return
		}
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			finish(nil)
			return
		}
		if err != nil {
			finish(err)
			return
		}
		select {
		case activity <- struct{}{}:
		default:
		}
		op := frame.GetOperation()
		if op == nil || op.Index != count || count == math.MaxUint64 {
			finish(status.Error(codes.InvalidArgument, "Bulk indexes must be consecutive"))
			return
		}
		count++
		f, operationErr := checkOperation(args.name, op)
		if f != nil {
			s.admission.rejections.WithLabelValues("operation").Inc()
		}
		if operationErr != nil {
			s.admission.rejections.WithLabelValues("operation").Inc()
			finish(operationErr)
			return
		}
		if err := s.admission.check(); err != nil {
			finish(err)
			return
		}
		var plan *execution.Plan
		if f == nil {
			plan, f = args.runtime.Prepare(op)
		}
		if f == nil {
			for {
				var changed <-chan struct{}
				_, f, changed = args.runtime.Submit(ctx, plan, session)
				if f == nil || f.Code != pb.FailureCode_RESOURCE_EXHAUSTED || args.runtime.Snapshot().Overloaded {
					break
				}
				select {
				case <-changed:
				case <-ctx.Done():
					finish(status.FromContextError(ctx.Err()).Err())
					return
				case <-s.draining:
					finish(status.Error(codes.Unavailable, "draining"))
					return
				}
			}
		}
		if f != nil {
			result := protocol.ResultError(op, pb.MutationOutcome_NOT_STARTED, f)
			select {
			case invalid <- result:
			case <-ctx.Done():
				finish(status.FromContextError(ctx.Err()).Err())
				return
			}
		}
		if args.runtime.Snapshot().Overloaded {
			finish(status.Error(codes.ResourceExhausted, "process overloaded"))
			return
		}
	}
}

func (s *Server) send(ctx context.Context, stream grpc.BidiStreamingServer[pb.BulkRequestFrame, pb.BulkResponseFrame], frame *pb.BulkResponseFrame) error {
	done := make(chan error, 1)
	go func() { done <- stream.Send(frame) }()
	timer := time.NewTimer(s.limits.Stall)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		if stream.Context().Err() == nil {
			s.abortPeer(stream.Context())
		}
		return status.FromContextError(ctx.Err()).Err()
	case <-timer.C:
		s.metrics.watchdogs.WithLabelValues("output").Inc()
		s.abortPeer(stream.Context())
		return status.Error(codes.DeadlineExceeded, "result send stalled")
	}
}
