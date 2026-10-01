package server

import (
	"context"
	"errors"
	"io"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type routeRelay struct {
	ctx      context.Context
	cancel   context.CancelFunc
	stream   grpc.BidiStreamingServer[pb.Request, pb.Response]
	first    *pb.Request
	remote   *RemoteWeir
	ledger   *routeLedger
	delivery *delivery
}

func (s *Server) remoteRoute(args routeRelay) (resultErr error) {
	next, err := forwardContext(args.ctx)
	if err != nil {
		s.admission.rejections.WithLabelValues("hop").Inc()
		return err
	}
	if err := args.remote.enter(args.delivery); err != nil {
		return err
	}
	defer func() { args.remote.terminations.WithLabelValues("Route", statusLabel(resultErr)).Inc() }()
	ctx, cancel := context.WithCancel(next)
	defer cancel()
	state := ctx.Value(ingressKey).(*ingress)
	client, err := args.remote.selectClient(ctx, state.diagnostic.Get("weir-request-id")[0])
	if err != nil {
		return err
	}
	downstream, err := client.Route(ctx)
	if err != nil {
		return err
	}
	// Commit the attempt and disable gRPC replay history before the first Send.
	_ = downstream.Context()
	uploaded := make(chan error, 1)
	uploadDone := make(chan struct{})
	args.delivery.setPump(uploadDone)
	go func(upload routeRelay) {
		defer close(uploadDone)
		err := s.uploadRemote(upload, ctx, cancel, downstream)
		select {
		case uploaded <- err:
		case <-ctx.Done():
		}
	}(args)
	args.first = nil
	// Exactly one response chunk is held between downstream Recv and upstream
	// Send. A blocked Send prevents further Recv and propagates HTTP/2 flow control.
	for {
		timer := time.AfterFunc(s.limits.Stall, cancel)
		frame, recvErr := downstream.Recv()
		timer.Stop()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				if !args.ledger.complete() {
					return args.ledger.failure(args.remote.incomplete("Route", recvErr, "incomplete downstream responses"))
				}
				select {
				case uploadErr := <-uploaded:
					if uploadErr != nil {
						return uploadErr
					}
				case <-ctx.Done():
					return status.FromContextError(ctx.Err()).Err()
				}
				if !args.ledger.complete() {
					return args.remote.incomplete("Route", recvErr, "incomplete downstream responses")
				}
				if args.ledger.truncated() {
					return status.Error(codes.Unavailable, "draining; incomplete writes are indeterminate")
				}
				return nil
			}
			// A local upload protocol failure is more informative than cancellation of
			// its downstream stream. No error implies uncompleted writes did not happen.
			select {
			case uploadErr := <-uploaded:
				if uploadErr != nil {
					return uploadErr
				}
			default:
			}
			return args.ledger.failure(recvErr)
		}
		if err := protocol.ValidateResponse(frame); err != nil {
			return status.Error(codes.Internal, "invalid downstream response envelope")
		}
		if !args.ledger.contains(frame.Id) {
			return status.Error(codes.Internal, "unknown or completed downstream request ID")
		}
		if err := s.sendRoute(args.ctx, args.stream, frame); err != nil {
			return err
		}
		if frame.End {
			args.ledger.release(frame.Id)
		}
	}
}

func (s *Server) uploadRemote(args routeRelay, ctx context.Context, cancel context.CancelFunc, downstream grpc.BidiStreamingClient[pb.Request, pb.Response]) error {
	request := args.first
	destination := request.Destination
	args.first = nil
	for {
		timer := time.AfterFunc(s.limits.Stall, cancel)
		err := downstream.Send(request)
		timer.Stop()
		if err != nil {
			// Send EOF is not authoritative: downstream Recv owns the final status.
			if errors.Is(err, io.EOF) {
				return nil
			}
			cancel()
			return err
		}
		request = nil
		if err := args.ledger.wait(ctx); err != nil {
			if args.ledger.isDraining() {
				args.ledger.finishDrain()
				return downstream.CloseSend()
			}
			return err
		}
		args.delivery.inputRead(true)
		request, err = args.stream.Recv()
		args.delivery.inputRead(false)
		if errors.Is(err, io.EOF) {
			args.ledger.finishInput()
			return downstream.CloseSend()
		}
		if err != nil {
			if args.ledger.isDraining() {
				args.ledger.finishDrain()
				return downstream.CloseSend()
			}
			cancel()
			return err
		}
		if args.ledger.isDraining() {
			args.ledger.finishDrain()
			return downstream.CloseSend()
		}
		if err := protocol.ValidateRequest(request, destination, args.ledger.lastID); err != nil {
			failure := status.Error(codes.InvalidArgument, err.Error())
			args.ledger.fail(failure)
			cancel()
			return failure
		}
		if err := s.admission.check(); err != nil {
			cancel()
			return err
		}
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		if !args.ledger.add(request) {
			return status.Error(codes.Canceled, "route closed")
		}
	}
}

func incomplete(err error, message string) error {
	if err == nil || errors.Is(err, io.EOF) {
		return status.Error(codes.Internal, message)
	}
	return err
}

func (r *RemoteWeir) enter(d *delivery) error {
	select {
	case r.slots <- struct{}{}:
		d.mu.Lock()
		if d.finished {
			d.mu.Unlock()
			<-r.slots
			return status.Error(codes.Canceled, "delivery closed")
		}
		d.remoteSlots = r.slots
		d.mu.Unlock()
		return nil
	default:
		r.rejections.WithLabelValues("relay").Inc()
		return status.Error(codes.ResourceExhausted, "peer relay bound")
	}
}
