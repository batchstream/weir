package server

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const relayOutstanding = 8

type bulkIndex struct {
	original uint64
	read     bool
}
type bulkReject struct {
	result *pb.BulkResult
	ack    chan struct{}
}
type bulkResponse struct {
	frame *pb.BulkResponseFrame
	err   error
}
type bulkInput struct {
	frame *pb.BulkRequestFrame
	err   error
}
type bulkRelay struct {
	cause               error
	ctx                 context.Context
	cancel              context.CancelFunc
	upstream            grpc.BidiStreamingServer[pb.BulkRequestFrame, pb.BulkResponseFrame]
	downstream          grpc.BidiStreamingClient[pb.BulkRequestFrame, pb.BulkResponseFrame]
	delivery            *delivery
	name                string
	mu                  sync.Mutex
	received, forwarded uint64
	indexes             map[uint64]bulkIndex
	halfClosed, drain   bool
	credit              chan struct{}
	requests            chan struct{}
	inputs              chan bulkInput
	invalid             chan bulkReject
	uploadDone          chan struct{}
}

func (s *Server) remoteBulk(upstream grpc.BidiStreamingServer[pb.BulkRequestFrame, pb.BulkResponseFrame], first *pb.BulkRequestFrame, remote *RemoteWeir, name string) (resultErr error) {
	d := upstream.Context().Value(deliveryKey).(*delivery)
	defer d.beginResponse()
	next, err := forwardContext(upstream.Context())
	if err != nil {
		s.admission.rejections.WithLabelValues("hop").Inc()
		return err
	}
	if err := remote.enter(d); err != nil {
		return err
	}
	defer func() { remote.terminations.WithLabelValues("Bulk", statusLabel(resultErr)).Inc() }()
	ctx, cancel := context.WithCancel(next)
	defer cancel()
	downstream, err := remote.client.Bulk(ctx)
	if err != nil {
		return err
	}
	_ = downstream.Context()
	timer := time.AfterFunc(s.limits.Stall, cancel)
	err = downstream.Send(first)
	timer.Stop()
	if err != nil {
		return err
	}
	relay := &bulkRelay{ctx: ctx, cancel: cancel, upstream: upstream, downstream: downstream, delivery: d, name: name, indexes: make(map[uint64]bulkIndex), credit: make(chan struct{}, relayOutstanding), requests: make(chan struct{}), inputs: make(chan bulkInput), invalid: make(chan bulkReject), uploadDone: make(chan struct{})}
	inputDone := make(chan struct{})
	d.mu.Lock()
	d.nativePump = inputDone
	d.mu.Unlock()
	go relay.readInput(inputDone)
	go s.uploadBulk(relay)
	responses := make(chan bulkResponse)
	demand := make(chan struct{}, 1)
	responseDone := make(chan struct{})
	go func() {
		defer close(responseDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-demand:
			}
			timer := time.AfterFunc(s.limits.Stall, cancel)
			frame, err := downstream.Recv()
			timer.Stop()
			response := bulkResponse{frame: frame, err: err}
			select {
			case <-ctx.Done():
				return
			case responses <- response:
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() { cancel(); <-relay.uploadDone; <-responseDone }()
	demand <- struct{}{}
	var delivered uint64
	var terminal *pb.BulkResponseFrame
	for {
		select {
		case <-ctx.Done():
			return relay.failure(status.FromContextError(ctx.Err()).Err())
		case invalid := <-relay.invalid:
			variant := &pb.BulkResponseFrame_Result{Result: invalid.result}
			frame := &pb.BulkResponseFrame{Frame: variant}
			if err := upstream.Send(frame); err != nil {
				return err
			}
			delivered++
			<-relay.credit
			close(invalid.ack)
		case response := <-responses:
			if response.err != nil {
				if !errors.Is(response.err, io.EOF) || terminal == nil {
					return relay.failure(remote.incomplete("Bulk", response.err, "missing Bulk End"))
				}
				<-relay.uploadDone
				relay.mu.Lock()
				end := terminal.GetEnd()
				valid := relay.halfClosed && len(relay.indexes) == 0 && end.ReceivedCount == relay.forwarded && end.ResultCount == relay.forwarded && delivered == relay.received
				received, draining := relay.received, relay.drain
				relay.mu.Unlock()
				if !valid {
					return status.Error(codes.Internal, "invalid peer Bulk counts")
				}
				if draining {
					return status.Error(codes.Unavailable, "draining; unreported operations are indeterminate")
				}
				end.ReceivedCount = received
				end.ResultCount = delivered
				return upstream.Send(terminal)
			}
			if terminal != nil {
				remote.incompletes.WithLabelValues("Bulk").Inc()
				return status.Error(codes.Internal, "Bulk frames after End")
			}
			if end := response.frame.GetEnd(); end != nil {
				terminal = response.frame
				demand <- struct{}{}
				continue
			}
			result := response.frame.GetResult()
			if result == nil {
				return status.Error(codes.Internal, "invalid peer Bulk frame")
			}
			relay.mu.Lock()
			index, ok := relay.indexes[result.Index]
			if ok {
				delete(relay.indexes, result.Index)
			}
			relay.mu.Unlock()
			if !ok || index.read != (result.GetRead() != nil) || !index.read && result.GetMutation() == nil {
				return status.Error(codes.Internal, "invalid peer Bulk correlation")
			}
			result.Index = index.original
			if err := upstream.Send(response.frame); err != nil {
				return err
			}
			delivered++
			<-relay.credit
			demand <- struct{}{}
		}
	}
}

// Only one requested frame may be held. A blocked upstream Recv is joined by
// delivery after gRPC sends final status; it cannot overwrite an early terminal.
func (r *bulkRelay) readInput(done chan<- struct{}) {
	defer close(done)
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.requests:
		}
		r.delivery.nativeRead(true)
		frame, err := r.upstream.Recv()
		r.delivery.nativeRead(false)
		input := bulkInput{frame: frame, err: err}
		select {
		case <-r.ctx.Done():
			return
		case r.inputs <- input:
		}
		if err != nil {
			return
		}
	}
}
func (s *Server) uploadBulk(r *bulkRelay) {
	defer close(r.uploadDone)
	for {
		if err := s.admission.check(); err != nil {
			r.finishInput(true)
			return
		}
		select {
		case <-r.ctx.Done():
			return
		case <-s.draining:
			r.finishInput(true)
			return
		case r.credit <- struct{}{}:
		}
		select {
		case <-r.ctx.Done():
			return
		case <-s.draining:
			r.finishInput(true)
			return
		case r.requests <- struct{}{}:
		}
		var input bulkInput
		select {
		case <-r.ctx.Done():
			return
		case <-s.draining:
			r.finishInput(true)
			return
		case input = <-r.inputs:
		}
		if errors.Is(input.err, io.EOF) {
			<-r.credit
			r.finishInput(false)
			return
		}
		if input.err != nil {
			r.cancel()
			return
		}
		if err := s.admission.check(); err != nil {
			r.finishInput(true)
			return
		}
		op := input.frame.GetOperation()
		r.mu.Lock()
		count := r.received
		r.mu.Unlock()
		if op == nil || op.Index != count || count == math.MaxUint64 {
			r.fail(status.Error(codes.InvalidArgument, "Bulk indexes must be consecutive"))
			return
		}
		failure, err := checkOperation(r.ctx, r.name, op)
		if failure != nil {
			s.admission.rejections.WithLabelValues("operation").Inc()
		}
		if err != nil {
			s.admission.rejections.WithLabelValues("permission").Inc()
			r.fail(err)
			return
		}
		r.mu.Lock()
		r.received++
		r.mu.Unlock()
		if failure != nil {
			rejected := bulkReject{result: protocol.ResultError(op, pb.MutationOutcome_NOT_STARTED, failure), ack: make(chan struct{})}
			select {
			case <-r.ctx.Done():
				return
			case r.invalid <- rejected:
			}
			select {
			case <-r.ctx.Done():
				return
			case <-rejected.ack:
			}
			continue
		}
		r.mu.Lock()
		index := bulkIndex{original: op.Index, read: op.GetRead() != nil}
		op.Index = r.forwarded
		r.indexes[op.Index] = index
		r.forwarded++
		r.mu.Unlock()
		timer := time.AfterFunc(s.limits.Stall, r.cancel)
		err = r.downstream.Send(input.frame)
		timer.Stop()
		if err != nil {
			return
		}
	}
}
func (r *bulkRelay) finishInput(draining bool) {
	r.mu.Lock()
	r.halfClosed = true
	r.drain = draining
	r.mu.Unlock()
	_ = r.downstream.CloseSend()
}

func (r *bulkRelay) fail(err error) {
	r.mu.Lock()
	r.cause = err
	r.mu.Unlock()
	r.cancel()
}
func (r *bulkRelay) failure(err error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cause != nil {
		return r.cause
	}
	return err
}
