package server

import (
	"io"
	"sync"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// One receiver and one sender per admitted exchange, with one bounded frame
// handoff and no goroutine per frame. HTTP/2 read-ahead keeps its credit.
type nativeStream struct {
	stream   grpc.BidiStreamingServer[pb.NativeRequestFrame, pb.NativeResponseFrame]
	delivery *delivery
	chunk    []byte
	readErr  error
	stop     chan struct{}
	requests chan struct{}
	received chan nativeReceive
	done     chan struct{}
	start    sync.Once
	once     sync.Once
}

func (s *Server) Native(stream grpc.BidiStreamingServer[pb.NativeRequestFrame, pb.NativeResponseFrame]) error {
	delivery, ok := stream.Context().Value(deliveryKey).(*delivery)
	if !ok {
		return status.Error(codes.Internal, "missing HTTP/2 delivery lifetime")
	}
	duplex := &nativeStream{stream: stream, delivery: delivery, stop: make(chan struct{}), requests: make(chan struct{}), received: make(chan nativeReceive, 1), done: make(chan struct{})}
	defer delivery.beginResponse()
	defer duplex.Close()
	delivery.nativeRead(true)
	first, err := stream.Recv()
	delivery.nativeRead(false)
	if err != nil {
		return err
	}
	if first.GetOpen() == nil {
		return status.Error(codes.InvalidArgument, "Native requires Open")
	}
	runtime, failure := s.resolve(first.GetOpen().Resource, false)
	var end *pb.NativeEnd
	if failure == nil {
		exchange := &execution.NativeExchange{Source: duplex, Sink: duplex}
		ticket, rejected := runtime.StartNative(stream.Context(), first.GetOpen(), exchange)
		failure = rejected
		if failure == nil {
			// The same bounded runner sends response frames and retains the execution
			// permit until the backend exchange and cleanup finish.
			end = ticket.WaitNative()
			delivery.retain(ticket)
		}
	}
	if failure != nil {
		end = protocol.NativeFailure(false, failure)
	}
	_ = duplex.Close()
	variant := &pb.NativeResponseFrame_End{End: end}
	frame := &pb.NativeResponseFrame{Frame: variant}
	return stream.Send(frame)
}

type nativeReceive struct {
	frame *pb.NativeRequestFrame
	err   error
}

func (s *nativeStream) receive() {
	defer close(s.done)
	for {
		select {
		case <-s.stop:
			return
		case <-s.requests:
		}
		frame, err := s.stream.Recv()
		result := nativeReceive{frame: frame, err: err}
		select {
		case <-s.stop:
			return
		case s.received <- result:
		}
		if err != nil {
			return
		}
	}
}
func (s *nativeStream) Read(dst []byte) (int, error) {
	select {
	case <-s.stop:
		return 0, io.ErrClosedPipe
	default:
	}
	if s.readErr != nil {
		return 0, s.readErr
	}
	if len(dst) == 0 {
		return 0, nil
	}
	if len(s.chunk) == 0 {
		// Close wakes the adapter without forcing an error through grpc.RecvMsg,
		// which writes its own terminal status. A pending Recv exits after final
		// status/transport cancellation; delivery joins it before releasing credit.
		s.start.Do(func() {
			s.delivery.mu.Lock()
			if s.delivery.finished {
				s.readErr = io.ErrClosedPipe
				close(s.done)
			} else {
				s.delivery.nativePump = s.done
				go s.receive()
			}
			s.delivery.mu.Unlock()
		})
		if s.readErr != nil {
			return 0, s.readErr
		}
		s.delivery.nativeRead(true)
		defer s.delivery.nativeRead(false)
		select {
		case <-s.stop:
			return 0, io.ErrClosedPipe
		case s.requests <- struct{}{}:
		}
		var result nativeReceive
		select {
		case <-s.stop:
			return 0, io.ErrClosedPipe
		case result = <-s.received:
		}
		if result.err != nil {
			s.readErr = result.err
			return 0, result.err
		}
		variant, ok := result.frame.Frame.(*pb.NativeRequestFrame_Chunk)
		if !ok || len(variant.Chunk) == 0 || len(variant.Chunk) > protocol.NativeChunk {
			return 0, status.Error(codes.InvalidArgument, "expected nonempty bounded Native chunk")
		}
		s.chunk = variant.Chunk
	}
	n := copy(dst, s.chunk)
	s.chunk = s.chunk[n:]
	return n, nil
}
func (s *nativeStream) Close() error {
	s.once.Do(func() { close(s.stop) })
	s.delivery.nativeRead(false)
	return nil
}
func (s *nativeStream) Head(head *pb.NativeHead) error {
	variant := &pb.NativeResponseFrame_Head{Head: head}
	frame := &pb.NativeResponseFrame{Frame: variant}
	return s.stream.Send(frame)
}
func (s *nativeStream) Chunk(raw []byte) error {
	variant := &pb.NativeResponseFrame_Chunk{Chunk: raw}
	frame := &pb.NativeResponseFrame{Frame: variant}
	return s.stream.Send(frame)
}
func (s *nativeStream) Interrupt() {
	d := s.delivery
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.finished {
		_ = d.controller.SetWriteDeadline(time.Now())
	}
}

// Only an outstanding application Recv has an input stall deadline. Queuing,
// item validation and backend/output backpressure are not slow client upload.
func (d *delivery) nativeRead(waiting bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.finished {
		return
	}
	d.nativeWaiting = waiting
	deadline := d.deadline
	if waiting {
		deadline = minTime(deadline, time.Now().Add(d.stall))
	}
	d.readDeadline = deadline
	_ = d.controller.SetReadDeadline(deadline)
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
