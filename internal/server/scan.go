package server

import (
	"math"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) Scan(req *pb.ScanRequest, stream grpc.ServerStreamingServer[pb.ScanResponseFrame]) error {
	ctx := stream.Context()
	delivery, ok := ctx.Value(deliveryKey).(*delivery)
	if !ok {
		return status.Error(codes.Internal, "missing HTTP/2 delivery lifetime")
	}
	// As for unary, the final status keeps a native transport deadline after
	// this handler returns. No per-document goroutine or unbounded send queue.
	defer delivery.beginResponse()
	var count uint64
	end := func(f *pb.Failure) error {
		terminal := &pb.ScanEnd{DocumentCount: count, Failure: f}
		variant := &pb.ScanResponseFrame_End{End: terminal}
		frame := &pb.ScanResponseFrame{Frame: variant}
		return stream.Send(frame)
	}
	runtime, failure := s.resolve(req.GetResource(), false)
	if failure != nil {
		return end(failure)
	}
	ticket, failure := runtime.StartScan(ctx, req)
	if failure != nil {
		return end(failure)
	}
	defer ticket.CloseScan()
	for {
		page, err := ticket.WaitPage(ctx)
		if err != nil {
			return status.FromContextError(err).Err()
		}
		for _, document := range page.Documents {
			if ctx.Err() != nil {
				return status.FromContextError(ctx.Err()).Err()
			}
			if count == math.MaxUint64 {
				return end(protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Scan count limit"))
			}
			variant := &pb.ScanResponseFrame_Document{Document: document}
			frame := &pb.ScanResponseFrame{Frame: variant}
			if err := stream.Send(frame); err != nil {
				return err
			}
			count++
		}
		if page.Failure != nil || page.Exhausted {
			failure := page.Failure
			page = nil
			if cleanup := ticket.CloseScan(); failure == nil {
				failure = cleanup
			}
			return end(failure)
		}
		page = nil
		if !ticket.AdvanceScan() {
			return status.Error(codes.Canceled, "Scan stopped")
		}
	}
}
