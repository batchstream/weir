package server

import (
	"context"
	"errors"
	"io"
	"math"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) remoteSingle(ctx context.Context, remote *RemoteWeir, op *pb.BulkOperation, d *delivery) (*pb.BulkResult, error) {
	next, err := forwardContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := remote.enter(d); err != nil {
		return nil, err
	}
	result := &pb.BulkResult{Index: op.Index}
	if req := op.GetRead(); req != nil {
		read, err := remote.client.Read(next, req)
		if err != nil {
			return nil, err
		}
		result.Result = &pb.BulkResult_Read{Read: read}
	} else {
		mutation, err := remote.client.Mutate(next, op.GetMutate())
		if err != nil {
			return nil, err
		}
		result.Result = &pb.BulkResult_Mutation{Mutation: mutation}
	}
	return result, nil
}

func (s *Server) remoteScan(args scanRelay) error {
	next, err := forwardContext(args.stream.Context())
	if err != nil {
		return err
	}
	if err := args.remote.enter(args.delivery); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(next)
	defer cancel()
	downstream, err := args.remote.client.Scan(ctx, args.request)
	if err != nil {
		return err
	}
	var count uint64
	for {
		timer := time.AfterFunc(s.limits.Stall, cancel)
		frame, err := downstream.Recv()
		timer.Stop()
		if err != nil {
			return incomplete(err, "missing Scan End")
		}
		if doc := frame.GetDocument(); doc != nil {
			if count == math.MaxUint64 || len(doc.Data) > protocol.MaxDocument {
				return status.Error(codes.Internal, "invalid peer Scan document")
			}
			if err := args.stream.Send(frame); err != nil {
				return err
			}
			count++
			continue
		}
		end := frame.GetEnd()
		if end == nil || end.DocumentCount != count {
			return status.Error(codes.Internal, "invalid peer Scan End")
		}
		timer = time.AfterFunc(s.limits.Stall, cancel)
		_, err = downstream.Recv()
		timer.Stop()
		if !errors.Is(err, io.EOF) {
			return incomplete(err, "Scan frames after End")
		}
		return args.stream.Send(frame)
	}
}

type scanRelay struct {
	request  *pb.ScanRequest
	stream   grpc.ServerStreamingServer[pb.ScanResponseFrame]
	remote   *RemoteWeir
	delivery *delivery
}

func (s *Server) remoteNative(args nativeRelay) error {
	next, err := forwardContext(args.stream.Context())
	if err != nil {
		return err
	}
	if err := args.remote.enter(args.delivery); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(next)
	defer cancel()
	downstream, err := args.remote.client.Native(ctx)
	if err != nil {
		return err
	}
	// Context commits this streaming attempt in the pinned gRPC version. Never
	// retain a replay history after opening the fixed downstream stream.
	_ = downstream.Context()
	timer := time.AfterFunc(s.limits.Stall, cancel)
	err = downstream.Send(args.first)
	timer.Stop()
	if err != nil {
		return err
	}
	done := make(chan struct{})
	args.delivery.mu.Lock()
	args.delivery.nativePump = done
	args.delivery.mu.Unlock()
	go func() {
		defer close(done)
		var bytes int
		for {
			args.delivery.nativeRead(true)
			frame, err := args.stream.Recv()
			args.delivery.nativeRead(false)
			if errors.Is(err, io.EOF) {
				_ = downstream.CloseSend()
				return
			}
			if err != nil {
				cancel()
				return
			}
			chunk, ok := frame.Frame.(*pb.NativeRequestFrame_Chunk)
			if !ok || len(chunk.Chunk) == 0 || len(chunk.Chunk) > protocol.NativeChunk || bytes+len(chunk.Chunk) > 8<<20 {
				cancel()
				return
			}
			bytes += len(chunk.Chunk)
			timer := time.AfterFunc(s.limits.Stall, cancel)
			err = downstream.Send(frame)
			timer.Stop()
			// Send EOF can mean a complete early response. Recv alone owns its status.
			if err != nil {
				return
			}
		}
	}()
	var head bool
	var bytes int
	for {
		timer := time.AfterFunc(s.limits.Stall, cancel)
		frame, err := downstream.Recv()
		timer.Stop()
		if err != nil {
			return incomplete(err, "missing Native End")
		}
		if end := frame.GetEnd(); end != nil {
			if end.Completion == pb.NativeCompletion_RESPONSE_COMPLETE && (!head || end.Failure != nil) || end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE && (end.Failure == nil || end.Completion != pb.NativeCompletion_NATIVE_NOT_STARTED && end.Completion != pb.NativeCompletion_RESPONSE_INCOMPLETE) {
				return status.Error(codes.Internal, "invalid peer Native completion")
			}
			timer = time.AfterFunc(s.limits.Stall, cancel)
			_, err = downstream.Recv()
			timer.Stop()
			if !errors.Is(err, io.EOF) {
				return incomplete(err, "Native frames after End")
			}
			return args.stream.Send(frame)
		}
		if frame.GetHead() != nil {
			if head {
				return status.Error(codes.Internal, "duplicate Native Head")
			}
			head = true
		} else {
			chunk, ok := frame.Frame.(*pb.NativeResponseFrame_Chunk)
			if !ok || !head || len(chunk.Chunk) == 0 || len(chunk.Chunk) > protocol.NativeChunk || bytes+len(chunk.Chunk) > 8<<20 {
				return status.Error(codes.Internal, "invalid peer Native chunk")
			}
			bytes += len(chunk.Chunk)
		}
		if err := args.stream.Send(frame); err != nil {
			return err
		}
	}
}

type nativeRelay struct {
	first    *pb.NativeRequestFrame
	stream   grpc.BidiStreamingServer[pb.NativeRequestFrame, pb.NativeResponseFrame]
	remote   *RemoteWeir
	delivery *delivery
}

func incomplete(err error, message string) error {
	if err == nil || errors.Is(err, io.EOF) {
		return status.Error(codes.Internal, message)
	}
	return err
}
