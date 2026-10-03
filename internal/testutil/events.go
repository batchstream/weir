package testutil

import (
	"context"
	"io"
	"strings"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/protobuf/proto"
)

// Events is a bounded typed Execute harness used by repository conformance tests.
// It retains one input Command and one output Event, not a complete batch.
type Events struct {
	ctx    context.Context
	input  chan *pb.Command
	output chan *pb.Event
	done   chan struct{}
	err    error
}

func OpenEvents(ctx context.Context, client pb.StoreServiceClient, destination string) *Events {
	stream := &Events{ctx: ctx, input: make(chan *pb.Command, 1), output: make(chan *pb.Event, 1), done: make(chan struct{})}
	opts := ExecuteOptions{StoreName: destination}
	opts.Produce = func(ctx context.Context) (*pb.Command, error) {
		select {
		case call, ok := <-stream.input:
			if !ok {
				return nil, io.EOF
			}
			return call, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	opts.Consume = func(ctx context.Context, _ uint64, event *pb.Event) error {
		select {
		case stream.output <- event:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	go func() { stream.err = Execute(ctx, client, opts); close(stream.output); close(stream.done) }()
	return stream
}

func (s *Events) Send(call *pb.Command) error {
	select {
	case s.input <- call:
		return nil
	case <-s.done:
		return s.err
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}
func (s *Events) CloseSend() error { close(s.input); return nil }
func (s *Events) Recv() (*pb.Event, error) {
	event, ok := <-s.output
	if ok {
		return event, nil
	}
	<-s.done
	if s.err != nil {
		return nil, s.err
	}
	return nil, io.EOF
}

// FixtureCommand converts backend conformance fixtures to a Store-relative Command.
func FixtureCommand(call *pb.Command) (string, *pb.Command) {
	call = proto.Clone(call).(*pb.Command)
	var resource *string
	switch v := call.Operation.(type) {
	case *pb.Command_Read:
		resource = &v.Read.Resource
	case *pb.Command_Mutate:
		resource = &v.Mutate.Resource
	case *pb.Command_Scan:
		resource = &v.Scan.Resource
	case *pb.Command_Native:
		resource = &v.Native.Open.Resource
	}
	destination, target, _ := strings.Cut(strings.TrimPrefix(*resource, "weir://"), "/")
	*resource = target
	return destination, call
}

func OneEvents(ctx context.Context, client pb.StoreServiceClient, call *pb.Command) (*Events, error) {
	destination, call := FixtureCommand(call)
	stream := OpenEvents(ctx, client, destination)
	if err := stream.Send(call); err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	return stream, nil
}

func OperationCommand(operation *pb.Operation) (string, *pb.Command) {
	call := &pb.Command{Version: 1}
	if read := operation.GetRead(); read != nil {
		variant := &pb.Command_Read{Read: read}
		call.Operation = variant
	} else {
		variant := &pb.Command_Mutate{Mutate: operation.GetMutate()}
		call.Operation = variant
	}
	return FixtureCommand(call)
}
