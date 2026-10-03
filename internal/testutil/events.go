package testutil

import (
	"context"
	"errors"
	"io"
	"strings"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Events holds a single typed Scan or Native server stream.
type Events struct {
	ctx    context.Context
	client pb.StoreServiceClient
	store  string
	stream grpc.ServerStreamingClient[pb.ExecuteResponse]
}

func OpenEvents(ctx context.Context, client pb.StoreServiceClient, store string) *Events {
	events := &Events{ctx: ctx, client: client, store: store}
	return events
}

func (events *Events) Send(command *pb.Command) error {
	if events.stream != nil {
		return errors.New("stream fixture accepts one command")
	}
	request := &pb.ExecuteRequest{StoreName: events.store, Command: command}
	stream, err := events.client.Execute(events.ctx, request)
	if err != nil {
		return err
	}
	events.stream = stream
	return nil
}

func (*Events) CloseSend() error { return nil }

func (events *Events) Recv() (*pb.Event, error) {
	if events.stream == nil {
		return nil, io.EOF
	}
	response, err := events.stream.Recv()
	if err != nil {
		return nil, err
	}
	return response.Event, nil
}

func FixtureCommand(command *pb.Command) (string, *pb.Command) {
	command = proto.Clone(command).(*pb.Command)
	var resource *string
	switch value := command.Operation.(type) {
	case *pb.Command_Scan:
		resource = &value.Scan.Resource
	case *pb.Command_Native:
		resource = &value.Native.Open.Resource
	}
	store, target, _ := strings.Cut(strings.TrimPrefix(*resource, "weir://"), "/")
	*resource = target
	return store, command
}

func OneEvents(ctx context.Context, client pb.StoreServiceClient, command *pb.Command) (*Events, error) {
	store, command := FixtureCommand(command)
	events := OpenEvents(ctx, client, store)
	if err := events.Send(command); err != nil {
		return nil, err
	}
	return events, nil
}
