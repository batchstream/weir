package testutil

import (
	"context"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
)

// Events exposes the typed values of one single-command Execute stream.
type Events struct {
	stream grpc.BidiStreamingClient[pb.ExecuteRequest, pb.ExecuteResponse]
}

func ExecuteEvents(ctx context.Context, client pb.StoreServiceClient, store string, command *pb.Command) (*Events, error) {
	request := &pb.ExecuteRequest{StoreName: store, Index: 1, Command: command}
	stream, err := client.Execute(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(request); err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	events := &Events{stream: stream}
	return events, nil
}

func (events *Events) Recv() (*pb.Event, error) {
	response, err := events.stream.Recv()
	if err != nil {
		return nil, err
	}
	return response.Event, nil
}
