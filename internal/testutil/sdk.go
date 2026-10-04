package testutil

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func ExecuteRecord(ctx context.Context, client pb.StoreServiceClient, fixture RecordFixture) (*pb.Event, error) {
	responses, err := recordResponses(ctx, client, fixture.StoreName, []*pb.Command{fixture.Command})
	if err != nil {
		return nil, err
	}
	return responses[0].Event, nil
}

// ReadRecords sends individual requests for raw protocol tests.
func ReadRecords(ctx context.Context, client pb.StoreServiceClient, store string, requests []*pb.ReadRequest) ([]*pb.ReadResult, error) {
	commands := make([]*pb.Command, len(requests))
	for i, read := range requests {
		operation := &pb.Command_Read{Read: read}
		commands[i] = &pb.Command{Operation: operation}
	}
	responses, err := recordResponses(ctx, client, store, commands)
	if err != nil {
		return nil, err
	}
	results := make([]*pb.ReadResult, len(responses))
	for i, response := range responses {
		results[i] = response.Event.GetReadResult()
		if results[i] == nil {
			return nil, errors.New("unexpected record event kind")
		}
	}
	return results, nil
}

func MutateRecords(ctx context.Context, client pb.StoreServiceClient, store string, requests []*pb.MutateRequest) ([]*pb.MutationResult, error) {
	commands := make([]*pb.Command, len(requests))
	for i, mutation := range requests {
		operation := &pb.Command_Mutate{Mutate: mutation}
		commands[i] = &pb.Command{Operation: operation}
	}
	responses, err := recordResponses(ctx, client, store, commands)
	if err != nil {
		return nil, err
	}
	results := make([]*pb.MutationResult, len(responses))
	for i, response := range responses {
		results[i] = response.Event.GetMutationResult()
		if results[i] == nil {
			return nil, errors.New("unexpected record event kind")
		}
	}
	return results, nil
}

func recordResponses(ctx context.Context, client pb.StoreServiceClient, store string, commands []*pb.Command) ([]*pb.ExecuteResponse, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Execute(ctx)
	if err != nil {
		return nil, err
	}
	sent := make(chan error, 1)
	go func() {
		for index, command := range commands {
			request := &pb.ExecuteRequest{StoreName: store, Index: uint64(index + 1), Command: command}
			if err := stream.Send(request); err != nil {
				sent <- err
				return
			}
		}
		sent <- stream.CloseSend()
	}()
	var responses []*pb.ExecuteResponse
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			if err := <-sent; err != nil {
				return nil, err
			}
			if len(responses) != len(commands) {
				return nil, fmt.Errorf("received %d record results, expected %d", len(responses), len(commands))
			}
			return responses, nil
		}
		if err != nil {
			return nil, err
		}
		if err := protocol.ValidateExecuteResponse(response); err != nil {
			return nil, err
		}
		if response.Index != uint64(len(responses)+1) {
			return nil, errors.New("nonconsecutive record result index")
		}
		responses = append(responses, response)
	}
}
