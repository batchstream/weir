package testutil

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"google.golang.org/protobuf/proto"
)

func ExecuteRecord(ctx context.Context, client pb.StoreServiceClient, fixture RecordFixture) (*execution.Result, error) {
	if fixture.Operation == nil {
		return nil, errors.New("missing record fixture")
	}
	result := &execution.Result{Index: 1}
	if read := fixture.Operation.Read; read != nil {
		results, err := ReadRecords(ctx, client, fixture.StoreName, []*pb.ReadRequest{read})
		if err != nil {
			return nil, err
		}
		if len(results) != 1 {
			return nil, errors.New("missing read fixture result")
		}
		result.Read = results[0]
	} else {
		results, err := MutateRecords(ctx, client, fixture.StoreName, []*pb.MutateRequest{fixture.Operation.Mutate})
		if err != nil {
			return nil, err
		}
		if len(results) != 1 {
			return nil, errors.New("missing mutation fixture result")
		}
		result.Mutation = results[0]
	}
	return result, nil
}

// ReadRecords collects fixture results while sending bounded wire frames.
// Production callers use the SDK; this helper keeps raw protocol tests concise.
func ReadRecords(ctx context.Context, client pb.StoreServiceClient, store string, requests []*pb.ReadRequest) ([]*pb.ReadResult, error) {
	read := &pb.ReadBatch{Requests: requests}
	operation := &pb.Command_Read{Read: read}
	command := &pb.Command{Operation: operation}
	request := &pb.ExecuteRequest{StoreName: store, Index: 1, Command: command}
	responses, err := recordResponses(ctx, client, request)
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
	mutations := &pb.MutationBatch{Requests: requests}
	operation := &pb.Command_Mutate{Mutate: mutations}
	command := &pb.Command{Operation: operation}
	request := &pb.ExecuteRequest{StoreName: store, Index: 1, Command: command}
	responses, err := recordResponses(ctx, client, request)
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

func recordResponses(ctx context.Context, client pb.StoreServiceClient, input *pb.ExecuteRequest) ([]*pb.ExecuteResponse, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Execute(ctx)
	if err != nil {
		return nil, err
	}
	count := len(input.Command.GetRead().GetRequests())
	if input.Command.GetMutate() != nil {
		count = len(input.Command.GetMutate().Requests)
	}
	sent := make(chan error, 1)
	go func() {
		for start := 0; start < count; {
			end := min(start+protocol.MaxRecordFrameItems, count)
			command := &pb.Command{}
			for {
				if reads := input.Command.GetRead(); reads != nil {
					batch := &pb.ReadBatch{Requests: reads.Requests[start:end]}
					command.Operation = &pb.Command_Read{Read: batch}
				} else {
					batch := &pb.MutationBatch{Requests: input.Command.GetMutate().Requests[start:end]}
					command.Operation = &pb.Command_Mutate{Mutate: batch}
				}
				if proto.Size(command) <= protocol.MaxRecordFrameBytes || end == start+1 {
					break
				}
				end = start + (end-start)/2
			}
			request := &pb.ExecuteRequest{StoreName: input.StoreName, Index: uint64(start + 1), Command: command}
			if err := stream.Send(request); err != nil {
				sent <- err
				return
			}
			start = end
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
			if len(responses) != count {
				return nil, fmt.Errorf("received %d record results, expected %d", len(responses), count)
			}
			return responses, nil
		}
		if err != nil {
			return nil, err
		}
		if response.Index != uint64(len(responses)+1) {
			return nil, errors.New("nonconsecutive record result index")
		}
		responses = append(responses, response)
	}
}
