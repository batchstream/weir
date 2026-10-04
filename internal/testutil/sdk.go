package testutil

import (
	"context"
	"errors"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func ExecuteRecord(ctx context.Context, client pb.StoreServiceClient, fixture RecordFixture) (*execution.Result, error) {
	if fixture.Operation == nil {
		return nil, errors.New("missing record fixture")
	}
	result := &execution.Result{Index: 1}
	if read := fixture.Operation.Read; read != nil {
		request := &pb.ReadBatchRequest{StoreName: fixture.StoreName, Requests: []*pb.ReadRequest{read}}
		response, err := client.Read(ctx, request)
		if err != nil {
			return nil, err
		}
		if len(response.Results) != 1 {
			return nil, errors.New("missing read fixture result")
		}
		result.Read = response.Results[0]
	} else {
		request := &pb.MutateBatchRequest{StoreName: fixture.StoreName, Requests: []*pb.MutateRequest{fixture.Operation.Mutate}}
		response, err := client.Mutate(ctx, request)
		if err != nil {
			return nil, err
		}
		if len(response.Results) != 1 {
			return nil, errors.New("missing mutation fixture result")
		}
		result.Mutation = response.Results[0]
	}
	return result, nil
}
