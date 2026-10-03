//go:build integration

package main

import (
	"context"
	"fmt"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
)

func smoke(ctx context.Context, client pb.StoreServiceClient, id string) error {
	fixture := testutil.RecordCommand(put(id))
	write := &pb.MutateBatchRequest{StoreName: "records", Requests: []*pb.MutateRequest{fixture.Operation.GetMutate()}}
	written, err := client.Mutate(ctx, write)
	if err != nil || len(written.GetResults()) != 1 || written.Results[0].GetOutcome() != pb.MutationOutcome_APPLIED || written.Results[0].GetFailure() != nil {
		return fmt.Errorf("write: %v %v", written, err)
	}
	read := &pb.ReadRequest{Resource: fixture.Operation.GetMutate().Resource}
	reads := &pb.ReadBatchRequest{StoreName: "records", Requests: []*pb.ReadRequest{read, read}}
	found, err := client.Read(ctx, reads)
	if err != nil || len(found.GetResults()) != len(reads.Requests) {
		return fmt.Errorf("read batch: %v %v", found, err)
	}
	for i, result := range found.Results {
		if result.GetFailure() != nil || result.GetDocument() == nil {
			return fmt.Errorf("read[%d]: %v", i, result)
		}
	}
	ids := []string{id + "-batch", id + "-batch-second"}
	requests := make([]*pb.MutateRequest, 0, len(ids))
	for _, item := range ids {
		fixture := testutil.RecordCommand(put(item))
		requests = append(requests, fixture.Operation.GetMutate())
	}
	batch := &pb.MutateBatchRequest{StoreName: "records", Requests: requests}
	reply, err := client.Mutate(ctx, batch)
	if err != nil || len(reply.GetResults()) != len(requests) {
		return fmt.Errorf("mutation batch: %v %v", reply, err)
	}
	for i, result := range reply.Results {
		if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
			return fmt.Errorf("mutation[%d]: %v", i, result)
		}
	}
	for _, item := range append([]string{id}, ids...) {
		if err := persisted(ctx, item); err != nil {
			return err
		}
	}
	fmt.Println("Service DNS: one mutation and two ordered read/mutation batches completed")
	return nil
}
