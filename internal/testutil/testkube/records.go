//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/batchstream/weir/api/weir/v1"
	"io"
)

func smoke(ctx context.Context, client pb.WeirClient, id string) error {
	request := put(id)
	reply, err := client.Mutate(ctx, request)
	if err != nil || reply.GetOutcome() != pb.MutationOutcome_APPLIED || reply.GetFailure() != nil {
		return fmt.Errorf("mutation: %v %v", reply, err)
	}
	fmt.Printf("operation id=%s outcome=APPLIED\n", id)
	read := &pb.ReadRequest{Resource: request.Resource}
	result, err := client.Read(ctx, read)
	if err != nil || result.GetFailure() != nil || result.GetDocument() == nil {
		return fmt.Errorf("read: %v %v", result, err)
	}
	stream, err := client.Bulk(ctx)
	if err != nil {
		return err
	}
	open := &pb.BulkOpen{Store: "weir://records"}
	opening := &pb.BulkRequestFrame_Open{Open: open}
	frame := &pb.BulkRequestFrame{Frame: opening}
	if err := stream.Send(frame); err != nil {
		return err
	}
	mutation := &pb.BulkOperation_Mutate{Mutate: put(id + "-bulk")}
	op := &pb.BulkOperation{Index: 0, Operation: mutation}
	variant := &pb.BulkRequestFrame_Operation{Operation: op}
	frame = &pb.BulkRequestFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	got, end := false, false
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if item := r.GetResult(); item != nil {
			if end || got || item.Index != 0 || item.GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
				return errors.New("bulk correlation/outcome")
			}
			got = true
			fmt.Printf("operation id=%s-bulk outcome=APPLIED\n", id)
		}
		if r.GetEnd() != nil {
			if end || !got || r.GetEnd().ReceivedCount != 1 || r.GetEnd().ResultCount != 1 {
				return errors.New("bulk End/counts")
			}
			end = true
		}
	}
	if !got || !end {
		return errors.New("bulk incomplete")
	}
	if err := persisted(ctx, id); err != nil {
		return err
	}
	if err := persisted(ctx, id+"-bulk"); err != nil {
		return err
	}
	fmt.Println("Service DNS Mutate=APPLIED Read=found Bulk index=0 APPLIED End+EOF")
	return nil
}
