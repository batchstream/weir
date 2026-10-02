//go:build integration

package main

import (
	"context"
	"fmt"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/weirclient"
	"io"
)

func smoke(ctx context.Context, client pb.StoreServiceClient, id string) error {
	next := 0
	opts := weirclient.Options{StoreName: "records"}
	opts.Produce = func(context.Context) (*pb.Call, error) {
		if next == 3 {
			return nil, io.EOF
		}
		next++
		request := put(id)
		if next == 2 {
			read := &pb.ReadRequest{Resource: request.Resource}
			fixture := testutil.RecordCall(read)
			return fixture.Call, nil
		}
		if next == 3 {
			request = put(id + "-batch")
		}
		fixture := testutil.RecordCall(request)
		return fixture.Call, nil
	}
	opts.Consume = func(_ context.Context, requestID uint64, event *pb.Event) error {
		result := event.GetResult()
		if requestID == 2 {
			if result.GetRead().GetFailure() != nil || result.GetRead().GetDocument() == nil {
				return fmt.Errorf("read: %v", result)
			}
		} else if result.GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED || result.GetMutation().GetFailure() != nil {
			return fmt.Errorf("write: %v", result)
		}
		fmt.Printf("operation request=%d complete=%v\n", requestID, result)
		return nil
	}
	if err := weirclient.Execute(ctx, client, opts); err != nil {
		return err
	}
	if err := persisted(ctx, id); err != nil {
		return err
	}
	if err := persisted(ctx, id+"-batch"); err != nil {
		return err
	}
	fmt.Println("Service DNS Route: 3 results, request ends and final OK")
	return nil
}
