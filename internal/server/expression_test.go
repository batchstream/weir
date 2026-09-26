package server

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
)

func TestExpressionValidationBeforeExecution(t *testing.T) {
	for _, hops := range []int{0, 1, 2} {
		for _, mode := range []string{"empty", "oversize", "program"} {
			t.Run(fmt.Sprintf("%d/%s", hops, mode), func(t *testing.T) {
				f := newChain(t, hops)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				doc := &pb.Document{MediaType: "application/bson"}
				if mode == "oversize" {
					doc.Data = make([]byte, protocol.MaxExpression+1)
				}
				form := &pb.Transform_BackendExpression{BackendExpression: doc}
				transform := &pb.Transform{Form: form}
				want := pb.FailureCode_INVALID_ARGUMENT
				if mode == "program" {
					program := &pb.ProgramTransform{}
					transform.Form = &pb.Transform_Program{Program: program}
					want = pb.FailureCode_UNSUPPORTED
				}
				action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
				request := &pb.MutateRequest{Resource: testRequest().Resource, Action: action}
				result, err := f.client.Mutate(ctx, request)
				if err != nil || result.GetOutcome() != pb.MutationOutcome_NOT_STARTED || result.GetFailure().GetCode() != want {
					t.Fatal(result, err)
				}
				stream, err := f.client.Bulk(ctx)
				if err != nil {
					t.Fatal(err)
				}
				open := &pb.BulkOpen{Store: "weir://records"}
				of := &pb.BulkRequestFrame_Open{Open: open}
				frame := &pb.BulkRequestFrame{Frame: of}
				if err := stream.Send(frame); err != nil {
					t.Fatal(err)
				}
				variant := &pb.BulkOperation_Mutate{Mutate: request}
				op := &pb.BulkOperation{Operation: variant}
				body := &pb.BulkRequestFrame_Operation{Operation: op}
				frame = &pb.BulkRequestFrame{Frame: body}
				if err := stream.Send(frame); err != nil {
					t.Fatal(err)
				}
				if err := stream.CloseSend(); err != nil {
					t.Fatal(err)
				}
				reply, err := stream.Recv()
				mutation := reply.GetResult().GetMutation()
				if err != nil || mutation.GetOutcome() != pb.MutationOutcome_NOT_STARTED || mutation.GetFailure().GetCode() != want {
					t.Fatal(reply, err)
				}
				reply, err = stream.Recv()
				if err != nil || reply.GetEnd().GetReceivedCount() != 1 || reply.GetEnd().GetResultCount() != 1 {
					t.Fatal(reply, err)
				}
				if _, err := stream.Recv(); err != io.EOF {
					t.Fatal(err)
				}
				if f.adapter.commands.Load() != 0 {
					t.Fatal("invalid/unsupported expression executed")
				}
			})
		}
	}
}
