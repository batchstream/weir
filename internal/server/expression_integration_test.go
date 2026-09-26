//go:build integration

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/mongostore"
	"github.com/batchstream/weir/internal/searchstore"
	"github.com/batchstream/weir/internal/testmetrics"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func realExpression(t *testing.T, f scanFixture, n int) *pb.MutateRequest {
	t.Helper()
	media := searchstore.ExpressionMedia
	raw := []byte(fmt.Sprintf(`{"doc":{"n":%d}}`, n))
	if f.backend == nil {
		media = mongostore.ExpressionMedia
		doc := bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: n}}}}
		var err error
		raw, err = bson.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	d := &pb.Document{MediaType: media, Data: raw}
	form := &pb.Transform_BackendExpression{BackendExpression: d}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	req := &pb.MutateRequest{Resource: f.root + "/s:counter", Action: action}
	return req
}
func expressionReadNumber(t *testing.T, f scanFixture, d *pb.Document) int64 {
	t.Helper()
	if d == nil {
		t.Fatal("missing document")
	}
	if f.backend == nil {
		return bson.Raw(d.Data).Lookup("n").AsInt64()
	}
	var doc struct{ N int64 }
	if json.Unmarshal(d.Data, &doc) != nil {
		t.Fatal("read JSON")
	}
	return doc.N
}

func TestPublicExpressionUnaryBulkAndOpaquePeers(t *testing.T) {
	for _, kind := range []string{"mongo", "search"} {
		t.Run(kind, func(t *testing.T) {
			for _, hops := range []int{0, 2} {
				t.Run(fmt.Sprint(hops), func(t *testing.T) {
					f := scanServer(t, kind, DefaultLimits())
					if hops > 0 {
						f = forwardFixture(t, f, hops)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					p := realMutation(t, f, "counter", 0)
					if r, e := f.client.Mutate(ctx, p); e != nil || r.GetOutcome() != pb.MutationOutcome_APPLIED {
						t.Fatal(r, e)
					}
					request := realExpression(t, f, 1)
					if r, e := f.client.Mutate(ctx, request); e != nil || r.GetOutcome() != pb.MutationOutcome_APPLIED {
						t.Fatal(r, e)
					}
					before := testmetrics.Sum(testmetrics.Gather(t, f.runtime), "weir_store_executions_total")
					invalid := realExpression(t, f, 2)
					invalid.GetAtomicTransform().GetBackendExpression().Data = []byte("opaque malformed backend bytes")
					r, e := f.client.Mutate(ctx, invalid)
					if e != nil || r.GetOutcome() != pb.MutationOutcome_NOT_STARTED || r.GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT {
						t.Fatal(r, e)
					}
					if testmetrics.Sum(testmetrics.Gather(t, f.runtime), "weir_store_executions_total") != before {
						t.Fatal("prevalidation executed")
					}
					stream, err := f.client.Bulk(ctx)
					if err != nil {
						t.Fatal(err)
					}
					name, _, _ := protocolResource(f.root)
					open := &pb.BulkOpen{Store: "weir://" + name}
					of := &pb.BulkRequestFrame_Open{Open: open}
					frame := &pb.BulkRequestFrame{Frame: of}
					if err := stream.Send(frame); err != nil {
						t.Fatal(err)
					}
					sent := make(chan error, 1)
					go func() {
						for i := 0; i < 8; i++ {
							op := &pb.BulkOperation{Index: uint64(i)}
							if i%2 == 0 {
								mutation := realExpression(t, f, i+2)
								if i == 4 {
									mutation.Resource = f.root + "/s:absent"
								}
								op.Operation = &pb.BulkOperation_Mutate{Mutate: mutation}
							} else {
								read := &pb.ReadRequest{Resource: request.Resource}
								op.Operation = &pb.BulkOperation_Read{Read: read}
							}
							variant := &pb.BulkRequestFrame_Operation{Operation: op}
							frame := &pb.BulkRequestFrame{Frame: variant}
							if err := stream.Send(frame); err != nil {
								sent <- err
								return
							}
						}
						sent <- stream.CloseSend()
					}()
					seen := map[uint64]bool{}
					for {
						frame, err := stream.Recv()
						if err != nil {
							t.Fatal(err)
						}
						if end := frame.GetEnd(); end != nil {
							if end.ReceivedCount != 8 || end.ResultCount != 8 || len(seen) != 8 {
								t.Fatal(end)
							}
							break
						}
						r := frame.GetResult()
						if r == nil || seen[r.Index] {
							t.Fatal(frame)
						}
						seen[r.Index] = true
						if r.Index%2 == 0 {
							want := pb.MutationOutcome_APPLIED
							if r.Index == 4 {
								want = pb.MutationOutcome_NOT_APPLIED
							}
							if r.GetMutation().GetOutcome() != want {
								t.Fatal(r)
							}
						} else {
							want := int64(r.Index + 1)
							if r.Index == 5 {
								want = 4
							}
							if expressionReadNumber(t, f, r.GetRead().GetDocument()) != want {
								t.Fatal("same-key order", r)
							}
						}
					}
					if _, err := stream.Recv(); err != io.EOF {
						t.Fatal(err)
					}
					if err := <-sent; err != nil {
						t.Fatal(err)
					}
					waitScanReleased(t, f)
					families := testmetrics.Gather(t, f.runtime)
					if testmetrics.Sum(families, "weir_store_executions_total") != 10 || testmetrics.Sum(families, "weir_store_records_total") != 10 {
						t.Fatal("execution duplicated")
					}
					for _, remote := range f.metricsRemotes {
						metrics := testmetrics.Gather(t, remote)
						if testmetrics.Sum(metrics, "weir_relay_terminations_total") != 4 || metrics["weir_store_executions_total"] != nil {
							t.Fatal("relay decoded or executed")
						}
					}
				})
			}
		})
	}
}
