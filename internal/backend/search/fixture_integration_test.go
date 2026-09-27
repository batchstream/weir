//go:build integration

package search

import (
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/testutil/testsearch"
)

func setupSearch(t *testing.T) (*Adapter, *testsearch.Backend) {
	t.Helper()
	backend := testsearch.Open(t)
	cfg := Config{Store: "search", URL: backend.URL, Index: backend.Index, Profile: backend.Profile, Pool: 4}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	adapter, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(); _ = adapter.Close() })
	return adapter, backend
}
func searchPlan(t *testing.T, a *Adapter, action, id string) *execution.Plan {
	t.Helper()
	resource := "weir://search/" + a.config.Index + "/" + protocol.EncodeSegment("s:"+id)
	op := &pb.BulkOperation{}
	if action == "read" {
		read := &pb.ReadRequest{Resource: resource}
		op.Operation = &pb.BulkOperation_Read{Read: read}
	} else {
		document := &pb.Document{MediaType: "application/json", Data: []byte(`{"n":9223372036854775807,"keep":"source"}`)}
		mutation := &pb.MutateRequest{Resource: resource}
		switch action {
		case "put":
			mutation.Action = &pb.MutateRequest_Put{Put: document}
		case "create":
			mutation.Action = &pb.MutateRequest_Create{Create: document}
		case "replace":
			mutation.Action = &pb.MutateRequest_Replace{Replace: document}
		case "delete":
			empty := &pb.Empty{}
			mutation.Action = &pb.MutateRequest_Delete{Delete: empty}
		}
		op.Operation = &pb.BulkOperation_Mutate{Mutate: mutation}
	}
	work, failure := a.Prepare(op)
	if failure != nil {
		t.Fatal(failure)
	}
	return work
}
func runSearch(t *testing.T, a *Adapter, work *execution.Plan) *pb.BulkResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	results, _ := a.Execute(ctx, []*execution.Plan{work})
	if len(results) != 1 {
		t.Fatal("result cardinality")
	}
	return results[0]
}
func assertOutcome(t *testing.T, result *pb.BulkResult, want pb.MutationOutcome, code pb.FailureCode) {
	t.Helper()
	got := result.GetMutation()
	if got == nil || got.Outcome != want || got.GetFailure().GetCode() != code {
		t.Fatalf("want %v/%v got %v", want, code, result)
	}
}
