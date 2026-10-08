//go:build integration

package search

import (
	"context"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testsearch"
)

func setupSearch(t *testing.T) (*Adapter, *testsearch.Backend) {
	t.Helper()
	backend := testsearch.Open(t)
	cfg := Config{Store: "search", URL: backend.URL}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	adapter, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(); _ = adapter.Close() })
	return adapter, backend
}

func reopenSearch(t *testing.T, adapter *Adapter) *Adapter {
	t.Helper()
	config := adapter.config
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reopened, err := Open(ctx, config)
	if err != nil {
		t.Fatal("reopen Store after changing target structure", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	return reopened
}

func searchResource(index, id string) string {
	return index + "/" + protocol.EncodeSegment("s:"+id)
}

func searchPlan(t *testing.T, a *Adapter, action, resource string) *execution.Plan {
	t.Helper()
	opCommand := &pb.Command{}
	op := &pb.ExecuteRequest{Index: 1, Command: opCommand}
	if action == "read" {
		read := &pb.ReadRequest{Resource: resource}
		recordOperation1 := &pb.Command_Read{Read: read}
		op.Command.Operation = recordOperation1
	} else {
		document := &pb.Document{ContentType: "application/json", Data: []byte(`{"n":9223372036854775807,"keep":"source"}`)}
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
		recordOperation2 := &pb.Command_Mutate{Mutate: mutation}
		op.Command.Operation = recordOperation2
	}
	work, failure := prepareTestRecord(a, op)
	if failure != nil {
		t.Fatal(failure)
	}
	return work
}
func runSearch(t *testing.T, a *Adapter, work *execution.Plan) *pb.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	results := a.executeRecords(ctx, []*execution.Plan{work})
	if len(results) != 1 {
		t.Fatal("result cardinality")
	}
	return results[0]
}
func assertOutcome(t *testing.T, result *pb.Event, want pb.MutationOutcome, code pb.FailureCode) {
	t.Helper()
	got := result.GetMutationResult()
	if got == nil || got.Outcome != want || got.GetFailure().GetCode() != code {
		t.Fatalf("want %v/%v got %v", want, code, result)
	}
}
