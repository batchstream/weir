//go:build integration

package searchstore

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/testsearch"
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
func TestSearchCRUD(t *testing.T) {
	a, b := setupSearch(t)
	for _, id := range []string{"plain", "a/b% space", "中文", "?query#fragment"} {
		t.Run(id, func(t *testing.T) {
			read := searchPlan(t, a, "read", id)
			if runSearch(t, a, read).GetRead().GetMissing() == nil {
				t.Fatal("missing read")
			}
			replace := searchPlan(t, a, "replace", id)
			assertOutcome(t, runSearch(t, a, replace), pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_PRECONDITION_FAILED)
			create := searchPlan(t, a, "create", id)
			assertOutcome(t, runSearch(t, a, create), pb.MutationOutcome_APPLIED, 0)
			assertOutcome(t, runSearch(t, a, create), pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_PRECONDITION_FAILED)
			assertOutcome(t, runSearch(t, a, replace), pb.MutationOutcome_APPLIED, 0)
			put := searchPlan(t, a, "put", id)
			assertOutcome(t, runSearch(t, a, put), pb.MutationOutcome_APPLIED, 0)
			result := runSearch(t, a, read).GetRead()
			if result.GetDocument() == nil || !strings.Contains(string(result.GetDocument().Data), "9223372036854775807") || strings.Contains(string(result.GetDocument().Data), "_id") {
				t.Fatal("source fidelity", result)
			}
			del := searchPlan(t, a, "delete", id)
			assertOutcome(t, runSearch(t, a, del), pb.MutationOutcome_APPLIED, 0)
			assertOutcome(t, runSearch(t, a, del), pb.MutationOutcome_APPLIED, 0)
			assertOutcome(t, runSearch(t, a, create), pb.MutationOutcome_APPLIED, 0)
		})
	}
	status, _ := b.Do(t, "DELETE", "/"+b.Index, "")
	if status != 200 {
		t.Fatal(status)
	}
	result := runSearch(t, a, searchPlan(t, a, "read", "plain"))
	if result.GetRead().GetMissing() != nil || result.GetRead().GetFailure() == nil {
		t.Fatal("index missing is not record missing", result)
	}
	assertOutcome(t, runSearch(t, a, searchPlan(t, a, "put", "plain")), pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_UNSUPPORTED)
	status, _ = b.Do(t, "HEAD", "/"+b.Index, "")
	if status != 404 {
		t.Fatal("index recreated", status)
	}
}
func TestSearchMixedBulk(t *testing.T) {
	a, _ := setupSearch(t)
	existing := searchPlan(t, a, "create", "exists")
	assertOutcome(t, runSearch(t, a, existing), pb.MutationOutcome_APPLIED, 0)
	good := searchPlan(t, a, "put", "good")
	bad := searchPlan(t, a, "put", "bad")
	bad.Backend.(*plan).source = []byte(`{"n":"not a number"}`)
	missing := searchPlan(t, a, "delete", "absent")
	works := []*execution.Plan{good, existing, bad, missing}
	for i, work := range works {
		work.Operation.Index = uint64(i)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	results, feedback := a.Execute(ctx, works)
	if feedback != execution.Neutral {
		t.Fatal("business conflict classified as congestion", feedback)
	}
	assertOutcome(t, results[0], pb.MutationOutcome_APPLIED, 0)
	assertOutcome(t, results[1], pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_PRECONDITION_FAILED)
	assertOutcome(t, results[2], pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_PRECONDITION_FAILED)
	assertOutcome(t, results[3], pb.MutationOutcome_APPLIED, 0)
	for _, id := range []string{"good", "exists"} {
		if runSearch(t, a, searchPlan(t, a, "read", id)).GetRead().GetDocument() == nil {
			t.Fatal("confirmed write missing")
		}
	}
	if runSearch(t, a, searchPlan(t, a, "read", "bad")).GetRead().GetMissing() == nil {
		t.Fatal("rejected write persisted")
	}
}
