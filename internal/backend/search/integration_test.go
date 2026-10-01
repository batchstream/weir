//go:build integration

package search

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestSearchCRUD(t *testing.T) {
	a, b := setupSearch(t)
	for _, id := range []string{"plain", "a/b% space", "中文", "?query#fragment"} {
		t.Run(id, func(t *testing.T) {
			read := searchPlan(t, a, "read", searchResource(b.Index, id))
			if runSearch(t, a, read).GetRead().GetMissing() == nil {
				t.Fatal("missing read")
			}
			replace := searchPlan(t, a, "replace", searchResource(b.Index, id))
			assertOutcome(t, runSearch(t, a, replace), pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_PRECONDITION_FAILED)
			create := searchPlan(t, a, "create", searchResource(b.Index, id))
			assertOutcome(t, runSearch(t, a, create), pb.MutationOutcome_APPLIED, 0)
			assertOutcome(t, runSearch(t, a, create), pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_PRECONDITION_FAILED)
			assertOutcome(t, runSearch(t, a, replace), pb.MutationOutcome_APPLIED, 0)
			put := searchPlan(t, a, "put", searchResource(b.Index, id))
			assertOutcome(t, runSearch(t, a, put), pb.MutationOutcome_APPLIED, 0)
			result := runSearch(t, a, read).GetRead()
			if result.GetDocument() == nil || !strings.Contains(string(result.GetDocument().Data), "9223372036854775807") || strings.Contains(string(result.GetDocument().Data), "_id") {
				t.Fatal("source fidelity", result)
			}
			del := searchPlan(t, a, "delete", searchResource(b.Index, id))
			assertOutcome(t, runSearch(t, a, del), pb.MutationOutcome_APPLIED, 0)
			assertOutcome(t, runSearch(t, a, del), pb.MutationOutcome_APPLIED, 0)
			assertOutcome(t, runSearch(t, a, create), pb.MutationOutcome_APPLIED, 0)
		})
	}
	status, _ := b.Do(t, "DELETE", "/"+b.Index, "")
	if status != 200 {
		t.Fatal(status)
	}
	result := runSearch(t, a, searchPlan(t, a, "read", searchResource(b.Index, "plain")))
	if result.GetRead().GetMissing() != nil || result.GetRead().GetFailure() == nil {
		t.Fatal("index missing is not record missing", result)
	}
	assertOutcome(t, runSearch(t, a, searchPlan(t, a, "put", searchResource(b.Index, "plain"))), pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_UNSUPPORTED)
	status, _ = b.Do(t, "HEAD", "/"+b.Index, "")
	if status != 404 {
		t.Fatal("index recreated", status)
	}
}
func TestSearchMixedBulk(t *testing.T) {
	a, b := setupSearch(t)
	existing := searchPlan(t, a, "create", searchResource(b.Index, "exists"))
	assertOutcome(t, runSearch(t, a, existing), pb.MutationOutcome_APPLIED, 0)
	good := searchPlan(t, a, "put", searchResource(b.Index, "good"))
	bad := searchPlan(t, a, "put", searchResource(b.Index, "bad"))
	bad.Backend.(*plan).source = []byte(`{"n":"not a number"}`)
	missing := searchPlan(t, a, "delete", searchResource(b.Index, "absent"))
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
		read := searchPlan(t, a, "read", searchResource(b.Index, id))
		if runSearch(t, a, read).GetRead().GetDocument() == nil {
			t.Fatal("confirmed write missing")
		}
	}
	read := searchPlan(t, a, "read", searchResource(b.Index, "bad"))
	if runSearch(t, a, read).GetRead().GetMissing() == nil {
		t.Fatal("rejected write persisted")
	}
}
