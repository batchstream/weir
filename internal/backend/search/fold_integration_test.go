//go:build integration

package search

import (
	"net/http"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestSearchFoldedMutationsOfOneRecordApplyAsOneVisibleWrite(t *testing.T) {
	adapter, backend := setupSearch(t)
	status, raw := backend.Do(t, "PUT", "/"+backend.Index+"/_settings", `{"index":{"refresh_interval":"100ms"}}`)
	if status != http.StatusOK {
		t.Fatal("fixture refresh interval", status, string(raw))
	}
	status, raw = backend.Do(t, "PUT", "/"+backend.Index+"/_doc/seller?refresh=true", `{"n":1}`)
	if status != http.StatusCreated {
		t.Fatal("fixture document", status, string(raw))
	}
	seller := recordMutations{t: t, adapter: adapter, resource: searchResource(backend.Index, "seller"), visible: true}
	other := recordMutations{t: t, adapter: adapter, resource: searchResource(backend.Index, "other"), visible: true}
	fresh := recordMutations{t: t, adapter: adapter, resource: searchResource(backend.Index, "fresh"), visible: true}
	precondition := expected{outcome: pb.MutationOutcome_NOT_APPLIED, code: pb.FailureCode_PRECONDITION_FAILED}

	works := []*execution.Plan{
		seller.prepare("add", `{"by":1}`),
		other.prepare("put", `{"n":7}`),
		seller.prepare("add", `{"by":10}`),
		seller.prepare("add", `{"by":100}`),
	}
	want := []expected{appliedResult, appliedResult, appliedResult, appliedResult}
	checkResults(t, adapter.ExecuteRecords(t.Context(), works), want)
	sellerState := searchVisibilityState{id: "seller", n: 112}
	assertVisibleSearchState(t, backend, sellerState)
	otherState := searchVisibilityState{id: "other", n: 7}
	assertVisibleSearchState(t, backend, otherState)

	works = []*execution.Plan{
		fresh.prepare("create", `{"n":1}`),
		fresh.prepare("create", `{"n":2}`),
		fresh.prepare("replace", `{"n":3}`),
		fresh.prepare("delete", ``),
		fresh.prepare("replace", `{"n":4}`),
		fresh.prepare("put", `{"n":5}`),
		fresh.prepare("add", `{"by":4}`),
	}
	want = []expected{appliedResult, precondition, appliedResult, appliedResult, precondition, appliedResult, appliedResult}
	checkResults(t, adapter.ExecuteRecords(t.Context(), works), want)
	freshState := searchVisibilityState{id: "fresh", n: 9}
	assertVisibleSearchState(t, backend, freshState)

	works = []*execution.Plan{
		seller.prepare("add", `{"by":1}`),
		seller.prepare("delete", ``),
	}
	want = []expected{appliedResult, appliedResult}
	checkResults(t, adapter.ExecuteRecords(t.Context(), works), want)
	deleted := searchVisibilityState{id: "seller", missing: true}
	assertVisibleSearchState(t, backend, deleted)
}
