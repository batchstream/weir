package search

import (
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
)

func TestLuaUnchangedDocumentSkipsSearchWrite(t *testing.T) {
	cases := []struct {
		name, current, source string
		found, unchanged      bool
	}{
		{name: "current", current: `{"n":1,"nested":{"text":"seller","values":[true,null,2]}}`, source: `return function(current) return current end`, found: true, unchanged: true},
		{name: "source whitespace", current: "{ \"n\": 1, \"text\": \"seller\" }", source: `return function(current) return current end`, found: true, unchanged: true},
		{name: "same assigned value", current: `{"n":1}`, source: `return function(current) current.n = current.n; return current end`, found: true, unchanged: true},
		{name: "different value", current: `{"n":1}`, source: `return function(current) current.n = 2; return current end`, found: true},
		{name: "new field", current: `{"n":1}`, source: `return function(current) current.text = "new"; return current end`, found: true},
		{name: "removed field", current: `{"n":1}`, source: `return function(current) current.n = nil; return current end`, found: true},
		{name: "missing empty object", source: `return function() return {} end`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			program := &luaengine.Program{Source: tc.source}
			native := &plan{program: program}
			found := tc.found
			current := &getReply{Found: &found, Source: []byte(tc.current)}
			adapter := &Adapter{}
			work := &execution.Plan{Backend: native}
			write, result := adapter.evaluateProgram(t.Context(), work, current)
			if tc.unchanged {
				if write != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
					t.Fatal("unchanged ordinary document was not a successful no-op", write, result)
				}
			} else if write == nil || result != nil {
				t.Fatal("changed or missing document lost its write", write, result)
			}
		})
	}
}

func TestLuaUnchangedDocumentRetainsSearchVisibilityBarrier(t *testing.T) {
	for _, visible := range []bool{false, true} {
		fixture := &visibilityFixture{}
		adapter := visibilityAdapter(t, fixture)
		opts := visibilityPlanOptions{action: "program", id: "unchanged", wait: visible, program: `return function(current) return current end`}
		work := visibilityPlan(t, adapter, opts)
		works := []*execution.Plan{work}
		results := adapter.executeRecords(t.Context(), works)
		if len(results) != 1 || results[0].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || results[0].GetMutationResult().GetFailure() != nil {
			t.Fatal("unchanged document acknowledgement", results)
		}
		fixture.mu.Lock()
		writes, refreshes := len(fixture.refreshModes), fixture.refreshes
		fixture.mu.Unlock()
		wantedRefreshes := 0
		if visible {
			wantedRefreshes = 1
		}
		if writes != 0 || refreshes != wantedRefreshes {
			t.Fatal("no-op wrote the document or changed visibility semantics", writes, refreshes, visible)
		}
	}
}
