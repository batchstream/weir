//go:build integration

package search

import (
	"encoding/json"
	"net/http"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testsearch"
)

type searchVisibilityState struct {
	id      string
	n       int
	missing bool
}

func assertVisibleSearchState(t *testing.T, backend *testsearch.Backend, state searchVisibilityState) {
	t.Helper()
	query := map[string]any{"query": map[string]any{"ids": map[string]any{"values": []string{state.id}}}}
	body, _ := json.Marshal(query)
	status, raw := backend.Do(t, "POST", "/"+backend.Index+"/_search", string(body))
	var reply struct {
		Hits struct {
			Hits []struct {
				ID     string          `json:"_id"`
				Source struct{ N int } `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if status != http.StatusOK || json.Unmarshal(raw, &reply) != nil {
		t.Fatal("independent search failed", status, string(raw))
	}
	hits := reply.Hits.Hits
	if state.missing {
		if len(hits) != 0 {
			t.Fatal("acknowledged deletion remains search visible", state.id, string(raw))
		}
		return
	}
	if len(hits) != 1 || hits[0].ID != state.id || hits[0].Source.N != state.n {
		t.Fatal("acknowledged mutation is not immediately searchable", state.id, state.n, string(raw))
	}
}

func TestSearchVisibleMutationsReturnOnlyAfterExactSearchState(t *testing.T) {
	adapter, backend := setupSearch(t)
	status, raw := backend.Do(t, "PUT", "/"+backend.Index+"/_settings", `{"index":{"refresh_interval":"100ms"}}`)
	if status != http.StatusOK {
		t.Fatal("fixture refresh interval", status, string(raw))
	}
	for _, action := range []string{"put", "create", "replace", "delete", "expression", "program"} {
		t.Run(action, func(t *testing.T) {
			if action != "put" && action != "create" {
				status, raw := backend.Do(t, "PUT", "/"+backend.Index+"/_doc/"+action+"?refresh=true", `{"n":1}`)
				if status != http.StatusCreated {
					t.Fatal("fixture document", status, string(raw))
				}
			}
			opts := visibilityPlanOptions{action: action, resource: searchResource(backend.Index, action), wait: true}
			if action == "program" {
				opts.program = `return function(current, incoming) current.n = current.n + 1; return current end`
			}
			work := visibilityPlan(t, adapter, opts)
			assertOutcome(t, runSearch(t, adapter, work), pb.MutationOutcome_APPLIED, 0)
			state := searchVisibilityState{id: action, n: 2, missing: action == "delete"}
			assertVisibleSearchState(t, backend, state)
		})
	}
	// A Lua create must propagate the option through its generated conditional write.
	opts := visibilityPlanOptions{action: "program", resource: searchResource(backend.Index, "lua_create"), wait: true, program: `return function(current, incoming) return {n = 2} end`}
	work := visibilityPlan(t, adapter, opts)
	assertOutcome(t, runSearch(t, adapter, work), pb.MutationOutcome_APPLIED, 0)
	state := searchVisibilityState{id: "lua_create", n: 2}
	assertVisibleSearchState(t, backend, state)
	status, raw = backend.Do(t, "PUT", "/"+backend.Index+"/_doc/lua_delete?refresh=true", `{"n":1}`)
	if status != http.StatusCreated {
		t.Fatal("fixture Lua delete document", status, string(raw))
	}
	opts.resource = searchResource(backend.Index, "lua_delete")
	opts.program = `return function(current, incoming) return weir.delete() end`
	work = visibilityPlan(t, adapter, opts)
	assertOutcome(t, runSearch(t, adapter, work), pb.MutationOutcome_APPLIED, 0)
	state = searchVisibilityState{id: "lua_delete", missing: true}
	assertVisibleSearchState(t, backend, state)
}

func TestSearchVisibleNoopsRefreshPreviouslyStaleSearchState(t *testing.T) {
	for _, action := range []string{"lua_keep", "lua_delete_missing", "delete_missing", "expression_noop"} {
		t.Run(action, func(t *testing.T) {
			adapter, backend := setupSearch(t)
			status, raw := backend.Do(t, "PUT", "/"+backend.Index+"/_settings", `{"index":{"refresh_interval":"-1"}}`)
			if status != http.StatusOK {
				t.Fatal("fixture refresh interval", status, string(raw))
			}
			status, raw = backend.Do(t, "PUT", "/"+backend.Index+"/_doc/"+action+"?refresh=true", `{"n":1}`)
			if status != http.StatusCreated {
				t.Fatal("fixture visible old document", status, string(raw))
			}
			missing := action == "lua_delete_missing" || action == "delete_missing"
			method, body := "PUT", `{"n":2}`
			if missing {
				method, body = "DELETE", ""
			}
			status, raw = backend.Do(t, method, "/"+backend.Index+"/_doc/"+action+"?refresh=false", body)
			if status != http.StatusOK {
				t.Fatal("fixture unrefreshed state", status, string(raw))
			}
			old := searchVisibilityState{id: action, n: 1}
			assertVisibleSearchState(t, backend, old)
			opts := visibilityPlanOptions{action: "program", resource: searchResource(backend.Index, action), wait: true}
			switch action {
			case "lua_keep":
				opts.program = `return function(current, incoming) return weir.keep() end`
			case "lua_delete_missing":
				opts.program = `return function(current, incoming) return weir.delete() end`
			case "delete_missing":
				opts.action = "delete"
			case "expression_noop":
				opts.action = "expression"
			}
			if opts.action != "program" {
				// A bulk delete may write a tombstone even when the document is
				// already absent; keep its wait_for completion bounded by refreshes.
				status, raw = backend.Do(t, "PUT", "/"+backend.Index+"/_settings", `{"index":{"refresh_interval":"100ms"}}`)
				if status != http.StatusOK {
					t.Fatal("fixture refresh interval", status, string(raw))
				}
			}
			work := visibilityPlan(t, adapter, opts)
			assertOutcome(t, runSearch(t, adapter, work), pb.MutationOutcome_APPLIED, 0)
			state := searchVisibilityState{id: action, n: 2, missing: missing}
			assertVisibleSearchState(t, backend, state)
		})
	}
}
