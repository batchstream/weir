//go:build integration

package search

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestSearchSingleIndexAliasMultiplePrimaryShards(t *testing.T) {
	adapter, backend := setupSearch(t)
	left, right := backend.Index+"_left", backend.Index+"_right"
	alias := backend.Index + "_alias"
	for _, index := range []string{left, right} {
		backend.Create(t, index, `{"settings":{"number_of_shards":3,"number_of_replicas":0},"mappings":{"properties":{"n":{"type":"long"}}}}`)
	}
	for position, index := range []string{left, right} {
		body := fmt.Sprintf(`{"id":"item","n":%d}`, position*20+1)
		status, raw := backend.Do(t, "PUT", "/"+index+"/_doc/item?refresh=false", body)
		if status != 201 {
			t.Fatal("seed alias target", status, string(raw))
		}
	}
	setAlias := func(previous, next string) {
		var actions []map[string]map[string]string
		if previous != "" {
			remove := map[string]map[string]string{"remove": {"index": previous, "alias": alias}}
			actions = append(actions, remove)
		}
		add := map[string]map[string]string{"add": {"index": next, "alias": alias}}
		actions = append(actions, add)
		request := map[string]any{"actions": actions}
		body, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		status, raw := backend.Do(t, "POST", "/_aliases", string(body))
		if status != 200 {
			t.Fatal("atomic alias switch", status, string(raw))
		}
	}
	setAlias("", left)
	mutate := func(source string) {
		options := visibilityPlanOptions{action: "program", resource: alias + "/s:item", wait: true, program: source}
		work := visibilityPlan(t, adapter, options)
		works := []*execution.Plan{work}
		result := adapter.executeRecords(t.Context(), works)[0].GetMutationResult()
		if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.Failure != nil {
			t.Fatal("multi-shard alias mutation", result)
		}
	}
	assertVisible := func(want int) {
		status, raw := backend.Do(t, "POST", "/"+alias+"/_search", `{"query":{"ids":{"values":["item"]}}}`)
		var result struct {
			Hits struct {
				Hits []struct {
					Source struct{ N int } `json:"_source"`
				}
			}
		}
		if status != 200 || json.Unmarshal(raw, &result) != nil || len(result.Hits.Hits) != 1 || result.Hits.Hits[0].Source.N != want {
			t.Fatal("independent alias query cannot observe acknowledged mutation", status, want, string(raw))
		}
	}
	increment := `return function(current, incoming) current.n = current.n + 1; return current end`
	mutate(increment)
	assertVisible(2)
	setAlias(left, right)
	mutate(increment)
	assertVisible(22)
	status, raw := backend.Do(t, "PUT", "/"+right+"/_doc/item?refresh=false", `{"id":"item","n":42}`)
	if status != 200 {
		t.Fatal("seed invisible no-op state", status, string(raw))
	}
	mutate(`return function(current, incoming) return weir.keep() end`)
	assertVisible(42)
	read := batchTestPlan(t, adapter, "read", alias+"/s:item")
	reads := []*execution.Plan{read}
	reply := adapter.executeRecords(t.Context(), reads)[0].GetReadResult()
	if reply.GetFailure() != nil || !strings.Contains(string(reply.GetDocument().GetData()), `"n":42`) {
		t.Fatal("alias read did not follow current target", reply)
	}
}
