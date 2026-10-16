package search

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestSingleIndexAliasPinsLuaCASAndVisibility(t *testing.T) {
	var generation, writes, refreshes atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			fmt.Fprint(w, `{"version":{"build_flavor":"default"}}`)
		case "/catalog":
			index := "left"
			if generation.Load() != 0 {
				index = "right"
			}
			fmt.Fprintf(w, `{%q:{"settings":{"index.uuid":%q,"index.number_of_shards":"3"},"aliases":{"catalog":{}},"mappings":{}}}`, index, index)
		case "/left/_mget":
			generation.Store(1)
			fmt.Fprint(w, `{"docs":[{"_index":"left","_id":"item","found":true,"_seq_no":7,"_primary_term":2,"_source":{"n":1}}]}`)
		case "/right/_mget":
			fmt.Fprint(w, `{"docs":[{"_index":"right","_id":"item","found":true,"_seq_no":17,"_primary_term":3,"_source":{"n":10}}]}`)
		case "/_bulk":
			var header map[string]struct {
				Index    string `json:"_index"`
				ID       string `json:"_id"`
				Sequence *int64 `json:"if_seq_no"`
				Term     *int64 `json:"if_primary_term"`
			}
			decoder := json.NewDecoder(r.Body)
			if err := decoder.Decode(&header); err != nil {
				t.Error(err)
				return
			}
			metadata := header["index"]
			if metadata.Index != "left" || metadata.ID != "item" || metadata.Sequence == nil || *metadata.Sequence != 7 || metadata.Term == nil || *metadata.Term != 2 || r.URL.Query().Get("refresh") != "wait_for" {
				t.Error("alias switch detached the write from its observation", metadata, r.URL.RawQuery)
			}
			var document struct{ N int }
			if err := decoder.Decode(&document); err != nil || document.N != 2 {
				t.Error("Lua did not transform the observed document", document, err)
			}
			writes.Add(1)
			fmt.Fprint(w, `{"errors":false,"took":1,"items":[{"index":{"_index":"left","_id":"item","status":200,"result":"updated","_version":2,"_seq_no":8,"_primary_term":2,"_shards":{"total":1,"successful":1,"failed":0}}}]}`)
		case "/right/_refresh":
			refreshes.Add(1)
			fmt.Fprint(w, `{"_shards":{"total":3,"successful":3,"failed":0}}`)
		default:
			t.Error("unexpected target or startup policy request", r.URL.Path)
			http.Error(w, "unexpected", http.StatusNotFound)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	config := Config{Store: "search", URL: server.URL}
	adapter, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	options := visibilityPlanOptions{action: "program", resource: "catalog/s:item", wait: true, program: `return function(current, incoming) current.n = current.n + 1; return current end`}
	work := visibilityPlan(t, adapter, options)
	works := []*execution.Plan{work}
	result := adapter.executeRecords(t.Context(), works)[0].GetMutationResult()
	if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.Failure != nil || writes.Load() != 1 || work.Backend.(*plan).index != "catalog" {
		t.Fatal("alias CAS or caller plan changed", result, writes.Load(), work.Backend.(*plan).index)
	}
	read := batchTestPlan(t, adapter, "read", "catalog/s:item")
	reads := []*execution.Plan{read}
	reply := adapter.executeRecords(t.Context(), reads)[0].GetReadResult()
	if string(reply.GetDocument().GetData()) != `{"n":10}` {
		t.Fatal("later call retained the old alias target", reply)
	}
	keepOptions := visibilityPlanOptions{action: "program", resource: "catalog/s:item", wait: true, program: `return function(current, incoming) return weir.keep() end`}
	keep := visibilityPlan(t, adapter, keepOptions)
	keeps := []*execution.Plan{keep}
	result = adapter.executeRecords(t.Context(), keeps)[0].GetMutationResult()
	if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.Failure != nil || refreshes.Load() != 1 || writes.Load() != 1 {
		t.Fatal("no-op visibility lost the resolved index", result, refreshes.Load(), writes.Load())
	}
}

func TestTargetAliasesRejectAmbiguousOrChangedSemantics(t *testing.T) {
	cases := []struct{ name, metadata string }{
		{name: "multiple", metadata: `{"left":{"settings":{"index.uuid":"l","index.number_of_shards":"3"},"aliases":{"catalog":{"is_write_index":true}}},"right":{"settings":{"index.uuid":"r","index.number_of_shards":"3"},"aliases":{"catalog":{"is_write_index":false}}}}`},
		{name: "filter", metadata: `{"left":{"settings":{"index.uuid":"l","index.number_of_shards":"3"},"aliases":{"catalog":{"filter":{"term":{"tenant":1}}}}}}`},
		{name: "routing", metadata: `{"left":{"settings":{"index.uuid":"l","index.number_of_shards":"3"},"aliases":{"catalog":{"routing":"tenant"}}}}`},
		{name: "index_routing", metadata: `{"left":{"settings":{"index.uuid":"l","index.number_of_shards":"3"},"aliases":{"catalog":{"index_routing":"tenant"}}}}`},
		{name: "search_routing", metadata: `{"left":{"settings":{"index.uuid":"l","index.number_of_shards":"3"},"aliases":{"catalog":{"search_routing":"tenant"}}}}`},
		{name: "foreign", metadata: `{"left":{"settings":{"index.uuid":"l","index.number_of_shards":"3"},"aliases":{"other":{}}}}`},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, test.metadata) })
			server := httptest.NewServer(handler)
			defer server.Close()
			config := Config{URL: server.URL}
			adapter := &Adapter{config: config, ctx: context.Background(), client: server.Client()}
			_, failure := adapter.inspect(t.Context(), "catalog", false)
			if failure.GetCode() != pb.FailureCode_UNSUPPORTED {
				t.Fatal("ambiguous alias or changed query/routing semantics accepted", failure)
			}
		})
	}
}
