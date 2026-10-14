package search

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestScanAliasCheckpointRetainsMultiShardPhysicalPIT(t *testing.T) {
	var metadataRequests atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/catalog":
			metadataRequests.Add(1)
			fmt.Fprint(w, `{"left":{"settings":{"index.uuid":"l","index.number_of_shards":"3"},"aliases":{"catalog":{}}}}`)
		case "/left/_pit":
			fmt.Fprint(w, `{"id":"left-pit","_shards":{"total":3,"successful":3,"skipped":0,"failed":0}}`)
		case "/_search":
			var request struct {
				PIT   struct{ ID string }
				After []int64 `json:"search_after"`
				Sort  []string
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.PIT.ID != "left-pit" || len(request.Sort) != 1 || request.Sort[0] != "_shard_doc" {
				t.Error("PIT identity or globally unique sort lost", request, err)
			}
			rows := `{"_index":"left","_id":"a","_score":null,"_source":{"id":"a"},"sort":[0]},{"_index":"left","_id":"b","_score":null,"_source":{"id":"b"},"sort":[4294967296]}`
			if len(request.After) != 0 {
				if request.After[0] != 4294967296 {
					t.Error("checkpoint truncated the shard/document sort", request.After)
				}
				rows = ""
			}
			fmt.Fprintf(w, `{"pit_id":"left-pit","took":1,"timed_out":false,"_shards":{"total":3,"successful":3,"skipped":1,"failed":0},"hits":{"max_score":null,"hits":[%s]}}`, rows)
		case "/_pit":
			fmt.Fprint(w, `{"succeeded":true,"num_freed":3}`)
		default:
			t.Error("Scan did not pin the physical index", r.URL.Path)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	config := Config{Store: "search", URL: server.URL}
	adapter := &Adapter{dialect: ElasticsearchProduct, config: config, ctx: context.Background(), client: server.Client()}
	request := &pb.ScanRequest{Resource: "catalog", PageSize: 2}
	work, failure := adapter.prepareScan(context.Background(), request)
	if failure != nil {
		t.Fatal(failure)
	}
	page := adapter.fetchScan(t.Context(), work)
	if page.Failure != nil || len(page.Documents) != 2 || len(page.NextContinuationToken) == 0 {
		t.Fatal(page)
	}
	resumedRequest := &pb.ScanRequest{Resource: "catalog", PageSize: 2, ContinuationToken: page.NextContinuationToken}
	resumed, failure := adapter.prepareScan(context.Background(), resumedRequest)
	if failure != nil {
		t.Fatal(failure)
	}
	page = adapter.fetchScan(t.Context(), resumed)
	if page.Failure != nil || !page.Exhausted || metadataRequests.Load() != 1 || resumed.Backend.(*scanPlan).index != "left" {
		t.Fatal("resumed alias Scan was re-resolved or lost physical hit validation", page, metadataRequests.Load())
	}
	if failure := adapter.closeScan(t.Context(), work); failure != nil {
		t.Fatal("multi-shard PIT cleanup", failure)
	}
}

func TestOpenSearchScanUsesBackendSortCapability(t *testing.T) {
	var pitRequests atomic.Int32
	var searchRequests atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/records":
			fmt.Fprint(w, strings.Replace(testIndexReply, `"index.number_of_shards":"1"`, `"index.number_of_shards":"3"`, 1))
		case "/records/_mget":
			fmt.Fprint(w, `{"docs":[{"_index":"records","_id":"item","found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1}}]}`)
		case "/records/_search/point_in_time":
			pitRequests.Add(1)
			fmt.Fprint(w, `{"pit_id":"opened","creation_time":1,"_shards":{"total":3,"successful":3,"skipped":0,"failed":0}}`)
		case "/_search":
			searchRequests.Add(1)
			var request struct{ Sort []string }
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.Sort) != 1 || request.Sort[0] != "_shard_doc" {
				t.Error("Scan chose a compatibility sort", request, err)
			}
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":{"type":"search_phase_execution_exception","reason":"No mapping found for [_shard_doc] in order to sort on"},"status":400}`)
		case "/_search/point_in_time":
			fmt.Fprint(w, `{"pits":[{"pit_id":"opened","successful":true}]}`)
		default:
			t.Error("unexpected Scan request", r.URL.Path)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	config := Config{Store: "search", URL: server.URL}
	adapter := &Adapter{dialect: OpenSearchProduct, config: config, ctx: context.Background(), client: server.Client()}
	read := batchTestPlan(t, adapter, "read", "records/s:item")
	reads := []*execution.Plan{read}
	if reply := adapter.executeRecords(t.Context(), reads)[0].GetReadResult(); reply.GetFailure() != nil || reply.GetDocument() == nil {
		t.Fatal("ordinary read retained Scan-only shard restriction", reply)
	}
	request := &pb.ScanRequest{Resource: "records"}
	work, failure := adapter.prepareScan(context.Background(), request)
	if failure != nil {
		t.Fatal(failure)
	}
	page := adapter.fetchScan(t.Context(), work)
	if page.Failure == nil || page.Failure.GetCode() == pb.FailureCode_UNSUPPORTED || pitRequests.Load() != 1 || searchRequests.Load() != 1 {
		t.Fatal("backend sort rejection was replaced by a local shard gate or fallback", page, pitRequests.Load(), searchRequests.Load())
	}
	if failure := adapter.closeScan(t.Context(), work); failure != nil {
		t.Fatal("failed backend query leaked its PIT", failure)
	}
}
