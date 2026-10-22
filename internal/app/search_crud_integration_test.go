//go:build integration

package app

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	dto "github.com/prometheus/client_model/go"
)

// TestProcessSearchCRUD drives a finite SDK sequence (Put, Read, Native, Lua,
// bulk, Update and Delete) through a CLI process against the local Search
// fixture. An independent oracle checks every acknowledged effect, and all
// owned execution, queue and session resources must return to zero after the
// SDK closes.
func TestProcessSearchCRUD(t *testing.T) {
	backend := testsearch.Open(t)
	binary := buildEndpointProcess(t)
	connection := &Search{URL: backend.URL}
	local := &Local{Backend: BackendConfig{Search: connection}}
	store := StoreConfig{Name: "search", Local: local}
	cfg := emptyConfig(t)
	cfg.Basic.Diagnostics.Address = "127.0.0.1:0"
	cfg.Routing.Stores = []StoreConfig{store}
	process := startProcess(t, binary, cfg)
	pad := "search-crud-no-business-replay"
	expected := map[string]loadRecord{"hot-0": {N: 1, Pad: pad}, "lua-000": {N: 0, Pad: pad}}
	dataset := &loadDataset{store: "search", root: backend.Index, media: "application/json", search: backend, pads: []string{pad}, expected: expected}
	for id, document := range expected {
		raw, err := json.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		status, _ := backend.Do(t, http.MethodPut, "/"+backend.Index+"/_doc/"+id+"?refresh=wait_for", string(raw))
		if status != http.StatusCreated {
			t.Fatal("Search seed failed", id, status)
		}
	}
	request, err := http.NewRequest(http.MethodGet, "/"+backend.Index+"/_doc/hot-0", nil)
	if err != nil {
		t.Fatal(err)
	}
	native, err := weirclient.NewHTTPNativeRequest("", request)
	if err != nil {
		t.Fatal(err)
	}
	dataset.native = []*pb.NativeRequest{native}
	initial := testmetrics.Scrape(t, process.diagnostic)
	rejections := searchCRUDRejections(t, initial)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	openOptions := weirclient.OpenOptions{Seed: process.address, Stores: []string{"search"}}
	client, err := weirclient.Open(ctx, openOptions)
	if err != nil {
		t.Fatal("SDK Resolve failed", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	for _, kind := range []int{1, 0, 2, 3, 4} {
		job := loadJob{kind: kind, sequence: kind}
		if err := executeLoadJob(ctx, client, dataset, job); err != nil {
			t.Fatal(loadMethod(kind), err)
		}
		rememberLoadMutation(dataset, job)
	}
	update := loadRecord{N: 2, Pad: pad}
	raw, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{ContentType: "application/json", Data: raw}
	write := &weirclient.WriteRequest{Resource: backend.Index + "/s:hot-0", Document: document}
	writeOptions := weirclient.WriteOptions{StoreName: "search", Request: write}
	updated, err := client.Put(ctx, writeOptions)
	if err := requireMutationApplied(updated, err); err != nil {
		t.Fatal("Update", err)
	}
	expected["hot-0"] = update
	putJob := loadJob{sequence: 1}
	deleteRequest := &weirclient.DeleteRequest{Resource: backend.Index + "/s:" + loadWriteID(putJob, 0)}
	deleteOptions := weirclient.DeleteOptions{StoreName: "search", Request: deleteRequest}
	deleted, err := client.Delete(ctx, deleteOptions)
	if err := requireMutationApplied(deleted, err); err != nil {
		t.Fatal("Delete", err)
	}
	delete(expected, loadWriteID(putJob, 0))
	verifyLoadOracle(t, dataset)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	waitSearchCRUDIdle(t, process.diagnostic, rejections)
}

func searchCRUDRejections(t *testing.T, metrics map[string]*dto.MetricFamily) float64 {
	t.Helper()
	if metrics["weir_execution_memory_budget_rejections_total"] == nil {
		t.Fatal("execution memory budget rejection counter is missing")
	}
	return testmetrics.Sum(metrics, "weir_execution_memory_budget_rejections_total")
}

// waitSearchCRUDIdle requires every owned execution, queue, publication and
// session gauge to return to zero and the memory latch to be clear within 5s,
// with no new budget rejections.
func waitSearchCRUDIdle(t *testing.T, diagnostics string, rejections float64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		metrics := testmetrics.Scrape(t, diagnostics)
		if searchCRUDRejections(t, metrics) != rejections {
			t.Fatal("finite SDK sequence was rejected by the execution memory budget")
		}
		nonzero := make(map[string]float64)
		if value := testmetrics.Sum(metrics, "weir_memory_latched"); value != 0 {
			nonzero["weir_memory_latched"] = value
		}
		for _, name := range []string{"weir_execution_memory_used_bytes", "weir_execution_memory_leases", "weir_execution_memory_native_leases", "weir_execution_memory_resident_bytes", "weir_execution_memory_input_reserved_bytes", "weir_execution_memory_input_used_bytes", "weir_execution_memory_wire_bytes", "weir_execution_memory_wire_buffers", "weir_execution_memory_starting", "weir_execution_memory_settled", "weir_execution_memory_borrowed", "weir_execution_memory_waiting", "weir_execution_memory_terminal", "weir_execution_memory_ingress_bytes", "weir_execution_memory_ingress_reserved_bytes", "weir_execution_memory_ingress_buffers", "weir_execution_memory_ingress_messages", "weir_execution_memory_ingress_waiting", "weir_execution_memory_ingress_connections", "weir_execution_memory_ingress_connection_bytes", "weir_store_pending_entries", "weir_store_pending_reserved_bytes", "weir_store_active_executions", "weir_store_paused_native_outputs", "weir_store_paused_native_reserved_bytes", "weir_store_result_reserved_entries", "weir_store_result_reserved_bytes", "weir_store_retained_result_reserved_bytes", "weir_store_working_reserved_bytes", "weir_store_publishers", "weir_ingress_sessions", "weir_ingress_queued_response_bytes"} {
			if metrics[name] == nil {
				t.Fatal("required gauge is missing", name)
			}
			if value := testmetrics.Sum(metrics, name); value != 0 {
				nonzero[name] = value
			}
		}
		if len(nonzero) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("owned resources did not return to zero within 5s", nonzero)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
