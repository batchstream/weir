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
	if metrics["weir_store_rejections_total"] == nil {
		t.Fatal("Store rejection counter is missing")
	}
	return testmetrics.Sum(metrics, "weir_store_rejections_total")
}

// waitSearchCRUDIdle requires every execution, queue and session gauge to
// return to zero and the memory latch to be clear within 5s, with no new
// Store rejections.
func waitSearchCRUDIdle(t *testing.T, diagnostics string, rejections float64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		metrics := testmetrics.Scrape(t, diagnostics)
		if searchCRUDRejections(t, metrics) != rejections {
			t.Fatal("finite SDK sequence was rejected by the Store")
		}
		nonzero := make(map[string]float64)
		for _, name := range []string{"weir_memory_latched", "weir_store_queued_operations", "weir_store_queued_bytes", "weir_store_active_executions", "weir_store_active_natives", "weir_ingress_sessions"} {
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
