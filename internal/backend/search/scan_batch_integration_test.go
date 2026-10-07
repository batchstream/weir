//go:build integration

package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestSearchScanLargeSourcesDownsizeAndResume(t *testing.T) {
	base, backend := setupSearch(t)
	status, _ := backend.Do(t, "PUT", "/"+backend.Index+"/_mapping", `{"properties":{"pad":{"type":"keyword","index":false,"doc_values":false}}}`)
	if status != 200 {
		t.Fatal("owned index mapping", status)
	}
	for position := 0; position < 9; position++ {
		body := fmt.Sprintf(`{"n":%d,"pad":"%s"}`, position, strings.Repeat("x", 1<<20))
		status, _ := backend.Do(t, "PUT", fmt.Sprintf("/%s/_doc/%d", backend.Index, position), body)
		if status != 201 {
			t.Fatal("large source setup", status)
		}
	}
	backend.Do(t, "POST", "/"+backend.Index+"/_refresh", "")
	var mu sync.Mutex
	var sizes []int
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(io.LimitReader(r.Body, metadataLimit))
		if err != nil {
			t.Error(err)
			return
		}
		if r.URL.Path == "/_search" {
			var request scanSearchRequest
			if err := json.Unmarshal(raw, &request); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			sizes = append(sizes, request.Size)
			mu.Unlock()
		}
		forward, err := http.NewRequestWithContext(r.Context(), r.Method, backend.URL+r.URL.RequestURI(), bytes.NewReader(raw))
		if err != nil {
			t.Error(err)
			return
		}
		forward.Header = r.Header.Clone()
		forward.GetBody = nil
		response, err := backend.Client.Do(forward)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
	})
	proxy := httptest.NewServer(handler)
	defer proxy.Close()
	config := base.config
	config.URL = proxy.URL
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Trace every native fetch, including the first one sharing PIT allocation.
	request := &pb.ScanRequest{Resource: backend.Index, PageSize: 8}
	work, failure := base.prepareScan(request)
	if failure != nil {
		t.Fatal(failure)
	}
	state := work.Backend.(*scanPlan)
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if failure := base.closeScan(cleanup, work); failure != nil {
			t.Error("owned PIT cleanup", failure)
		}
	}()
	transport := newTransport(1)
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
	adapter := &Adapter{dialect: backend.Product, config: config, ctx: ctx, client: client}
	seen := map[string]bool{}
	var token []byte
	for step := 0; step < 4; step++ {
		page := adapter.fetchScan(ctx, work)
		if page.Failure != nil || page.Exhausted || len(page.Documents) == 0 {
			t.Fatal("bounded large-source page", step, page.Failure, len(page.Documents))
		}
		retained := 0
		for _, document := range page.Documents {
			retained += len(document.Data)
			var hit struct {
				N *int `json:"n"`
			}
			if json.Unmarshal(document.Data, &hit) != nil || hit.N == nil || seen[fmt.Sprint(*hit.N)] {
				t.Fatal("duplicate or invalid native hit", hit.N)
			}
			seen[fmt.Sprint(*hit.N)] = true
		}
		if retained > execution.ScanBatchBytes {
			t.Fatal("large source output exceeded retained byte bound", retained)
		}
		state.Count += uint64(len(page.Documents))
		if page.Complete {
			token = page.NextContinuationToken
			break
		}
	}
	if len(seen) != 8 || len(token) == 0 || state.batchSize != 3 {
		t.Fatal("large source checkpoint omitted hits or forgot safe size", len(seen), state.batchSize)
	}
	request.ContinuationToken = token
	resumed, failure := adapter.prepareScan(request)
	if failure != nil {
		t.Fatal(failure)
	}
	resumedState := resumed.Backend.(*scanPlan)
	if resumedState.batchSize != execution.ScanBatchDocuments {
		t.Fatal("resume inherited internal batch tuning", resumedState.batchSize)
	}
	page := adapter.fetchScan(ctx, resumed)
	if page.Failure != nil || len(page.Documents) != 1 || page.Exhausted {
		t.Fatal("resume lost final source", page.Failure, len(page.Documents))
	}
	var last struct {
		N *int `json:"n"`
	}
	if json.Unmarshal(page.Documents[0].Data, &last) != nil || last.N == nil || seen[fmt.Sprint(*last.N)] {
		t.Fatal("resumed source duplicate", last.N)
	}
	resumedState.Count++
	page = adapter.fetchScan(ctx, resumed)
	if page.Failure != nil || !page.Exhausted || len(page.Documents) != 0 {
		t.Fatal("validated final response did not exhaust", page.Failure)
	}
	mu.Lock()
	observed := append([]int(nil), sizes...)
	mu.Unlock()
	if len(observed) != 6 || observed[0] != 8 || observed[1] != 4 || observed[2] != 3 || observed[3] != 2 || observed[4] != 8 || observed[5] != 7 {
		t.Fatal("physical read sizing or resume regressed", observed)
	}
	if protocol.ScanPageSize(request) != 8 {
		t.Fatal("logical page changed during adaptation")
	}
	if failure := base.closeScan(ctx, work); failure != nil {
		t.Fatal("owned PIT cleanup", failure)
	}
	t.Logf("9 one-MiB sources: native request sizes %v; first logical page8+resumed1 all unique, bounded output", observed)
}
