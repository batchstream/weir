//go:build integration

package search

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/batchstream/weir/internal/execution"
)

func TestSearchCrossIndexPureReadsUseOneNativeExchange(t *testing.T) {
	base, backend := setupSearch(t)
	other := backend.Index + "_other"
	backend.Create(t, other, `{"settings":{"number_of_shards":1,"number_of_replicas":0}}`)
	for i, index := range []string{backend.Index, other} {
		status, raw := backend.Do(t, "PUT", "/"+index+"/_doc/same", fmt.Sprintf(`{"n":%d}`, i+1))
		if status != 201 {
			t.Fatal("seed failed", status, string(raw))
		}
	}
	endpoint, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxyHandler := httputil.NewSingleHostReverseProxy(endpoint)
	var reads atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/_mget") {
			reads.Add(1)
		}
		proxyHandler.ServeHTTP(w, r)
	})
	proxy := httptest.NewServer(handler)
	defer proxy.Close()
	config := base.config
	config.URL = proxy.URL
	adapter, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	works := make([]*execution.Plan, 128)
	for i := range works {
		index := backend.Index
		if i%2 != 0 {
			index = other
		}
		id := fmt.Sprintf("missing-%d", i)
		if i < 4 {
			id = "same"
		}
		works[i] = batchTestPlan(t, adapter, "read", searchResource(index, id))
	}
	results := adapter.executeRecords(context.Background(), works)
	for i, result := range results {
		if i < 4 {
			want := fmt.Sprintf(`{"n":%d}`, i%2+1)
			if string(result.GetReadResult().GetDocument().GetData()) != want {
				t.Fatal("same ID crossed indexes", result)
			}
		} else if result.GetReadResult().GetMissing() == nil {
			t.Fatal("independent missing ID lost correspondence", result)
		}
	}
	if reads.Load() != 1 {
		t.Fatal("128 small reads were split into conservative 4 MiB/source windows", reads.Load())
	}
	t.Log("128 calls across two concrete indexes -> one real-time native _mget, 126 unique records, exact sources and missing IDs verified")
}
