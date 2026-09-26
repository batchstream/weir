//go:build integration

// Package testsearch owns only unique test indexes on fixed loopback test backends.
package testsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var sequence atomic.Uint64

type Backend struct {
	URL, Profile, Index string
	Client              *http.Client
}

func Open(t *testing.T) *Backend {
	t.Helper()
	var endpoint, profile string
	switch os.Getenv("WEIR_SEARCH_INTEGRATION") {
	case "elasticsearch":
		endpoint, profile = "http://127.0.0.1:19200", "elasticsearch-8.17.0"
	case "opensearch":
		endpoint, profile = "http://127.0.0.1:19201", "opensearch-2.19.0"
	case "":
		t.Skip("Search real-backend suite requires explicit WEIR_SEARCH_INTEGRATION=elasticsearch|opensearch")
	default:
		t.Fatal("invalid search integration profile")
	}
	transport := &http.Transport{Proxy: nil, MaxConnsPerHost: 8, MaxIdleConnsPerHost: 8, DisableCompression: true}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	index := fmt.Sprintf("weir_m2_%d_%d", os.Getpid(), sequence.Add(1))
	b := &Backend{URL: endpoint, Profile: profile, Index: index, Client: client}
	status, raw := b.Do(t, "GET", "/", "")
	var root struct {
		ClusterName string `json:"cluster_name"`
		Version     struct{ Number, Distribution string }
	}
	if status != 200 || json.Unmarshal(raw, &root) != nil || root.ClusterName != "weir-m2-"+os.Getenv("WEIR_SEARCH_INTEGRATION") || strings.TrimPrefix(profile, "elasticsearch-") != root.Version.Number && strings.TrimPrefix(profile, "opensearch-") != root.Version.Number {
		t.Fatal("wrong isolated backend", status)
	}
	t.Cleanup(transport.CloseIdleConnections)
	b.Create(t, index, `{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"properties":{"n":{"type":"long"}}}}`)
	return b
}
func (b *Backend) Create(t *testing.T, index, body string) {
	t.Helper()
	if !strings.HasPrefix(index, b.Index) {
		t.Fatal("refusing non-owned index")
	}
	status, raw := b.Do(t, "PUT", "/"+index, body)
	if status != 200 {
		t.Fatalf("test index setup status=%d body=%s", status, raw)
	}
	t.Cleanup(func() {
		status, _ := b.Do(t, "DELETE", "/"+index, "")
		if status != 200 && status != 404 {
			t.Error("test index cleanup failed", status)
		}
	})
}
func (b *Backend) Do(t *testing.T, method, path, body string) (int, []byte) {
	t.Helper()
	request, err := http.NewRequest(method, b.URL+path, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	request.GetBody = nil
	request.Header.Set("Content-Type", "application/json")
	response, err := b.Client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, raw
}
