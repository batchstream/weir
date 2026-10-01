//go:build integration

package search

import (
	"strings"
	"testing"

	"github.com/batchstream/weir/internal/testutil/testsearch"
)

// Safe validation probes on a newly owned backend, not a CVE exploit reproduction.
func TestSearchSecurityInputBoundary(t *testing.T) {
	b := testsearch.Open(t)
	if b.Product == ElasticsearchProduct {
		body := `{"text":"small fixture input","tokenizer":"standard","filter":[{"type":"min_hash","hash_count":10001,"bucket_count":1,"hash_set_size":1}]}`
		status, raw := b.Do(t, "POST", "/"+b.Index+"/_analyze", body)
		if status != 400 || !strings.Contains(string(raw), "must not exceed [10000]") {
			t.Fatal("Elasticsearch min_hash parameter cap not enforced", status, string(raw))
		}
		t.Log("new native ES min_hash cap rejects 10001 before filter construction; no OOM input")
		return
	}
	// This disposable cluster has no external users. Restore the temporary limit
	// independently of the test result; production Weir never changes settings.
	t.Cleanup(func() {
		status, _ := b.Do(t, "PUT", "/_cluster/settings", `{"transient":{"search.query.max_query_string_length":null}}`)
		if status != 200 {
			t.Error("fixture query limit cleanup", status)
		}
	})
	status, raw := b.Do(t, "PUT", "/_cluster/settings", `{"transient":{"search.query.max_query_string_length":32}}`)
	if status != 200 {
		t.Fatal("owned query limit setup", status, string(raw))
	}
	body := `{"query":{"query_string":{"query":"` + strings.Repeat("a", 33) + `"}}}`
	status, raw = b.Do(t, "POST", "/"+b.Index+"/_search", body)
	if status != 400 || !strings.Contains(string(raw), "32") {
		t.Fatal("OpenSearch query string cap not enforced", status, string(raw))
	}
	t.Log("new native OS query_string length cap rejects 33 characters at configured limit 32; no complex query attack")
}
