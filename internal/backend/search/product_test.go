package search

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestSearchProductDetection(t *testing.T) {
	type productCase struct {
		name     string
		metadata string
		product  string
		failure  string
		status   int
	}
	cases := []productCase{
		{name: "es-missing-number", metadata: `{"version":{"build_flavor":"default"}}`, product: ElasticsearchProduct},
		{name: "os-missing-number", metadata: `{"version":{"distribution":"opensearch"}}`, product: OpenSearchProduct},
		{name: "es-unparsed-number", metadata: `{"version":{"number":{"ignored":true},"build_flavor":"default"}}`, product: ElasticsearchProduct},
		{name: "os-unparsed-number", metadata: `{"version":{"number":null,"distribution":"opensearch"}}`, product: OpenSearchProduct},
		{name: "missing-version", metadata: `{}`, failure: "unsupported Search server product"},
		{name: "empty-version", metadata: `{"version":{}}`, failure: "unsupported Search server product"},
		{name: "null-version", metadata: `{"version":null}`, failure: "unsupported Search server product"},
		{name: "number-only", metadata: `{"version":{"number":"8.19.22"}}`, failure: "unsupported Search server product"},
		{name: "wrong-es-flavor", metadata: `{"version":{"build_flavor":"oss"}}`, failure: "unsupported Search server product"},
		{name: "wrong-es-distribution", metadata: `{"version":{"build_flavor":"default","distribution":"unknown"}}`, failure: "unsupported Search server product"},
		{name: "unknown-distribution", metadata: `{"version":{"distribution":"metadata-secret-sentinel"}}`, failure: "unsupported Search server product"},
		{name: "wrong-os-distribution", metadata: `{"version":{"distribution":"OpenSearch"}}`, failure: "unsupported Search server product"},
		{name: "wrong-flavor-type", metadata: `{"version":{"build_flavor":123}}`, failure: "search product identification failed"},
		{name: "wrong-distribution-type", metadata: `{"version":{"distribution":{}}}`, failure: "search product identification failed"},
		{name: "wrong-version-type", metadata: `{"version":[]}`, failure: "search product identification failed"},
		{name: "malformed", metadata: `{"version":`, failure: "search product identification failed"},
		{name: "unauthorized", metadata: `{"error":"metadata-secret-sentinel"}`, status: http.StatusUnauthorized, failure: "search product identification failed"},
		{name: "unavailable", metadata: `{"error":"metadata-secret-sentinel"}`, status: http.StatusServiceUnavailable, failure: "search product identification failed"},
	}
	products := []struct {
		name     string
		identity string
	}{
		{name: ElasticsearchProduct, identity: `"build_flavor":"default"`},
		{name: OpenSearchProduct, identity: `"distribution":"opensearch"`},
	}
	versions := []string{"1.0.0", "2.19.0", "7.17.0", "8.19.22", "8.19.23", "9.0.0", "12.3.4", "99.2.1-preview+build.7", ""}
	for _, product := range products {
		for _, version := range versions {
			metadata := fmt.Sprintf(`{"version":{"number":%q,%s}}`, version, product.identity)
			variant := productCase{name: product.name + "-" + version, metadata: metadata, product: product.name}
			cases = append(cases, variant)
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Method != http.MethodGet {
					t.Error("product identification must not modify backend state", r.Method)
				}
				switch r.URL.Path {
				case "/":
					if tc.status != 0 {
						w.WriteHeader(tc.status)
					}
					fmt.Fprint(w, tc.metadata)
				case "/_cluster/settings":
					if tc.product == "" {
						t.Error("product rejection performed further backend work")
					}
					fmt.Fprint(w, `{"persistent":{"action.auto_create_index":"false"}}`)
				default:
					t.Error("startup attempted index qualification", r.URL.Path)
					http.Error(w, "unexpected index request", http.StatusNotFound)
				}
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			cfg := Config{Store: "search", URL: server.URL, Pool: 1}
			a, err := Open(context.Background(), cfg)
			if tc.product == "" {
				if a != nil {
					_ = a.Close()
					t.Fatal("unidentified product accepted")
				}
				if err == nil || err.Error() != tc.failure || requests.Load() != 1 {
					t.Fatal("expected one product probe and sanitized startup failure", err, requests.Load())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			if a.dialect != tc.product || requests.Load() != 2 {
				t.Fatal("incorrect automatic product detection", a.dialect, requests.Load())
			}
		})
	}
}
