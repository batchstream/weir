package search

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestExactSearchProfiles(t *testing.T) {
	for _, profile := range []string{"elasticsearch-8.17.0", "opensearch-2.19.0", "elasticsearch-8.19.21", "opensearch-2.19.5", "elasticsearch-8", "opensearch-2", ""} {
		cfg := Config{Store: "search", URL: "http://127.0.0.1:1", Index: "records", Profile: profile, Pool: 1}
		if err := ValidateConfig(cfg); err == nil {
			t.Fatal("unqualified profile accepted", profile)
		}
	}
	cases := []struct {
		name, profile, version, distribution, flavor string
	}{
		{name: "old-es", profile: ElasticsearchProfile, version: "8.17.0", flavor: "default"},
		{name: "old-os", profile: OpenSearchProfile, version: "2.19.0", distribution: "opensearch"},
		{name: "unqualified-es-patch", profile: ElasticsearchProfile, version: "8.19.23", flavor: "default"},
		{name: "unqualified-os-patch", profile: OpenSearchProfile, version: "2.19.7", distribution: "opensearch"},
		{name: "wrong-es-flavor", profile: ElasticsearchProfile, version: ElasticsearchVersion, flavor: "oss"},
		{name: "wrong-es-distribution", profile: ElasticsearchProfile, version: ElasticsearchVersion, flavor: "default", distribution: "opensearch"},
		{name: "wrong-os-distribution", profile: OpenSearchProfile, version: OpenSearchVersion},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.URL.Path != "/" {
					t.Error("version rejection performed further backend work")
				}
				fmt.Fprintf(w, `{"version":{"number":%q,"distribution":%q,"build_flavor":%q}}`, tc.version, tc.distribution, tc.flavor)
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			cfg := Config{Store: "search", URL: server.URL, Index: "records", Profile: tc.profile, Pool: 1}
			a, err := Open(context.Background(), cfg)
			if a != nil {
				_ = a.Close()
				t.Fatal("unqualified server accepted")
			}
			if err == nil || requests.Load() != 1 {
				t.Fatal("expected one version probe and startup failure", err, requests.Load())
			}
		})
	}
}
