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
	cases := []struct {
		name         string
		version      string
		distribution string
		flavor       string
		dialect      string
	}{
		{name: "elasticsearch", version: ElasticsearchVersion, flavor: "default", dialect: ElasticsearchProfile},
		{name: "opensearch", version: OpenSearchVersion, distribution: "opensearch", dialect: OpenSearchProfile},
		{name: "old-es", version: "8.17.0", flavor: "default"},
		{name: "old-os", version: "2.19.0", distribution: "opensearch"},
		{name: "unqualified-es-patch", version: "8.19.23", flavor: "default"},
		{name: "unqualified-os-patch", version: "2.19.7", distribution: "opensearch"},
		{name: "wrong-es-flavor", version: ElasticsearchVersion, flavor: "oss"},
		{name: "wrong-es-distribution", version: ElasticsearchVersion, flavor: "default", distribution: "opensearch"},
		{name: "wrong-os-distribution", version: OpenSearchVersion},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				switch r.URL.Path {
				case "/":
					fmt.Fprintf(w, `{"version":{"number":%q,"distribution":%q,"build_flavor":%q}}`, tc.version, tc.distribution, tc.flavor)
				case "/_cluster/settings":
					if tc.dialect == "" {
						t.Error("version rejection performed further backend work")
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
			if tc.dialect == "" {
				if a != nil {
					_ = a.Close()
					t.Fatal("unqualified server accepted")
				}
				if err == nil || requests.Load() != 1 {
					t.Fatal("expected one version probe and startup failure", err, requests.Load())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			if a.dialect != tc.dialect || requests.Load() != 2 {
				t.Fatal("incorrect automatic product detection", a.dialect, requests.Load())
			}
		})
	}
}
