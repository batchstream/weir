package app

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func TestSearchConnectionFullGraphPreflight(t *testing.T) {
	var contacts atomic.Int32
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { contacts.Add(1) })
	endpoint := httptest.NewServer(handler)
	defer endpoint.Close()
	c := &SearchConnection{Username: "app", Password: "password-sentinel", CAFile: "/missing/ca-sentinel.pem"}
	backend := &Search{
		URL:        "https://unresolved.invalid:443",
		Connection: c,
	}
	local := &Local{Search: backend}
	definition := Service{Name: "search", Local: local}
	route := Route{Store: "records", Service: "search"}
	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	cfg.Routing.Services = []Service{definition}
	cfg.Routing.Routes = []Route{route}
	raw, err := yaml.Marshal(cfg.Routing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRouting(bytes.NewReader(raw)); err != nil {
		t.Fatal("Decode accessed CA/DNS", err)
	}
	// Even an invalid later service must be rejected before the first connection.
	backend.URL = endpoint.URL
	backend.Connection = nil
	invalid := Service{Name: "bad"}
	cfg.Routing.Services = append(cfg.Routing.Services, invalid)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	cfg.Basic.Listeners.Application = address
	start := time.Now()
	node, err := Open(context.Background(), cfg)
	if node != nil || err == nil || contacts.Load() != 0 || time.Since(start) > time.Second {
		t.Fatal("full graph validation performed side effects")
	}
	listener, err = net.Listen("tcp", address)
	if err != nil {
		t.Fatal("preflight bound listener")
	}
	_ = listener.Close()
	cfg.Routing.Services = cfg.Routing.Services[:1]
	backend.URL = "http://host:80"
	backend.Connection = c
	node, err = Open(context.Background(), cfg)
	if node != nil || err == nil || strings.Contains(err.Error(), "sentinel") {
		t.Fatal("HTTP credentials accepted/leaked")
	}
	backend.URL = "https://search.test:443"
	raw, err = yaml.Marshal(cfg.Routing)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"server_name", "insecure_skip_verify", "auth_provider", "token", "resolver", "tls"} {
		input := strings.Replace(string(raw), "connection:\n", "connection:\n                    "+field+": unknown\n", 1)
		if _, err := DecodeRouting(strings.NewReader(input)); err == nil {
			t.Fatal("unknown connection/identity option accepted", field)
		}
	}
}

func TestStartupPreservesRedactedQualificationReason(t *testing.T) {
	connection := &SearchConnection{
		Username: "user-sentinel",
		Password: "password-sentinel",
		CAFile:   "/missing/ca-sentinel.pem",
	}
	backend := &Search{
		URL:        "https://unresolved.invalid:9200",
		Connection: connection,
	}
	local := &Local{Search: backend}
	service := Service{Name: "catalog", Local: local}
	route := Route{Store: "records", Service: service.Name}
	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	cfg.Routing.Services = []Service{service}
	cfg.Routing.Routes = []Route{route}
	node, err := Open(context.Background(), cfg)
	if node != nil ||
		err == nil ||
		!strings.Contains(err.Error(), `local Store "records" startup qualification failed: Search CA file unavailable or invalid`) ||
		strings.Contains(err.Error(), "sentinel") ||
		strings.Contains(err.Error(), "unresolved.invalid") {
		t.Fatal("startup must preserve the reason while redacting configuration", err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/" {
			_, _ = w.Write([]byte(`{"version":{"number":"8.19.22","build_flavor":"default"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"persistent":{"action.auto_create_index":"true"},"secret":"response-sentinel"}`))
	})
	endpoint := httptest.NewServer(handler)
	defer endpoint.Close()
	backend.URL = endpoint.URL
	backend.Connection = nil
	node, err = Open(context.Background(), cfg)
	if node != nil ||
		err == nil ||
		!strings.Contains(err.Error(), "action.auto_create_index=false") ||
		strings.Contains(err.Error(), "sentinel") ||
		strings.Contains(err.Error(), endpoint.URL) {
		t.Fatal("startup must identify the rejected backend policy without its response", err)
	}
}

func TestSearchStartupIdentifiesProductsWithoutVersionRestrictions(t *testing.T) {
	cases := []struct {
		name, identity, rejection string
	}{
		{"elasticsearch-older", `{"version":{"number":"1.2.3","build_flavor":"default"}}`, ""},
		{"elasticsearch-newer", `{"version":{"number":"99.10.20-next+fixture","build_flavor":"default"}}`, ""},
		{"elasticsearch-without-number", `{"version":{"build_flavor":"default"}}`, ""},
		{"opensearch-older", `{"version":{"number":"1.2.3","distribution":"opensearch"}}`, ""},
		{"opensearch-newer", `{"version":{"number":"99.10.20-next+fixture","distribution":"opensearch"}}`, ""},
		{"opensearch-without-number", `{"version":{"distribution":"opensearch"}}`, ""},
		{"unknown-product", `{"version":{"number":"8.19.22","distribution":"unknown-product"},"secret":"response-sentinel"}`, "unsupported Search server product"},
		{"malformed-product", `{"version":{"distribution":123},"secret":"response-sentinel"}`, "search product identification failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var roots, settings atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/":
					roots.Add(1)
					_, _ = w.Write([]byte(tc.identity))
				case "/_cluster/settings":
					settings.Add(1)
					_, _ = w.Write([]byte(`{"defaults":{"action.auto_create_index":"false"}}`))
				default:
					t.Error("startup must identify the server without requesting resource targets")
					http.Error(w, "unexpected fixture request", http.StatusNotFound)
				}
			})
			endpoint := httptest.NewServer(handler)
			defer endpoint.Close()

			backend := &Search{URL: endpoint.URL}
			local := &Local{Search: backend}
			service := Service{Name: "server", Local: local}
			route := Route{Store: "records", Service: service.Name}
			cfg := DefaultConfig()
			cfg.Basic.Listeners.Application = "127.0.0.1:0"
			cfg.Routing.Services = []Service{service}
			cfg.Routing.Routes = []Route{route}
			if tc.rejection != "" {
				// An occupied listener catches any assembly attempted after rejection.
				cfg.Basic.Listeners.Application = strings.TrimPrefix(endpoint.URL, "http://")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			node, err := Open(ctx, cfg)
			if tc.rejection != "" {
				if node != nil {
					_ = node.Close(context.Background())
					t.Fatal("unsupported product reached assembly")
				}
				if err == nil || err.Error() != `local Store "records" startup qualification failed: `+tc.rejection || roots.Load() != 1 || settings.Load() != 0 {
					t.Fatal("product rejection must precede policy checks and listeners, preserving its safe reason", err, roots.Load(), settings.Load())
				}
				return
			}
			if err != nil {
				t.Fatal("identified products must start regardless of version number", err)
			}
			t.Cleanup(func() {
				drain, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				if err := node.Close(drain); err != nil {
					t.Error(err)
				}
			})
			if err := node.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if roots.Load() != 1 || settings.Load() != 1 {
				t.Fatal("startup must retain the server policy check", roots.Load(), settings.Load())
			}
		})
	}
}
