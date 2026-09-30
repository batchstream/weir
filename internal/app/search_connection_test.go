package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/backend/search"
)

func TestSearchConnectionFullGraphPreflight(t *testing.T) {
	var contacts atomic.Int32
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { contacts.Add(1) })
	endpoint := httptest.NewServer(handler)
	defer endpoint.Close()
	c := &search.Connection{Username: "app", Password: "password-sentinel", CAFile: "/missing/ca-sentinel.pem"}
	backend := &Search{URL: "https://unresolved.invalid:443", Index: "records", Profile: "elasticsearch-8.19.22", Connection: c}
	local := &Local{Search: backend}
	definition := Service{Name: "search", Local: local}
	route := Route{Store: "records", Service: "search"}
	cfg := DefaultConfig()
	cfg.Application = "127.0.0.1:0"
	cfg.Services = []Service{definition}
	cfg.Routes = []Route{route}
	raw, err := json.Marshal(cfg.RoutingConfig)
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
	cfg.Services = append(cfg.Services, invalid)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	cfg.Application = address
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
	cfg.Services = cfg.Services[:1]
	backend.URL = "http://host:80"
	backend.Connection = c
	node, err = Open(context.Background(), cfg)
	if node != nil || err == nil || strings.Contains(err.Error(), "sentinel") {
		t.Fatal("HTTP credentials accepted/leaked")
	}
	backend.URL = "https://search.test:443"
	raw, err = json.Marshal(cfg.RoutingConfig)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"server_name", "insecure_skip_verify", "auth_provider", "token", "resolver", "tls"} {
		input := strings.Replace(string(raw), `"connection":{`, `"connection":{"`+field+`":true,`, 1)
		if _, err := DecodeRouting(strings.NewReader(input)); err == nil {
			t.Fatal("unknown connection/identity option accepted", field)
		}
	}
}

func TestStartupPreservesRedactedQualificationReason(t *testing.T) {
	connection := &search.Connection{Username: "user-sentinel", Password: "password-sentinel", CAFile: "/missing/ca-sentinel.pem"}
	backend := &Search{URL: "https://unresolved.invalid:9200", Index: "records", Profile: search.ElasticsearchProfile, Connection: connection}
	local := &Local{Search: backend}
	service := Service{Name: "catalog", Local: local}
	route := Route{Store: "records", Service: service.Name}
	cfg := DefaultConfig()
	cfg.Application = "127.0.0.1:0"
	cfg.Services = []Service{service}
	cfg.Routes = []Route{route}
	node, err := Open(context.Background(), cfg)
	if node != nil || err == nil || !strings.Contains(err.Error(), `local Store "records" startup qualification failed: Search CA file unavailable or invalid`) || strings.Contains(err.Error(), "sentinel") || strings.Contains(err.Error(), "unresolved.invalid") {
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
	if node != nil || err == nil || !strings.Contains(err.Error(), "action.auto_create_index=false") || strings.Contains(err.Error(), "sentinel") || strings.Contains(err.Error(), endpoint.URL) {
		t.Fatal("startup must identify the rejected backend policy without its response", err)
	}
}
