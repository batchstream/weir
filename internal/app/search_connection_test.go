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
	backend := &Search{URL: "https://unresolved.invalid:443", Index: "records", Profile: "elasticsearch-8.17.0", Connection: c}
	local := &Local{Search: backend}
	definition := Service{Name: "search", Local: local}
	route := Route{Store: "records", Service: "search"}
	cfg := DefaultConfig()
	cfg.Application = "127.0.0.1:0"
	cfg.Services = []Service{definition}
	cfg.Routes = []Route{route}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(bytes.NewReader(raw)); err != nil {
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
	raw, err = json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"server_name", "insecure_skip_verify", "auth_provider", "token", "resolver", "tls"} {
		input := strings.Replace(string(raw), `"connection":{`, `"connection":{"`+field+`":true,`, 1)
		if _, err := Decode(strings.NewReader(input)); err == nil {
			t.Fatal("unknown connection/identity option accepted", field)
		}
	}
}
