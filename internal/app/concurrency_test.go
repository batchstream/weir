package app

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestLocalConcurrencyConfiguration(t *testing.T) {
	for _, c := range []int{0, 1, 2, 4, 32, -1, 33} {
		t.Run(fmt.Sprint(c), func(t *testing.T) {
			connection := &SearchConnection{CAFile: "/missing/concurrency-ca.pem"}
			backend := &Search{
				URL:        "https://unresolved.invalid:443",
				Connection: connection,
			}
			local := &Local{Search: backend, MaxConcurrency: c}
			service := Service{Name: "local", Local: local}
			route := Route{Store: "records", Service: "local"}
			cfg := DefaultConfig()
			cfg.Basic.Listeners.Application = "127.0.0.1:0"
			cfg.Routing.Services, cfg.Routing.Routes = []Service{service}, []Route{route}
			raw, err := yaml.Marshal(cfg.Routing)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeRouting(bytes.NewReader(raw))
			if c < 0 || c > 32 {
				if err == nil {
					t.Fatal("invalid concurrency accepted")
				}
				return
			}
			if err != nil {
				t.Fatal("pure validation attempted CA or DNS I/O", err)
			}
			expected := c
			if c == 0 {
				expected = 4
			}
			actual := decoded.Services[0].Local
			if actual.runtimeLimits().Concurrency != expected || actual.searchConfig("records").Pool != expected {
				t.Fatal("different validation/assembly limits")
			}
			if c == 0 {
				omitted := strings.Replace(string(raw), "        max_concurrency: 0\n", "", 1)
				decoded, err = DecodeRouting(strings.NewReader(omitted))
				if err != nil || decoded.Services[0].Local.runtimeLimits().Concurrency != 4 {
					t.Fatal("omitted default", err)
				}
			}
		})
	}
}

func TestLocalConcurrencyWholeGraphBeforeIO(t *testing.T) {
	var contacts atomic.Int32
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { contacts.Add(1) })
	endpoint := httptest.NewServer(handler)
	defer endpoint.Close()
	backend := &Search{URL: endpoint.URL}
	first := &Local{Search: backend}
	for _, c := range []int{-1, 33} {
		mongo := &Mongo{
			URI: "mongodb://unresolved.invalid:27017/?tls=true&tlsCAFile=/missing/concurrency-ca.pem",
		}
		invalid := &Local{MongoDB: mongo, MaxConcurrency: c}
		firstService := Service{Name: "first", Local: first}
		invalidService := Service{Name: "invalid", Local: invalid}
		firstRoute := Route{Store: "first", Service: "first"}
		invalidRoute := Route{Store: "invalid", Service: "invalid"}
		cfg := DefaultConfig()
		// Occupied address would fail if Open reached listener binding.
		cfg.Basic.Listeners.Application = strings.TrimPrefix(endpoint.URL, "http://")
		cfg.Routing.Services = []Service{firstService, invalidService}
		cfg.Routing.Routes = []Route{firstRoute, invalidRoute}
		node, err := Open(context.Background(), cfg)
		if node != nil || err == nil || !strings.Contains(err.Error(), "runtime bounds") || contacts.Load() != 0 {
			t.Fatal("invalid later concurrency reached I/O", err, contacts.Load())
		}
	}
}
