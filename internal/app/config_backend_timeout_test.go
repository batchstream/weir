package app

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func TestBackendTimeoutConfiguration(t *testing.T) {
	prefix := "stores:\n  - name: records\n    search:\n      url: http://127.0.0.1:9200\n"
	for _, configured := range []string{"", "    backend_timeout: 10s\n", "    backend_timeout: 250ms\n"} {
		input := prefix + configured
		routing, err := DecodeRouting(strings.NewReader(input))
		if err != nil {
			t.Fatal(err)
		}
		expected := 2 * time.Second
		if strings.Contains(configured, "10s") {
			expected = 10 * time.Second
		} else if strings.Contains(configured, "250ms") {
			expected = 250 * time.Millisecond
		}
		if actual := routing.Stores[0].Local.runtimeLimits().BackendTimeout; actual != expected {
			t.Fatal("backend deadline did not reach Store runtime", actual, expected)
		}
		encodedYAML, err := yaml.Marshal(routing)
		if err != nil {
			t.Fatal(err)
		}
		encodedJSON, err := json.Marshal(routing)
		if err != nil {
			t.Fatal(err)
		}
		for _, encoded := range [][]byte{encodedYAML, encodedJSON} {
			decoded, err := DecodeRouting(strings.NewReader(string(encoded)))
			if err != nil || decoded.Stores[0].Local.runtimeLimits().BackendTimeout != expected {
				t.Fatal("configuration round trip changed the deadline", err)
			}
		}
	}
}

func TestBackendTimeoutRejectsInvalidConfiguration(t *testing.T) {
	prefix := "stores:\n  - name: records\n    mongodb:\n      uri: mongodb://127.0.0.1:27017\n"
	for _, value := range []string{"0s", "-1s", "10000000000000000000s", "10", "null", "true", "{}", "[]", "missing-unit"} {
		input := prefix + "    backend_timeout: " + value + "\n"
		if _, err := DecodeRouting(strings.NewReader(input)); err == nil {
			t.Fatal("invalid backend timeout accepted", value)
		}
	}
	zero := Duration(0)
	local := &Local{MongoDB: &Mongo{URI: "mongodb://127.0.0.1:27017"}, BackendTimeout: &zero}
	definition := StoreConfig{Name: "records", Local: local}
	routing := RoutingConfig{Stores: []StoreConfig{definition}}
	if err := routing.Validate(); err == nil {
		t.Fatal("programmatically supplied zero timeout accepted")
	}
}
