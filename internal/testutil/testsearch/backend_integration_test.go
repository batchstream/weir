//go:build integration

package testsearch

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
)

func TestResponseError(t *testing.T) {
	tests := []struct {
		name, body, want string
	}{
		{"capacity", `{"error":{"type":"circuit_breaking_exception","reason":"private URL credential resource","root_cause":[{"reason":"private"}]},"status":503}`, "circuit_breaking_exception"},
		{"rejection", `{"error":{"type":"es_rejected_execution_exception"}}`, "es_rejected_execution_exception"},
		{"malformed", `{"error":`, "unavailable"},
		{"string error", `{"error":"private"}`, "unavailable"},
		{"missing type", `{"error":{"reason":"private"}}`, "unavailable"},
		{"unsafe type", `{"error":{"type":"https://private/credential"}}`, "unavailable"},
		{"long type", `{"error":{"type":"` + strings.Repeat("a", 97) + `"}}`, "unavailable"},
		{"oversized body", `{"error":{"type":"exception"},"reason":"` + strings.Repeat("a", 2<<20) + `"}`, "unavailable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, status := range []int{429, 503} {
				got := ResponseError(status, []byte(test.body)).Error()
				want := fmt.Sprintf("HTTP status=%d backend_error_type=%s", status, test.want)
				if got != want {
					t.Fatalf("got %q, want %q", got, want)
				}
			}
		})
	}
}

func TestExecutionProfileContractOwnedProduct(t *testing.T) {
	for product, expected := range map[string]string{"elasticsearch": "elasticsearch-8.19.22", "opensearch": "opensearch-3.3.0"} {
		actual, err := ownedProfile(product)
		if err != nil || actual != expected {
			t.Fatal(product, actual, err)
		}
	}
	for _, product := range []string{"", "unknown", "opensearch-2.19", "linux-native-opensearch-functional-v1"} {
		if _, err := ownedProfile(product); err == nil {
			t.Fatal("invalid product accepted before HTTP", product)
		}
	}
}

func TestExecutionProfileContractOwnedRejectHelper(t *testing.T) {
	if os.Getenv("WEIR_OWNED_REJECT_HELPER") != "1" {
		return
	}
	opts := OwnedEndpoint{URL: os.Getenv("WEIR_OWNED_REJECT_URL"), Owner: "weir-finite-" + strings.Repeat("a", 32), Product: os.Getenv("WEIR_OWNED_REJECT_PRODUCT")}
	OpenOwned(t, opts)
}

func TestExecutionProfileContractOwnedInvalidProductBeforeHTTP(t *testing.T) {
	var requests atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	for _, product := range []string{"", "unknown", "opensearch-2.19"} {
		command := exec.Command(os.Args[0], "-test.run=^TestExecutionProfileContractOwnedRejectHelper$", "-test.count=1")
		command.Env = []string{"WEIR_OWNED_REJECT_HELPER=1", "WEIR_OWNED_REJECT_URL=" + server.URL, "WEIR_OWNED_REJECT_PRODUCT=" + product}
		raw, err := command.CombinedOutput()
		if err == nil || !strings.Contains(string(raw), "missing or unsupported owned search product") {
			t.Fatal("missing early product rejection", product, string(raw), err)
		}
	}
	if requests.Load() != 0 {
		t.Fatal("invalid product made HTTP/index mutations", requests.Load())
	}
}
