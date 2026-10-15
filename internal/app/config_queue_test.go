package app

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/store"
	"go.yaml.in/yaml/v3"
)

const queueRoutingPrefix = "stores:\n  - name: records\n    backend:\n      mongodb:\n        uri: mongodb://127.0.0.1:27017/?directConnection=true\n"

func TestBatchingDefaultsAndExplicitZerosRemainVisible(t *testing.T) {
	for _, test := range []struct {
		name     string
		fragment string
		want     int
		wait     time.Duration
	}{
		{name: "omitted", want: store.DefaultLimits().MaxInFlight, wait: store.DefaultLimits().BatchWait},
		{name: "explicit immediate and unlimited", fragment: "    batching:\n      max_in_flight: 0\n      max_wait: 0s\n", want: 0, wait: 0},
		{name: "explicit finite", fragment: "    batching:\n      max_in_flight: 7\n      max_wait: 2ms\n", want: 7, wait: 2 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			routing, err := DecodeRouting(strings.NewReader(queueRoutingPrefix + test.fragment))
			if err != nil {
				t.Fatal(err)
			}
			local := routing.Stores[0].Local
			if local.Batching.MaxInFlight == nil || *local.Batching.MaxInFlight != test.want || local.runtimeLimits().MaxInFlight != test.want {
				t.Fatal("execution policy did not resolve", local.Batching.MaxInFlight)
			}
			if local.Batching.MaxWait == nil || time.Duration(*local.Batching.MaxWait) != test.wait || local.runtimeLimits().BatchWait != test.wait {
				t.Fatal("collection policy did not resolve", local.Batching.MaxWait)
			}
			resolved := routing.resolved()
			if resolved.Stores[0].Batching.MaxInFlight == local.Batching.MaxInFlight {
				t.Fatal("resolved execution policy aliases its input")
			}
			encoded, err := json.Marshal(resolved)
			if err != nil || !strings.Contains(string(encoded), `"max_in_flight":`) {
				t.Fatal("resolved execution policy was hidden", err)
			}
			decoded, err := DecodeRouting(strings.NewReader(string(encoded)))
			if err != nil || decoded.Stores[0].runtimeLimits().MaxInFlight != test.want || decoded.Stores[0].runtimeLimits().BatchWait != test.wait {
				t.Fatal("explicit execution policy changed on round trip", err)
			}
		})
	}
}

func TestBatchAndQueueConfigurationRoundTrip(t *testing.T) {
	input := queueRoutingPrefix + "    batching:\n      max_operations: 128\n      max_bytes: 16MiB\n      max_in_flight: 7\n      max_wait: 20ms\n    queue:\n      max_operations: 4096\n      max_bytes: 256MiB\n"
	routing, err := DecodeRouting(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	limits := routing.Stores[0].runtimeLimits()
	if limits.BatchOperations != 128 || limits.BatchBytes != 16<<20 || limits.MaxInFlight != 7 || limits.BatchWait != 20*time.Millisecond || limits.QueueOperations != 4096 || limits.QueueBytes != 256<<20 {
		t.Fatal(limits)
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
		if err != nil || !reflect.DeepEqual(decoded, routing) {
			t.Fatal("queue configuration changed on round trip", err)
		}
	}
}

func TestBatchQueueBoundsRejectInvalidConfiguration(t *testing.T) {
	for _, fragment := range []string{
		"    queue:\n      max_operations: -1\n",
		"    queue:\n      max_operations: '16'\n",
		"    queue:\n      max_bytes: 0B\n",
		"    queue:\n      max_bytes: 9223372036854775808B\n",
		"    batching:\n      max_bytes: 0B\n",
		"    batching:\n      max_operations: -1\n",
		"    batching:\n      max_in_flight: -1\n",
		"    batching:\n      max_in_flight: '4'\n",
		"    batching:\n      max_in_flight: null\n",
		"    batching:\n      max_wait: -1ms\n",
		"    batching:\n      max_wait: 20\n",
		"    batching:\n      max_wait: null\n",
	} {
		if _, err := DecodeRouting(strings.NewReader(queueRoutingPrefix + fragment)); err == nil {
			t.Fatal("invalid capacity accepted", fragment)
		}
	}
}

func TestRemovedResourceConfigurationHasNoCompatibilityAliases(t *testing.T) {
	basic := "listeners:\n  application: 127.0.0.1:0\n"
	for _, fragment := range []string{
		"memory: 2GiB\n",
		"transport:\n  max_connections: 16\n",
		"transport:\n  max_sessions: 4\n",
		"transport:\n  timeouts:\n    request: 15m\n",
	} {
		if _, err := DecodeBasic(strings.NewReader(basic + fragment)); err == nil {
			t.Fatal("removed process field accepted", fragment)
		}
	}
	for _, field := range []string{"max_concurrency: 2", "working_memory: 384MiB", "max_read_size: 16KiB", "backend_timeout: 2s"} {
		if _, err := DecodeRouting(strings.NewReader(queueRoutingPrefix + "    " + field + "\n")); err == nil {
			t.Fatal("removed Store field accepted", field)
		}
	}
}
