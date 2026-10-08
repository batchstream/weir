package app

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

const queueRoutingPrefix = "stores:\n  - name: records\n    mongodb:\n      uri: mongodb://127.0.0.1:27017/?directConnection=true\n"

func TestBatchAndQueueConfigurationRoundTrip(t *testing.T) {
	input := queueRoutingPrefix + "    max_batch_operations: 128\n    max_batch_bytes: 16MiB\n    batch_queue:\n      max_operations: 4096\n      max_bytes: 256MiB\n"
	routing, err := DecodeRouting(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	limits := routing.Stores[0].runtimeLimits()
	if limits.BatchOperations != 128 || limits.BatchBytes != 16<<20 || limits.QueueOperations != 4096 || limits.QueueBytes != 256<<20 {
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
		"    batch_queue:\n      max_operations: -1\n",
		"    batch_queue:\n      max_operations: '16'\n",
		"    batch_queue:\n      max_bytes: 0B\n",
		"    batch_queue:\n      max_bytes: 9223372036854775808B\n",
		"    max_batch_bytes: 0B\n",
		"    max_batch_operations: -1\n",
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
