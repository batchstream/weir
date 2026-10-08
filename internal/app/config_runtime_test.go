package app

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestStructuredRuntimeConfigurationReachesAdaptersAndScheduler(t *testing.T) {
	input := `stores:
  - name: search
    backend:
      search:
        url: http://127.0.0.1:9200
      connect_timeout: 17ms
      metadata_cache_entries: 0
    batching:
      max_operations: 256
      max_bytes: 64MiB
      max_exchange_bytes: 16MiB
      queue:
        max_operations: 8192
        max_bytes: 256MiB
    streaming:
      max_pending_records: 96
      scan:
        max_batch_documents: 17
        max_batch_bytes: 8MiB
    lua:
      vm:
        max_instructions: 9000000
        max_call_depth: 512
        max_stack_slots: 65536
      values:
        max_bytes: 1MiB
        max_depth: 64
        max_nodes: 32768
`
	cfg, err := DecodeRouting(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	local := cfg.Stores[0].Local
	native := local.searchConfig("search")
	settings := *native.Options
	if settings.ConnectTimeout != 17*time.Millisecond || settings.MetadataCacheEntries != 0 || settings.ExchangeBytes != 16<<20 || settings.Scan.Documents != 17 || settings.Scan.Bytes != 8<<20 || settings.Lua.MaxInstructions != 9_000_000 || settings.Lua.MaxCallDepth != 512 || settings.Lua.MaxStackSlots != 65536 || settings.Lua.Values.MaxDepth != 64 || settings.Lua.Values.MaxNodes != 32768 || settings.Lua.Values.MaxBytes != 1<<20 {
		t.Fatal(settings)
	}
	limits := local.runtimeLimits()
	if limits.BatchOperations != 256 || limits.BatchBytes != 64<<20 || limits.QueueOperations != 8192 || limits.QueueBytes != 256<<20 || limits.RecordWindow != 96 || limits.Scan != settings.Scan {
		t.Fatal(limits)
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRouting(strings.NewReader(string(encoded)))
	if err != nil || !reflect.DeepEqual(decoded, cfg) {
		t.Fatal("structured runtime configuration did not round trip", err)
	}
	for _, old := range []string{"max_batch_operations: 4", "max_batch_bytes: 4MiB", "batch_queue: {}", "mongodb: {}", "search: {}"} {
		if _, err := DecodeRouting(strings.NewReader(queueRoutingPrefix + "    " + old + "\n")); err == nil {
			t.Fatal("old field accepted", old)
		}
	}
}

func TestProcessTimeoutAndMemoryPolicyConfiguration(t *testing.T) {
	input := `listeners:
  application: 127.0.0.1:0
transport:
  timeouts:
    handshake: 3s
    idle: 0s
    stall: 2m
lifecycle:
  startup_timeout: 30s
  shutdown_timeout: 20s
diagnostics:
  timeout: 5s
overload:
  memory:
    high_watermark: 95
    low_watermark: 90
    sample_interval: 7ms
`
	cfg, err := DecodeBasic(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	transport := cfg.Transport.serverLimits()
	memory := cfg.Overload.Memory.limits()
	if transport.Handshake != 3*time.Second || transport.Idle != 0 || transport.Stall != 2*time.Minute || cfg.Lifecycle.ShutdownTimeout != Duration(20*time.Second) || cfg.Diagnostics.Timeout != Duration(5*time.Second) || memory.HighWatermark != 95 || memory.LowWatermark != 90 || memory.SampleInterval != 7*time.Millisecond {
		t.Fatal(cfg)
	}
	for _, fragment := range []string{
		"overload:\n  memory:\n    high_watermark: 0\n",
		"overload:\n  memory:\n    low_watermark: 90\n",
		"overload:\n  memory:\n    sample_interval: 0s\n",
		"lifecycle:\n  shutdown_timeout: 0s\n",
		"transport:\n  timeouts:\n    handshake: 0s\n",
	} {
		if _, err := DecodeBasic(strings.NewReader("listeners:\n  application: 127.0.0.1:0\n" + fragment)); err == nil {
			t.Fatal("invalid runtime policy accepted", fragment)
		}
	}
}

func TestMongoConnectionWorkerConfiguration(t *testing.T) {
	input := `stores:
  - name: mongo
    backend:
      mongodb:
        uri: mongodb://127.0.0.1:27017
        pool:
          max_connecting: 32
`
	cfg, err := DecodeRouting(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	native := cfg.Stores[0].Local.mongoConfig("mongo")
	if native.MaxConnecting != 32 {
		t.Fatal("connection worker configuration not propagated", native.MaxConnecting)
	}
	for _, invalid := range []string{"-1", "18446744073709551615", "two"} {
		if _, err := DecodeRouting(strings.NewReader(strings.Replace(input, "32", invalid, 1))); err == nil {
			t.Fatal("invalid worker count accepted", invalid)
		}
	}
}
