package app

import (
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestWorkingMemoryParticipatesInProcessAdmission(t *testing.T) {
	raw := "stores:\n  - name: records\n    search:\n      url: http://127.0.0.1:9200\n    max_concurrency: 32\n    working_memory: 3GiB\n"
	routing, err := DecodeRouting(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	cfg := emptyConfig(t)
	cfg.Routing = routing
	cfg.Basic.Transport.MaxSessions = 64
	cfg.Basic.Transport.MaxConnections = 64
	cfg.Basic.Memory = 4 << 30
	if err := cfg.Validate(); err == nil {
		t.Fatal("4GiB accepted despite 64 stream windows and the 3GiB backend workspace")
	}
	cfg.Basic.Memory = 8 << 30
	if err := cfg.Validate(); err != nil {
		t.Fatal("bounded stream windows and backend workspace fit within 8GiB", err)
	}
	limits := routing.Stores[0].Local.runtimeLimits()
	if limits.WorkingBytes != 3<<30 || limits.Concurrency != 32 {
		t.Fatal("configured workspace did not reach the Store runtime", limits)
	}
	encoded, err := yaml.Marshal(routing)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRouting(strings.NewReader(string(encoded)))
	if err != nil || decoded.Stores[0].Local.runtimeLimits().WorkingBytes != limits.WorkingBytes {
		t.Fatal("workspace changed during configuration round trip", err)
	}
}

func TestWorkingMemoryRejectsInvalidDeclarations(t *testing.T) {
	prefix := "stores:\n  - name: records\n    mongodb:\n      uri: mongodb://127.0.0.1:27017\n"
	for _, value := range []string{"0B", "23MiB", "18446744073709551615B", "null", "3072", "{}"} {
		input := prefix + "    working_memory: " + value + "\n"
		if _, err := DecodeRouting(strings.NewReader(input)); err == nil {
			t.Fatal("invalid workspace accepted", value)
		}
	}
	defaults, err := DecodeRouting(strings.NewReader(prefix))
	if err != nil || defaults.Stores[0].Local.runtimeLimits().WorkingBytes != 384<<20 {
		t.Fatal("default workspace changed", err)
	}
}

func TestWorkingMemoryCannotWrapTheProcessBudget(t *testing.T) {
	raw := "stores:\n  - name: first\n    mongodb:\n      uri: mongodb://127.0.0.1:27017\n    working_memory: 9223372036854775807B\n  - name: second\n    search:\n      url: http://127.0.0.1:9200\n    working_memory: 9223372036854775807B\n"
	routing, err := DecodeRouting(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	cfg := emptyConfig(t)
	cfg.Routing = routing
	cfg.Basic.Transport.MaxSessions = 64
	cfg.Basic.Transport.MaxConnections = 64
	cfg.Basic.Memory = 8 << 30
	if cfg.ReservedMemory() != (64<<30)+1 {
		t.Fatal("unrepresentable Store budgets did not saturate the memory calculation", cfg.ReservedMemory())
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("wrapped backend memory bypassed process admission")
	}
}
