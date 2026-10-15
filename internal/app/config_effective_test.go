package app

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iceisfun/golua/vm"
	"go.yaml.in/yaml/v3"
)

func TestEffectiveConfigurationDefaultsAndRedaction(t *testing.T) {
	cfg := credentialTestConfig(t, "search")
	cfg.Routing.Stores[0].Backend.Authentication.Username = "username-secret-sentinel"
	cfg.Routing.Stores[0].Backend.Authentication.Password = "password-secret-sentinel"
	trust := &BackendTLS{CAFile: "/missing/ca-secret-sentinel.pem"}
	cfg.Routing.Stores[0].Backend.TLS = trust
	mongo := &Mongo{URI: "mongodb://127.0.0.1:27017"}
	backend := BackendConfig{MongoDB: mongo}
	local := &Local{Backend: backend}
	definition := StoreConfig{Name: "mongo", Local: local}
	cfg.Routing.Stores = append(cfg.Routing.Stores, definition)
	before, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	report, err := cfg.Effective()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := yaml.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "secret-sentinel") || !strings.Contains(string(raw), "<redacted>") {
		t.Fatal("effective configuration disclosed a credential or trust path")
	}
	if report.Basic.Lua.MaxCallDepth != vm.DefaultMaxCallDepth || report.Basic.Lua.MaxStackSlots != vm.DefaultMaxStackSlots {
		t.Fatal("effective configuration hid native Lua defaults", report.Basic.Lua)
	}
	if report.Basic.Lua.MaxInstructions != 0 || report.Automatic["memory_capacity"] == "" || report.Automatic["discovery.group"] == "" || report.Automatic["discovery.advertise"] == "" {
		t.Fatal("effective configuration lost unbounded or automatic policy")
	}
	for _, store := range report.Routing.Stores {
		if store.Queue.MaxOperations != 1024 || *store.Queue.MaxBytes != 32<<20 || store.Batching.MaxOperations != 32 || *store.Batching.MaxBytes != 8<<20 {
			t.Fatal("scheduler defaults missing", store.Name)
		}
		if store.Scan.MaxBatchDocuments != 128 || *store.Scan.MaxBatchBytes != 4<<20 || *store.Backend.ConnectTimeout != Duration(2*time.Second) || *store.Backend.MaxExchangeBytes != 32<<20 {
			t.Fatal("backend and scan defaults missing", store.Name)
		}
		if store.Backend.MongoDB != nil && (store.Backend.MongoDB.MaxConnecting != 2 || *store.Backend.MongoDB.MetadataCacheEntries != 64 || store.Backend.MongoDB.RetryAttempts != 5 || *store.Backend.MongoDB.RetryBackoff != Duration(5*time.Millisecond)) {
			t.Fatal("MongoDB defaults missing")
		}
		if store.Backend.Search != nil && (store.Backend.Search.RetryAttempts != 5 || *store.Backend.Search.PITKeepAlive != Duration(time.Minute) || *store.Backend.Search.IdleTimeout != Duration(30*time.Second)) {
			t.Fatal("ineffective Search cache setting was reported")
		}
	}
	after, err := yaml.Marshal(cfg)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("effective inspection changed live configuration", err)
	}
}

func TestEffectiveConfigurationPreservesExplicitPolicy(t *testing.T) {
	cfg := credentialTestConfig(t, "mongodb")
	cfg.Basic.Lua.MaxInstructions = 1234
	cfg.Basic.Lua.MaxCallDepth = 12
	cfg.Basic.Lua.MaxStackSlots = 3456
	local := cfg.Routing.Stores[0].Local
	connectTimeout := Duration(17 * time.Millisecond)
	queueBytes := ByteSize(16 << 20)
	entries := 0
	local.Backend.ConnectTimeout = &connectTimeout
	local.Backend.MongoDB.MetadataCacheEntries = &entries
	local.Backend.MongoDB.MaxConnecting = 7
	local.Queue.MaxOperations = 77
	local.Queue.MaxBytes = &queueBytes
	report, err := cfg.Effective()
	if err != nil {
		t.Fatal(err)
	}
	resolved := report.Routing.Stores[0].Local
	if report.Basic.Lua != cfg.Basic.Lua || report.Automatic["lua.native_defaults"] == "" {
		t.Fatal("inspection replaced explicit Lua policy")
	}
	if *resolved.Backend.ConnectTimeout != connectTimeout || *resolved.Backend.MongoDB.MetadataCacheEntries != 0 || resolved.Backend.MongoDB.MaxConnecting != 7 || resolved.Queue.MaxOperations != 77 || *resolved.Queue.MaxBytes != queueBytes {
		t.Fatal("inspection replaced explicit Store policy")
	}
}

func TestEffectiveConfigurationRejectsUnvalidatedInput(t *testing.T) {
	cfg := credentialTestConfig(t, "search")
	cfg.Routing.Stores[0].Backend.Search.URL = "https://user:secret-sentinel@127.0.0.1:9200"
	report, err := cfg.Effective()
	if err == nil || strings.Contains(err.Error(), "sentinel") || report.Routing.Stores != nil {
		t.Fatal("inspection accepted or disclosed invalid credential-bearing URLs", err)
	}
}

func TestSearchRejectsInapplicableMetadataCacheSetting(t *testing.T) {
	for _, value := range []string{"0", "64"} {
		input := "stores:\n  - name: search\n    backend:\n      search:\n        url: http://127.0.0.1:9200\n      metadata_cache_entries: " + value + "\n"
		_, err := DecodeRouting(strings.NewReader(input))
		if err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatal("ineffective Search metadata cache setting accepted", value, err)
		}
	}
}

func TestRemovedLuaVMWrapperIsRejected(t *testing.T) {
	input := "listeners:\n  application: 127.0.0.1:0\nlua:\n  vm:\n    max_instructions: 100\n"
	if _, err := DecodeBasic(strings.NewReader(input)); err == nil {
		t.Fatal("removed Lua VM wrapper was accepted")
	}
}
