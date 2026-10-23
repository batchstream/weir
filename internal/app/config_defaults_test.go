package app

import (
	"reflect"
	"strings"
	"testing"
)

func TestConfigurationResolutionIsIdempotentAndIndependent(t *testing.T) {
	cfg := credentialTestConfig(t, "mongodb")
	zeroEntries := 0
	zeroBackoff := Duration(0)
	local := cfg.Routing.Stores[0].Local
	local.Backend.MongoDB.MetadataCacheEntries = &zeroEntries
	local.Backend.MongoDB.RetryBackoff = &zeroBackoff
	resolved, err := cfg.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	again, err := resolved.Resolve()
	if err != nil || !reflect.DeepEqual(again, resolved) {
		t.Fatal("resolving an effective configuration changed it", err)
	}
	if local.Queue.MaxBytes != nil || local.Backend.ConnectTimeout != nil || local.Backend.MongoDB.MaxConnecting != 0 {
		t.Fatal("resolution changed sparse input")
	}
	actual := resolved.Routing.Stores[0].Local
	if *actual.Backend.MongoDB.MetadataCacheEntries != 0 || *actual.Backend.MongoDB.RetryBackoff != 0 {
		t.Fatal("explicit zero policy was replaced by a default")
	}
	*actual.Queue.MaxBytes = 1
	*actual.Backend.MongoDB.MetadataCacheEntries = 7
	actual.Backend.Authentication.Password = "changed"
	if *again.Routing.Stores[0].Queue.MaxBytes == 1 || zeroEntries != 0 || cfg.Routing.Stores[0].Backend.Authentication.Password == "changed" {
		t.Fatal("resolved configurations share mutable input state")
	}
}

func TestFileAndProgrammaticConfigurationUseSameDefaults(t *testing.T) {
	input := "stores:\n  - name: mongo\n    backend:\n      mongodb:\n        uri: mongodb://127.0.0.1:27017\n"
	decoded, err := DecodeRouting(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	mongo := &Mongo{URI: "mongodb://127.0.0.1:27017"}
	backend := BackendConfig{MongoDB: mongo}
	local := &Local{Backend: backend}
	definition := StoreConfig{Name: "mongo", Local: local}
	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	cfg.Routing.Stores = []StoreConfig{definition}
	resolved, err := cfg.Resolve()
	if err != nil || !reflect.DeepEqual(decoded, resolved.Routing) {
		t.Fatal("file and programmatic defaults diverged", err)
	}
}

func TestAdapterSchemaRejectsCrossAdapterFields(t *testing.T) {
	for _, fragment := range []string{
		"      metadata_cache_entries: 64\n      mongodb:\n        uri: mongodb://127.0.0.1:27017\n",
		"      search:\n        url: http://127.0.0.1:9200\n        metadata_cache_entries: 64\n",
	} {
		input := "stores:\n  - name: example\n    backend:\n" + fragment
		if _, err := DecodeRouting(strings.NewReader(input)); err == nil {
			t.Fatal("unknown or inapplicable adapter field was accepted", fragment)
		}
	}
}
