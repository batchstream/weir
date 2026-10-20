package app

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil"
)

func TestBasicConfigurationFile(t *testing.T) {
	filename := filepath.Join(testutil.Root(t), "config", "weir.reference.yaml")
	cfg, err := Load(filename, "")
	if err != nil {
		t.Fatal("basic configuration must load without routing", err)
	}

	expected := DefaultConfig().Basic.resolved()
	expected.Listeners.Application = "127.0.0.1:7447"
	expected.Diagnostics.Address = "127.0.0.1:7449"
	if !reflect.DeepEqual(cfg.Basic, expected) {
		t.Fatal("basic configuration differs from its documented defaults and addresses")
	}
}

func TestRoutingConfigurationFile(t *testing.T) {
	filename := filepath.Join(testutil.Root(t), "config", "routes.reference.yaml")
	raw, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := DecodeRouting(bytes.NewReader(raw))
	if err != nil {
		t.Fatal("Store file must validate without backend IO", err)
	}
	if len(cfg.Stores) != 2 {
		t.Fatal("reference must declare MongoDB and Search Stores")
	}
	for _, definition := range cfg.Stores {
		limits := definition.runtimeLimits()
		if limits.BatchOperations != 128 || limits.BatchBytes != 4<<20 || limits.MaxInFlight != 16 || limits.BatchWait != 20*time.Millisecond {
			t.Fatal("documented scheduler defaults differ")
		}
		if limits.QueueOperations != 1024 || limits.QueueBytes != 32<<20 {
			t.Fatal("documented working memory or backend timeout differs")
		}
		if definition.Name == "mongo" {
			if definition.Backend.MongoDB == nil || definition.Backend.Search != nil {
				t.Fatal("invalid documented Mongo Store")
			}
		} else if definition.Name == "search" {
			if definition.Backend.Search == nil || definition.Backend.MongoDB != nil {
				t.Fatal("invalid documented Search Store")
			}
			backend := definition.Backend.Search
			if !strings.HasPrefix(backend.URL, "https://") || definition.Backend.Authentication == nil || definition.Backend.Authentication.Username != "weir" || definition.Backend.Authentication.Password != "change-me" || definition.Backend.TLS.CAFile != "/etc/weir/ca.pem" {
				t.Fatal("documented Search HTTPS fields differ")
			}
		} else {
			t.Fatal("unexpected local Store")
		}
	}
}

func TestRoutingMongoReplicaProfiles(t *testing.T) {
	for _, uri := range []string{
		"mongodb://mongo-a.invalid:27017/?replicaSet=orders",
		"mongodb://mongo-a.invalid:27017,mongo-b.invalid:27018/?replicaSet=orders",
		"mongodb://[::1]:27017/?replicaSet=orders",
		"mongodb://[::1]:27017,[::2]:27018/?replicaSet=orders",
		"mongodb://mongo-a.invalid:27017,[::1]:27018/?replicaSet=orders",
		"mongodb://[::1]:27017,mongo-a.invalid:27018,[::2]:27019/?replicaSet=orders",
		"mongodb://[::1]:27017/?directConnection=true",
		"mongodb://mongo-a.invalid:27017/",
		"mongodb://[::1]:27017,mongo-b.invalid:27018/?replicaSet=orders&directConnection=false",
	} {
		input := "stores:\n  - name: mongo\n    backend:\n      mongodb:\n        uri: " + uri + "\n"
		cfg, err := DecodeRouting(strings.NewReader(input))
		if err != nil || len(cfg.Stores) != 1 || cfg.Stores[0].Backend.MongoDB.URI != uri {
			t.Fatal("documented discovery profile must decode without DNS or backend IO", err)
		}
		full := emptyConfig(t)
		full.Routing = cfg
		if err := full.Validate(); err != nil {
			t.Fatal("full connection profile validation must remain pure", err)
		}
		basic, routing := writeCredentialTestDocuments(t, full)
		loaded, err := Load(basic, routing)
		if err != nil || loaded.Routing.Stores[0].Backend.MongoDB.URI != uri {
			t.Fatal("Load must use the same URI grammar without backend IO", err)
		}
	}
	for _, uri := range []string{
		"mongodb://mongo-a.invalid:27017,mongo-b.invalid:27018/",
		"mongodb://mongo-a.invalid:27017/?replicaSet=orders&directConnection=true",
		"mongodb://mongo-a.invalid:27017/?replicaSet=orders&readPreference=secondary",
	} {
		input := "stores:\n  - name: mongo\n    backend:\n      mongodb:\n        uri: " + uri + "\n"
		if _, err := DecodeRouting(strings.NewReader(input)); err == nil {
			t.Fatal("ambiguous or unsupported discovery profile accepted")
		}
	}
}
