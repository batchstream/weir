package app

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/batchstream/weir/internal/testutil"
)

func TestBasicConfigurationFile(t *testing.T) {
	filename := filepath.Join(testutil.Root(t), "config", "weir.yaml")
	cfg, err := Load(filename, "")
	if err != nil {
		t.Fatal("basic configuration must load without routing", err)
	}

	expected := DefaultConfig().Basic
	expected.Listeners.Application = "127.0.0.1:7447"
	expected.Diagnostics.Address = "127.0.0.1:7449"
	if !reflect.DeepEqual(cfg.Basic, expected) {
		t.Fatal("basic configuration differs from its documented defaults and addresses")
	}
}

func TestRoutingConfigurationFile(t *testing.T) {
	filename := filepath.Join(testutil.Root(t), "config", "routes.yaml")
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
		if limits.Concurrency != 2 || limits.BatchOperations != 32 {
			t.Fatal("documented scheduler defaults differ")
		}
		if definition.Name == "mongo" {
			if definition.MongoDB == nil || definition.Search != nil || definition.mongoConfig("mongo").MaxReadSize != 16<<10 {
				t.Fatal("invalid documented Mongo Store")
			}
		} else if definition.Name == "search" {
			if definition.Search == nil || definition.MongoDB != nil || definition.searchConfig("search").MaxReadSize != 16<<10 {
				t.Fatal("invalid documented Search Store")
			}
			backend := definition.Search
			if !strings.HasPrefix(backend.URL, "https://") || backend.Connection == nil || backend.Connection.Username != "weir" || backend.Connection.Password != "change-me" || backend.Connection.CAFile != "/etc/weir/ca.pem" {
				t.Fatal("documented Search HTTPS fields differ")
			}
		} else {
			t.Fatal("unexpected local Store")
		}
	}
}
