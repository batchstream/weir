package app

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	if cfg.Basic != expected {
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
		t.Fatal("routing file must validate without accessing its backend", err)
	}
	if len(cfg.Services) != 3 || len(cfg.Routes) != 3 {
		t.Fatal("routing file must declare MongoDB, Search and remote services and routes")
	}
	service := cfg.Services[0]
	if service.Name != "database" || service.Remote != nil || service.Local == nil ||
		service.Local.MongoDB == nil || service.Local.Search != nil {
		t.Fatal("routing file must declare a local MongoDB service")
	}
	limits := service.Local.runtimeLimits()
	if limits.Concurrency != 4 || limits.BatchOperations != 16 || limits.Collect != time.Millisecond {
		t.Fatal("local scheduler limits differ from their documented defaults")
	}
	if cfg.Routes[0].Store != "mongo" || cfg.Routes[0].Service != service.Name {
		t.Fatal("public MongoDB Store must target the declared service")
	}

	service = cfg.Services[1]
	if service.Name != "search" || service.Remote != nil || service.Local == nil ||
		service.Local.Search == nil || service.Local.MongoDB != nil {
		t.Fatal("routing file must declare a local Search service")
	}
	backend := service.Local.Search
	if !strings.HasPrefix(backend.URL, "https://") ||
		backend.Connection == nil || backend.Connection.Username != "weir" ||
		backend.Connection.Password != "change-me" || backend.Connection.CAFile != "/etc/weir/ca.pem" {
		t.Fatal("Search routing configuration must include the documented HTTPS connection fields")
	}
	if cfg.Routes[1].Store != "search" || cfg.Routes[1].Service != service.Name {
		t.Fatal("public Search Store must target the declared service")
	}

	service = cfg.Services[2]
	if service.Name != "upstream" || service.Local != nil || service.Remote == nil {
		t.Fatal("routing file must declare a remote service")
	}
	if cfg.Routes[2].Store != "remote" || cfg.Routes[2].Service != service.Name {
		t.Fatal("public remote Store must target the declared service")
	}
}
