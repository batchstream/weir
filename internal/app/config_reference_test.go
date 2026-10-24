package app

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil"
)

func TestReferenceConfigurationFile(t *testing.T) {
	filename := filepath.Join(testutil.Root(t), "config", "weir.reference.yaml")
	cfg, err := Load(filename)
	if err != nil {
		t.Fatal("reference configuration must load without backend IO", err)
	}
	if cfg.Listeners.Application != "127.0.0.1:7447" || cfg.Listeners.Diagnostics != "127.0.0.1:7449" || cfg.shutdownTimeout() != defaultShutdownTimeout {
		t.Fatal("reference listeners or shutdown timeout differ from the documented values", cfg)
	}
	if len(cfg.Stores) != 2 {
		t.Fatal("reference must declare MongoDB and Search Stores")
	}
	mongo, search := cfg.Stores[0], cfg.Stores[1]
	if mongo.MongoDB == nil || mongo.Search != nil || mongo.runtimeLimits() != store.DefaultLimits() {
		t.Fatal("documented MongoDB Store or batching defaults differ")
	}
	authentication := search.Authentication
	if search.Search == nil || !strings.HasPrefix(search.Search.URL, "https://") || authentication == nil ||
		authentication.Username != "weir" || authentication.Password != "change-me" || search.TLS.CAFile != "/etc/weir/ca.pem" {
		t.Fatal("documented Search HTTPS fields differ")
	}
}

func TestMongoReplicaProfiles(t *testing.T) {
	prefix := "listeners:\n  application: 127.0.0.1:0\nstores:\n  - name: mongo\n    mongodb:\n      uri: "
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
		cfg, err := Decode(strings.NewReader(prefix + uri + "\n"))
		if err != nil || cfg.Stores[0].MongoDB.URI != uri {
			t.Fatal("documented discovery profile must decode without DNS or backend IO", uri, err)
		}
		loaded, err := Load(writeCredentialTestDocument(t, cfg))
		if err != nil || loaded.Stores[0].MongoDB.URI != uri {
			t.Fatal("Load must use the same URI grammar without backend IO", uri, err)
		}
	}
	for _, uri := range []string{
		"mongodb://mongo-a.invalid:27017,mongo-b.invalid:27018/",
		"mongodb://mongo-a.invalid:27017/?replicaSet=orders&directConnection=true",
		"mongodb://mongo-a.invalid:27017/?replicaSet=orders&readPreference=secondary",
	} {
		if _, err := Decode(strings.NewReader(prefix + uri + "\n")); err == nil {
			t.Fatal("ambiguous or unsupported discovery profile accepted", uri)
		}
	}
}
