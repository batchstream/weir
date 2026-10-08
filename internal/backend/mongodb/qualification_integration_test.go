//go:build integration

package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil/testmongo"
)

func TestMongoOpenDoesNotQueryServerVersion(t *testing.T) {
	fixture := testmongo.Open(t)
	proxy := testmongo.StartProxy(t, fixture)
	cfg := Config{URI: proxy.URI(), Store: "mongo"}
	cfg = mongoFixtureConfig(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	adapter, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()

	hello := false
	for _, wireEvent := range proxy.Events() {
		if wireEvent.Command == "buildInfo" {
			t.Fatal("production Open queried the server version")
		}
		if wireEvent.Command == "hello" && wireEvent.Acknowledged {
			hello = true
		}
	}
	if !hello {
		t.Fatal("production Open did not qualify the replica-set topology")
	}
}
