//go:build integration

package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil/testmongo"
)

// TestMongoOpenQualifiesTopologyWithHello checks that Open qualifies the
// deployment from the handshake alone: the driver's initial isMaster and the
// acknowledged hello.
func TestMongoOpenQualifiesTopologyWithHello(t *testing.T) {
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
	defer adapter.Close(context.Background())

	hello := false
	for _, wireEvent := range proxy.Events() {
		if wireEvent.Command != "hello" && wireEvent.Command != "isMaster" {
			t.Fatal("Open sent a command outside the handshake", wireEvent.Command)
		}
		if wireEvent.Command == "hello" && wireEvent.Acknowledged {
			hello = true
		}
	}
	if !hello {
		t.Fatal("Open did not qualify the replica-set topology with an acknowledged hello")
	}
}
