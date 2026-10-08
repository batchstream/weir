//go:build integration

package app

import (
	"context"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestPartialStartupReleasesConstructedMongo(t *testing.T) {
	backend := testmongo.Open(t)
	native := backend.Admin
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	count := func() int {
		command := bson.D{{Key: "serverStatus", Value: 1}}
		var reply struct{ Connections struct{ Current int } }
		if err := native.Database("admin").RunCommand(ctx, command).Decode(&reply); err != nil {
			t.Fatal(err)
		}
		return reply.Connections.Current
	}
	before := count()
	mongo := mongoFixtureConfig(t, backend.URI)
	// The second valid static configuration fails only after Mongo opens.
	search := &Search{URL: "http://127.0.0.1:1"}
	first := &Local{Backend: BackendConfig{MongoDB: mongo}}
	second := &Local{Backend: BackendConfig{Search: search}}
	mongoService := StoreConfig{Name: "mongo", Local: first}
	searchService := StoreConfig{Name: "search", Local: second}

	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	cfg.Routing.Stores = []StoreConfig{mongoService, searchService}

	for range 5 {
		if routes, err := Open(ctx, cfg); err == nil || routes != nil {
			t.Fatal("partial startup served routes")
		}
	}
	deadline := time.Now().Add(time.Second)
	after := count()
	for after > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		after = count()
	}
	if after > before {
		t.Fatal("partial startup leaked MongoDB sockets", before, after)
	}
	t.Log("real backend connections before/after partial startups", before, after)
}
