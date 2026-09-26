//go:build integration

package app

import (
	"context"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/mongostore"
	"github.com/batchstream/weir/internal/searchstore"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestPartialStartupReleasesConstructedMongo(t *testing.T) {
	native, db := testmongo.Open(t)
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
	mongo := mongostore.Config{URI: testmongo.URI, Store: "mongo", Database: db, Collection: "records"}
	// This fails only after a real MongoDB adapter/pool and runtime were opened.
	search := &searchstore.Config{Store: "search", URL: "http://127.0.0.1:19200", Index: "records", Profile: "not-qualified"}
	cfg := Config{Mongo: mongo, Search: search, Limits: store.DefaultLimits()}
	for range 5 {
		if routes, err := OpenStores(ctx, cfg); err == nil || routes != nil {
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
