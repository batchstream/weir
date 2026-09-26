//go:build integration

// Package testmongo is explicit opt-in test infrastructure, never linked into Weir.
package testmongo

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const URI = "mongodb://127.0.0.1:27028/?directConnection=true&serverMonitoringMode=poll"

var sequence atomic.Uint64

func Open(t *testing.T) (*mongo.Client, string) {
	t.Helper()
	if os.Getenv("WEIR_INTEGRATION") != "1" {
		t.Fatal("integration requires WEIR_INTEGRATION=1 and scripts/mongo-local.sh start; not silently skipped")
	}
	opts := options.Client().ApplyURI(URI).SetRetryReads(false).SetRetryWrites(false).SetMaxAdaptiveRetries(0).SetMaxPoolSize(8).SetServerSelectionTimeout(time.Second)
	client, err := mongo.Connect(opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := bson.D{{Key: "hello", Value: 1}}
	var hello struct {
		Set string `bson:"setName"`
	}
	if err := client.Database("admin").RunCommand(ctx, cmd).Decode(&hello); err != nil || hello.Set != "weir_m1" {
		_ = client.Disconnect(ctx)
		t.Fatalf("wrong/missing isolated replica set: %+v %v", hello, err)
	}
	db := fmt.Sprintf("weir_test_%d_%d", os.Getpid(), sequence.Add(1))
	if err := client.Database(db).CreateCollection(ctx, "records"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := client.Database(db).Drop(cleanup); err != nil {
			t.Error(err)
		}
		if err := client.Disconnect(cleanup); err != nil {
			t.Error(err)
		}
	})
	return client, db
}
func FailCommand(t *testing.T, client *mongo.Client, data bson.D, times int) {
	t.Helper()
	mode := bson.D{{Key: "times", Value: times}}
	cmd := bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: mode}, {Key: "data", Value: data}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Database("admin").RunCommand(ctx, cmd).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		off := bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: "off"}}
		_ = client.Database("admin").RunCommand(ctx, off).Err()
	})
}
