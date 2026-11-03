package mongodb

import (
	"context"
	"sync/atomic"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoQualificationFailureIsNotCached(t *testing.T) {
	missing := collectionQualificationResponse("db", "records")
	cursor := missing[1].Value.(bson.D)
	cursor[2].Value = bson.A{}
	responses := []bson.D{missing, collectionQualificationResponse("db", "records")}
	var commands atomic.Int32
	monitor := &event.CommandMonitor{Started: func(_ context.Context, event *event.CommandStartedEvent) {
		commands.Add(1)
	}}
	adapter := batchMockAdapter(t, responses, monitor)
	target := namespace{database: "db", collection: "records"}
	if failure := adapter.qualifyTarget(context.Background(), target); failure.GetCode() != pb.FailureCode_TARGET_NOT_FOUND {
		t.Fatal("missing collection passed qualification", failure)
	}
	for range 2 {
		if failure := adapter.qualifyTarget(context.Background(), target); failure != nil {
			t.Fatal("failed metadata check poisoned retry", failure)
		}
	}
	if commands.Load() != 2 {
		t.Fatal("failure was cached or successful retry was rechecked", commands.Load())
	}
}

func TestMongoQualificationCacheIncludesDatabase(t *testing.T) {
	responses := []bson.D{
		collectionQualificationResponse("first", "records"),
		collectionQualificationResponse("second", "records"),
	}
	var databases []string
	monitor := &event.CommandMonitor{Started: func(_ context.Context, event *event.CommandStartedEvent) {
		databases = append(databases, event.DatabaseName)
	}}
	adapter := batchMockAdapter(t, responses, monitor)
	for range 2 {
		for _, database := range []string{"first", "second"} {
			target := namespace{database: database, collection: "records"}
			if failure := adapter.qualifyTarget(context.Background(), target); failure != nil {
				t.Fatal(failure)
			}
		}
	}
	if len(databases) != 2 || databases[0] != "first" || databases[1] != "second" {
		t.Fatal("same collection name crossed database cache keys", databases)
	}
}

func TestMongoCanceledMetadataCheckIsNotCached(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var commands atomic.Int32
	monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, event *event.CommandSucceededEvent) {
		if commands.Add(1) == 1 {
			cancel()
		}
	}}
	responses := []bson.D{
		collectionQualificationResponse("db", "records"),
		collectionQualificationResponse("db", "records"),
	}
	adapter := batchMockAdapter(t, responses, monitor)
	target := namespace{database: "db", collection: "records"}
	if failure := adapter.qualifyTarget(ctx, target); failure.GetCode() != pb.FailureCode_CANCELLED {
		t.Fatal("canceled metadata check returned success", failure)
	}
	if failure := adapter.qualifyTarget(context.Background(), target); failure != nil {
		t.Fatal(failure)
	}
	if commands.Load() != 2 {
		t.Fatal("canceled metadata check was cached", commands.Load())
	}
	if failure := adapter.qualifyTarget(ctx, target); failure.GetCode() != pb.FailureCode_CANCELLED || commands.Load() != 2 {
		t.Fatal("hot metadata cache ignored cancellation", failure, commands.Load())
	}
}
