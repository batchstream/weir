package mongodb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoConcurrentTargetQualificationUsesOneCommand(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var commands atomic.Int32
	monitor := &event.CommandMonitor{Started: func(_ context.Context, event *event.CommandStartedEvent) {
		if event.CommandName != "listCollections" {
			return
		}
		if commands.Add(1) == 1 {
			close(started)
			<-release
		}
	}}
	responses := []bson.D{collectionQualificationResponse("db", "records")}
	adapter := batchMockAdapter(t, responses, monitor)
	target := namespace{database: "db", collection: "records"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var workers sync.WaitGroup
	workers.Go(func() {
		if failure, signal := adapter.qualifyTarget(ctx, target); failure != nil || signal != execution.Healthy {
			t.Error(failure, signal)
		}
	})
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first qualification did not start")
	}
	for range 16 {
		workers.Go(func() {
			if failure, signal := adapter.qualifyTarget(ctx, target); failure != nil || signal != execution.Healthy {
				t.Error(failure, signal)
			}
		})
	}
	waiter, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	defer stop()
	if failure, signal := adapter.qualifyTarget(waiter, target); failure.GetCode() != pb.FailureCode_DEADLINE_EXCEEDED || signal != execution.Neutral {
		t.Fatal("metadata waiter ignored cancellation", failure, signal)
	}
	close(release)
	workers.Wait()
	if commands.Load() != 1 {
		t.Fatal("concurrent target qualification duplicated metadata I/O", commands.Load())
	}
}

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
	if failure, _ := adapter.qualifyTarget(context.Background(), target); failure.GetCode() != pb.FailureCode_PRECONDITION_FAILED {
		t.Fatal("missing collection passed qualification", failure)
	}
	for range 2 {
		if failure, _ := adapter.qualifyTarget(context.Background(), target); failure != nil {
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
			if failure, _ := adapter.qualifyTarget(context.Background(), target); failure != nil {
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
	if failure, signal := adapter.qualifyTarget(ctx, target); failure.GetCode() != pb.FailureCode_CANCELLED || signal != execution.Neutral {
		t.Fatal("canceled metadata check returned success", failure, signal)
	}
	if failure, _ := adapter.qualifyTarget(context.Background(), target); failure != nil {
		t.Fatal(failure)
	}
	if commands.Load() != 2 {
		t.Fatal("canceled metadata check was cached", commands.Load())
	}
	if failure, _ := adapter.qualifyTarget(ctx, target); failure.GetCode() != pb.FailureCode_CANCELLED || commands.Load() != 2 {
		t.Fatal("hot metadata cache ignored cancellation", failure, commands.Load())
	}
}
