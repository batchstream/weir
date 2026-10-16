package mongodb

import (
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoQueuedReadCompletesBeforeUnrelatedSlowWrite(t *testing.T) {
	occupyDocument := bson.D{{Key: "_id", Value: "occupy"}, {Key: "n", Value: 1}}
	readDocument := bson.D{{Key: "_id", Value: "read"}, {Key: "n", Value: 1}}
	occupyCursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{occupyDocument}}}
	readCursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{readDocument}}}
	writeItem := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
	writeReply := programBulkResponse(bson.A{writeItem}, 1)
	responses := []bson.D{
		collectionQualificationResponse("db", "records"), readCursorResponse(occupyCursor),
		readCursorResponse(readCursor), writeReply,
	}
	occupyStarted := make(chan struct{})
	releaseOccupy := make(chan struct{})
	bulkStarted := make(chan struct{})
	releaseBulk := make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	monitor := &event.CommandMonitor{Started: func(caller context.Context, command *event.CommandStartedEvent) {
		switch command.CommandName {
		case "find":
			id, _ := command.Command.Lookup("filter").Document().Lookup("_id").StringValueOK()
			if id == "occupy" {
				close(occupyStarted)
				select {
				case <-releaseOccupy:
				case <-caller.Done():
				}
			}
		case "bulkWrite":
			close(bulkStarted)
			select {
			case <-releaseBulk:
			case <-caller.Done():
			}
		}
	}}
	adapter := batchMockAdapter(t, responses, monitor)
	limits := store.DefaultLimits()
	limits.MaxInFlight = 1
	limits.BatchWait = 0
	runtime, err := store.New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := runtime.Close(cleanup); err != nil {
			t.Error(err)
		}
	}()
	submit := func(caller context.Context, options batchOperationOptions) *store.Ticket {
		operation := batchOperation(t, options)
		record, err := execution.NewRecord("mongo", operation.Index, operation.Command)
		if err != nil {
			t.Fatal(err)
		}
		work, failure := runtime.PrepareRecord(caller, record)
		if failure != nil {
			t.Fatal(failure)
		}
		session := runtime.NewSession()
		t.Cleanup(session.Close)
		ticket, failure, _ := runtime.Submit(caller, work, session)
		if failure != nil {
			t.Fatal(failure)
		}
		return ticket
	}
	occupyOptions := batchOperationOptions{resource: "db/records/s:occupy", action: "read"}
	occupy := submit(ctx, occupyOptions)
	select {
	case <-occupyStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	readContext, stopRead := context.WithTimeout(ctx, time.Second)
	defer stopRead()
	readOptions := batchOperationOptions{resource: "db/records/s:read", action: "read"}
	read := submit(readContext, readOptions)
	writeDocument := bson.D{{Key: "_id", Value: "write"}, {Key: "n", Value: 2}}
	writeOptions := batchOperationOptions{resource: "db/records/s:write", action: "put", document: writeDocument}
	write := submit(ctx, writeOptions)
	if snapshot := runtime.Snapshot(); snapshot.Pending != 2 || snapshot.Active != 1 {
		t.Fatal("test did not queue both operations behind the occupied slot", snapshot)
	}
	close(releaseOccupy)
	if _, err := occupy.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	occupy.Ack()
	result, err := read.Wait(readContext)
	if err != nil || result.GetReadResult().GetDocument() == nil {
		t.Fatal("successful read was held by an unrelated same-collection write", result, err)
	}
	read.Ack()
	select {
	case <-bulkStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if snapshot := runtime.Snapshot(); snapshot.Active != 1 || snapshot.Ready != 0 || snapshot.Retained != 1 {
		t.Fatal("read completion retained write ownership or execution credit", snapshot)
	}
	close(releaseBulk)
	result, err = write.Wait(ctx)
	if err != nil || result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("independent write lost its confirmed outcome", result, err)
	}
	write.Ack()
}
