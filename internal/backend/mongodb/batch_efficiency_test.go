package mongodb

import (
	"context"
	"reflect"
	"strings"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoMixedBatchRetainsQualificationAcrossExecutions(t *testing.T) {
	document := bson.D{{Key: "_id", Value: "read"}}
	readCursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document}}}
	item := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
	writeCursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: bson.A{item}}}
	writeReply := bson.D{
		{Key: "ok", Value: 1}, {Key: "cursor", Value: writeCursor},
		{Key: "nErrors", Value: 0}, {Key: "nInserted", Value: 0},
		{Key: "nDeleted", Value: 0}, {Key: "nMatched", Value: 1},
		{Key: "nModified", Value: 1}, {Key: "nUpserted", Value: 0},
	}
	responses := []bson.D{
		collectionQualificationResponse("db", "records"), readCursorResponse(readCursor), writeReply,
		readCursorResponse(readCursor), writeReply,
	}
	var commands []string
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		commands = append(commands, e.CommandName)
	}}
	adapter := batchMockAdapter(t, responses, monitor)
	readOptions := batchOperationOptions{resource: "weir://mongo/db/records/s:read", action: "read", index: 9}
	writeDocument := bson.D{{Key: "_id", Value: "write"}}
	writeOptions := batchOperationOptions{resource: "weir://mongo/db/records/s:write", action: "put", index: 8, document: writeDocument}
	var plans []*execution.Plan
	for _, opts := range []batchOperationOptions{writeOptions, readOptions} {
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	for range 2 {
		results, signal := adapter.executeRecords(context.Background(), plans)
		if signal != execution.Healthy || results[0].Index != 8 || results[0].GetMutation().Outcome != pb.MutationOutcome_APPLIED || results[1].Index != 9 || results[1].GetRead().GetDocument() == nil {
			t.Fatal("mixed batch lost result correspondence or acknowledgement", results, signal)
		}
	}
	want := []string{"listCollections", "find", "bulkWrite", "find", "bulkWrite"}
	if !reflect.DeepEqual(commands, want) {
		t.Fatal("hot batch repeated metadata I/O or lost business commands", commands)
	}
}

func TestMongoPointReadUsesEqualityForOneUniqueID(t *testing.T) {
	for _, ids := range [][]string{{"a"}, {"a", "a"}, {"a", "b"}} {
		t.Run(strings.Join(ids, "_"), func(t *testing.T) {
			var filter bson.RawValue
			monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
				if e.CommandName == "find" {
					filter = e.Command.Lookup("filter").Document().Lookup("_id")
				}
			}}
			documents := bson.A{bson.D{{Key: "_id", Value: "a"}}}
			if ids[len(ids)-1] == "b" {
				document := bson.D{{Key: "_id", Value: "b"}}
				documents = append(documents, document)
			}
			cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: documents}}
			responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor)}
			adapter := batchMockAdapter(t, responses, monitor)
			var plans []*execution.Plan
			for i, id := range ids {
				opts := batchOperationOptions{resource: "weir://mongo/db/records/s:" + id, action: "read", index: uint64(i + 1)}
				work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
				if failure != nil {
					t.Fatal(failure)
				}
				plans = append(plans, work)
			}
			results, signal := adapter.executeRecords(context.Background(), plans)
			for i, result := range results {
				if result.Index != uint64(i+1) || result.GetRead().GetDocument() == nil {
					t.Fatal("read or duplicate-ID correspondence lost", results)
				}
			}
			if signal != execution.Healthy {
				t.Fatal(signal)
			}
			if len(documents) == 1 {
				if id, ok := filter.StringValueOK(); !ok || id != "a" {
					t.Fatal("singleton read did not use equality", filter)
				}
			} else if list, ok := filter.Document().Lookup("$in").ArrayOK(); !ok || len(list) == 0 {
				t.Fatal("multi-ID read did not use a batch selector", filter)
			}
		})
	}
}

func TestMongoMixedBatchRejectsUnqualifiedTargetBeforeReadingOrWriting(t *testing.T) {
	qualification := collectionQualificationResponse("db", "records")
	cursor := qualification[1].Value.(bson.D)
	specification := cursor[2].Value.(bson.A)[0].(bson.D)
	specification[1].Value = "view"
	var commands []string
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		commands = append(commands, e.CommandName)
	}}
	responses := []bson.D{qualification}
	adapter := batchMockAdapter(t, responses, monitor)
	readOptions := batchOperationOptions{resource: "weir://mongo/db/records/s:read", action: "read"}
	writeOptions := batchOperationOptions{resource: "weir://mongo/db/records/s:write", action: "put", document: bson.D{{Key: "_id", Value: "write"}}}
	var plans []*execution.Plan
	for _, opts := range []batchOperationOptions{readOptions, writeOptions} {
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	results, signal := adapter.executeRecords(context.Background(), plans)
	mutation := results[1].GetMutation()
	if results[0].GetRead().GetFailure().GetCode() != pb.FailureCode_PRECONDITION_FAILED || mutation.Outcome != pb.MutationOutcome_NOT_STARTED || mutation.Failure.GetCode() != pb.FailureCode_PRECONDITION_FAILED || signal != execution.Neutral {
		t.Fatal("mixed batch bypassed qualification", results, signal)
	}
	want := []string{"listCollections"}
	if !reflect.DeepEqual(commands, want) {
		t.Fatal("unqualified mixed batch executed data commands", commands)
	}
}

func TestMongoMixedBatchRechecksWriteCallerAfterRead(t *testing.T) {
	caller, cancel := context.WithCancel(context.Background())
	defer cancel()
	var commands []string
	monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
		commands = append(commands, e.CommandName)
		if e.CommandName == "find" {
			cancel()
		}
	}}
	document := bson.D{{Key: "_id", Value: "read"}}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document}}}
	responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor)}
	adapter := batchMockAdapter(t, responses, monitor)
	readOptions := batchOperationOptions{resource: "weir://mongo/db/records/s:read", action: "read"}
	writeOptions := batchOperationOptions{resource: "weir://mongo/db/records/s:write", action: "put", document: bson.D{{Key: "_id", Value: "write"}}}
	var plans []*execution.Plan
	for _, opts := range []batchOperationOptions{readOptions, writeOptions} {
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	plans[1].Context = caller
	results, signal := adapter.executeRecords(context.Background(), plans)
	mutation := results[1].GetMutation()
	if results[0].GetRead().GetDocument() == nil || mutation.Outcome != pb.MutationOutcome_NOT_STARTED || mutation.Failure.GetCode() != pb.FailureCode_CANCELLED || signal != execution.Neutral {
		t.Fatal("canceled write was dispatched after shared qualification", results, signal)
	}
	want := []string{"listCollections", "find"}
	if !reflect.DeepEqual(commands, want) {
		t.Fatal("canceled write contacted the backend", commands)
	}
}
