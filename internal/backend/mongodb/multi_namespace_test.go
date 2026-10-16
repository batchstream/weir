package mongodb

import (
	"context"
	"reflect"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoCrossNamespaceBulkPreservesIndexesAndPartialFailures(t *testing.T) {
	first := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 1}, {Key: "n", Value: 1}}
	second := bson.D{{Key: "ok", Value: 0}, {Key: "idx", Value: 0}, {Key: "code", Value: 11000}}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: bson.A{first, second}}}
	reply := bson.D{
		{Key: "ok", Value: 1}, {Key: "cursor", Value: cursor}, {Key: "nErrors", Value: 1},
		{Key: "nInserted", Value: 1}, {Key: "nDeleted", Value: 0}, {Key: "nMatched", Value: 0},
		{Key: "nModified", Value: 0}, {Key: "nUpserted", Value: 0},
	}
	responses := []bson.D{
		collectionQualificationResponse("first", "records"),
		collectionQualificationResponse("second", "other"), reply,
	}
	var commands []string
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		commands = append(commands, e.CommandName)
		if e.CommandName != "bulkWrite" {
			return
		}
		namespaces, _ := e.Command.Lookup("nsInfo").Array().Values()
		ops, _ := e.Command.Lookup("ops").Array().Values()
		if len(namespaces) != 2 || len(ops) != 2 {
			t.Error("cross-namespace writes were separated", e.Command)
			return
		}
		for i, want := range []string{"first.records", "second.other"} {
			if namespaces[i].Document().Lookup("ns").StringValue() != want || ops[i].Document().Lookup("insert").Int32() != int32(i) {
				t.Error("operation lost namespace correspondence", e.Command)
			}
		}
	}}
	adapter := batchMockAdapter(t, responses, monitor)
	var plans []*execution.Plan
	for i, target := range []string{"first/records", "second/other"} {
		document := bson.D{{Key: "_id", Value: "same"}, {Key: "n", Value: i}}
		opts := batchOperationOptions{resource: target + "/s:same", action: "create", document: document}
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	if plans[0].BatchKey != plans[1].BatchKey || plans[0].Key == plans[1].Key {
		t.Fatal("cross-target writes cannot batch or share a false ordering conflict")
	}
	results := adapter.executeRecords(t.Context(), plans)
	if results[0].GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_APPLIED || results[0].GetMutationResult().GetFailure().GetCode() != pb.FailureCode_PRECONDITION_FAILED || results[1].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("out-of-order partial failure lost caller correspondence", results)
	}
	want := []string{"listCollections", "listCollections", "bulkWrite"}
	if !reflect.DeepEqual(commands, want) {
		t.Fatal("cross-namespace bulk was split or replayed", commands)
	}
}

func TestMongoCrossNamespaceBulkSplitsWithinNativeByteWindow(t *testing.T) {
	var responses []bson.D
	for _, collection := range []string{"first", "second", "third"} {
		responses = append(responses, collectionQualificationResponse("db", collection))
	}
	for range 3 {
		item := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
		responses = append(responses, programBulkResponse(bson.A{item}, 1))
	}
	writes := 0
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName != "bulkWrite" {
			return
		}
		writes++
		ops, _ := e.Command.Lookup("ops").Array().Values()
		namespaces, _ := e.Command.Lookup("nsInfo").Array().Values()
		if len(ops) != 1 || len(namespaces) != 1 || ops[0].Document().Lookup("update").Int32() != 0 {
			t.Error("split retained an invalid namespace index", e.Command)
		}
	}}
	adapter := batchMockAdapter(t, responses, monitor)
	settings := backend.DefaultOptions()
	// Keep all requests in one scheduled chunk, then force native BSON splitting.
	settings.ExchangeBytes = 4096
	adapter.config.Options = &settings
	adapter.maxDocumentBytes = 300
	var plans []*execution.Plan
	for _, collection := range []string{"first", "second", "third"} {
		document := bson.D{{Key: "_id", Value: "same"}, {Key: "n", Value: 1}}
		opts := batchOperationOptions{resource: "db/" + collection + "/s:same", action: "put", document: document}
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	for _, result := range adapter.executeRecords(t.Context(), plans) {
		if result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
			t.Fatal("byte split failed a legal write", result)
		}
	}
	if writes != 3 {
		t.Fatal("native command boundary did not split writes", writes)
	}
}
