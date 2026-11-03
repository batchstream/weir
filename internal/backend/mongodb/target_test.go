package mongodb

import (
	"context"
	"strings"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoResourceTargetsPrepareWithoutIO(t *testing.T) {
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	initializeTestLifetime(adapter)
	for _, database := range []string{"first", "second"} {
		for _, collection := range []string{"records", "other"} {
			resource := database + "/" + collection
			opts := batchOperationOptions{resource: resource + "/s:shared", action: "read"}
			operation := batchOperation(t, opts)
			work, failure := prepareTestRecord(adapter, operation)
			if failure != nil {
				t.Fatal(failure)
			}
			want := namespace{database: database, collection: collection}
			operation.Command.GetRead().Resource = "changed/changed/s:changed"
			if work.Backend.(*plan).target != want {
				t.Fatal("prepared target changed with request", work)
			}
		}
	}
}

func TestMongoInvalidResourceTargetsDoNotAccessClient(t *testing.T) {
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	initializeTestLifetime(adapter)
	for _, target := range []string{"db", "db/records/extra", "bad.db/records", strings.Repeat("d", 64) + "/records", "db/" + strings.Repeat("c", 253), "db/system.users", "db/%24cmd"} {
		t.Run(target, func(t *testing.T) {
			resource := target
			opts := batchOperationOptions{resource: resource + "/s:id", action: "read"}
			if _, failure := prepareTestRecord(adapter, batchOperation(t, opts)); failure == nil {
				t.Fatal("invalid record target accepted")
			}
		})
	}
}

func TestMongoPointReadsIsolateTargetsAndDuplicateIDs(t *testing.T) {
	var responses []bson.D
	for i, target := range []namespace{{database: "first", collection: "records"}, {database: "second", collection: "other"}} {
		responses = append(responses, collectionQualificationResponse(target.database, target.collection))
		document := bson.D{{Key: "_id", Value: "shared"}, {Key: "n", Value: int32(i + 1)}}
		cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: target.String()}, {Key: "firstBatch", Value: bson.A{document}}}
		responses = append(responses, readCursorResponse(cursor))
	}
	adapter := batchMockAdapter(t, responses, nil)
	batches := [][]string{{"first/records", "first/records"}, {"second/other"}}
	keys := make(map[string]bool)
	for i, batch := range batches {
		var plans []*execution.Plan
		for j, target := range batch {
			opts := batchOperationOptions{resource: target + "/s:shared", action: "read", index: uint64(j + 8)}
			work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
			if failure != nil {
				t.Fatal(failure)
			}
			plans = append(plans, work)
		}
		keys[plans[0].BatchKey] = true
		for _, reply := range adapter.ExecuteRecords(context.Background(), plans) {
			document := reply.GetReadResult().GetDocument()
			if document == nil || bson.Raw(document.Data).Lookup("n").Int32() != int32(i+1) {
				t.Fatal("namespace/id correspondence lost", i, reply)
			}
		}
	}
	if len(keys) != len(batches) {
		t.Fatal("reads of different collections share a batch key", keys)
	}
}

func TestMongoNativeCommandOwnsCollectionTarget(t *testing.T) {
	var calls int
	monitor := &event.CommandMonitor{Started: func(_ context.Context, call *event.CommandStartedEvent) {
		if call.CommandName == "endSessions" {
			return
		}
		calls++
		if call.CommandName != "count" || call.DatabaseName != "db" || call.Command.Lookup("count").StringValue() != "other" {
			t.Error("Native target or command was changed", call)
		}
	}}
	response := bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 7}}
	responses := []bson.D{response}
	adapter := batchMockAdapter(t, responses, monitor)
	nativeBody := &pb.Document{ContentType: "application/bson", Data: []byte{5, 0, 0, 0, 0}}
	open := &pb.NativeRequest{Resource: "db", Request: nativeBody}
	command := bson.D{{Key: "count", Value: "other"}}
	raw := expressionBSON(t, command)
	capture := &nativeCapture{}
	open.Request.Data = raw
	end := adapter.ExecuteNative(context.Background(), testutil.NativeCommand(open), capture.Emit)
	if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil || calls != 1 || bson.Raw(capture.body.Bytes()).Lookup("n").Int32() != 7 {
		t.Fatal(end, calls)
	}
}

func TestMongoQualificationFailureDoesNotAttemptWrites(t *testing.T) {
	for _, mode := range []string{"missing", "view", "capped", "collation"} {
		t.Run(mode, func(t *testing.T) {
			qualification := collectionQualificationResponse("db", "records")
			cursor := qualification[1].Value.(bson.D)
			specification := cursor[2].Value.(bson.A)[0].(bson.D)
			switch mode {
			case "missing":
				cursor[2].Value = bson.A{}
			case "view":
				specification[1].Value = "view"
			case "capped":
				specification[2].Value = bson.D{{Key: "capped", Value: true}}
			case "collation":
				specification[2].Value = bson.D{{Key: "collation", Value: bson.D{{Key: "locale", Value: "en"}}}}
			}
			var commands []string
			monitor := &event.CommandMonitor{Started: func(_ context.Context, event *event.CommandStartedEvent) {
				commands = append(commands, event.CommandName)
			}}
			adapter := batchMockAdapter(t, []bson.D{qualification}, monitor)
			document := bson.D{{Key: "_id", Value: "id"}}
			opts := batchOperationOptions{resource: "db/records/s:id", action: "put", document: document}
			work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
			if failure != nil {
				t.Fatal(failure)
			}
			results := adapter.ExecuteRecords(context.Background(), []*execution.Plan{work})
			if results[0].GetMutationResult().Outcome != pb.MutationOutcome_NOT_STARTED || results[0].GetMutationResult().Failure == nil || len(commands) != 1 || commands[0] != "listCollections" {
				t.Fatal("target rejection performed a write", results, commands)
			}
		})
	}
}

func TestMongoCallerCanceledDuringQualificationIsNotDispatched(t *testing.T) {
	for _, action := range []string{"read", "put"} {
		t.Run(action, func(t *testing.T) {
			caller, cancel := context.WithCancel(context.Background())
			defer cancel()
			var commands []string
			monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, event *event.CommandSucceededEvent) {
				commands = append(commands, event.CommandName)
				if event.CommandName == "listCollections" {
					cancel()
				}
			}}
			responses := []bson.D{collectionQualificationResponse("db", "records")}
			adapter := batchMockAdapter(t, responses, monitor)
			document := bson.D{{Key: "_id", Value: "id"}}
			opts := batchOperationOptions{resource: "db/records/s:id", action: action, document: document}
			work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
			if failure != nil {
				t.Fatal(failure)
			}
			work.Context = caller
			results := adapter.ExecuteRecords(context.Background(), []*execution.Plan{work})
			code := results[0].GetReadResult().GetFailure().GetCode()
			if action == "put" {
				code = results[0].GetMutationResult().GetFailure().GetCode()
				if results[0].GetMutationResult().Outcome != pb.MutationOutcome_NOT_STARTED {
					t.Fatal(results)
				}
			}
			if code != pb.FailureCode_CANCELLED || len(commands) != 1 || commands[0] != "listCollections" {
				t.Fatal("canceled caller dispatched after metadata I/O", results, commands)
			}
		})
	}
}

func TestMongoCanceledTargetsDoNotContactBackend(t *testing.T) {
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	initializeTestLifetime(adapter)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, action := range []string{"read", "put", "program"} {
		document := bson.D{{Key: "_id", Value: "same"}}
		opts := batchOperationOptions{resource: "db/records/s:same", action: action, document: document, program: "return function(current, incoming) return weir.keep() end"}
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		results := adapter.ExecuteRecords(ctx, []*execution.Plan{work})
		if results[0] == nil {
			t.Fatal(results)
		}
	}
	nativeBody := &pb.Document{ContentType: "application/bson", Data: []byte{5, 0, 0, 0, 0}}
	open := &pb.NativeRequest{Resource: "db", Request: nativeBody}
	command := bson.D{{Key: "count", Value: "records"}}
	raw := expressionBSON(t, command)
	capture := &nativeCapture{}
	open.Request.Data = raw
	end := adapter.ExecuteNative(ctx, testutil.NativeCommand(open), capture.Emit)
	if end.Completion != pb.NativeCompletion_NATIVE_NOT_STARTED || end.Failure.GetCode() != pb.FailureCode_CANCELLED {
		t.Fatal(end)
	}
}
