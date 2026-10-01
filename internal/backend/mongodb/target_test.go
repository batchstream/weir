package mongodb

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoResourceTargetsPrepareWithoutIO(t *testing.T) {
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	for _, database := range []string{"first", "second"} {
		for _, collection := range []string{"records", "other"} {
			resource := "weir://mongo/" + database + "/" + collection
			opts := batchOperationOptions{resource: resource + "/s:shared", action: "read"}
			operation := batchOperation(t, opts)
			work, failure := adapter.prepareRecord(operation)
			if failure != nil {
				t.Fatal(failure)
			}
			request := &pb.ScanRequest{Resource: resource}
			scan, failure := adapter.prepareScan(request)
			if failure != nil {
				t.Fatal(failure)
			}
			descriptor := &pb.Document{MediaType: NativeDescriptor}
			open := &pb.NativeOpen{Resource: resource, Descriptor_: descriptor, BodyMediaType: "application/bson"}
			native, failure := adapter.prepareNative(open)
			if failure != nil {
				t.Fatal(failure)
			}
			want := namespace{database: database, collection: collection}
			operation.GetRead().Resource = "weir://mongo/changed/changed/s:changed"
			request.Resource = "weir://mongo/changed/changed"
			open.Resource = "weir://mongo/changed/changed"
			if work.Backend.(*plan).target != want || scan.Backend.(*scanPlan).target != want || native.Backend.(namespace) != want {
				t.Fatal("prepared target changed with request", work, scan, native)
			}
		}
	}
}

func TestMongoInvalidResourceTargetsDoNotAccessClient(t *testing.T) {
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	for _, target := range []string{"db", "db/records/extra", "bad.db/records", "db/bad.collection", "_db/records", "db/_records", strings.Repeat("d", 64) + "/records", "db/" + strings.Repeat("c", 64), "db/system.users", "db/%24cmd"} {
		t.Run(target, func(t *testing.T) {
			resource := "weir://mongo/" + target
			opts := batchOperationOptions{resource: resource + "/s:id", action: "read"}
			if _, failure := adapter.prepareRecord(batchOperation(t, opts)); failure == nil {
				t.Fatal("invalid record target accepted")
			}
			request := &pb.ScanRequest{Resource: resource}
			if _, failure := adapter.prepareScan(request); failure == nil {
				t.Fatal("invalid Scan target accepted")
			}
			descriptor := &pb.Document{MediaType: NativeDescriptor}
			open := &pb.NativeOpen{Resource: resource, Descriptor_: descriptor, BodyMediaType: "application/bson"}
			if _, failure := adapter.prepareNative(open); failure == nil {
				t.Fatal("invalid Native target accepted")
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
	var plans []*execution.Plan
	for i, target := range []string{"first/records", "second/other", "first/records"} {
		opts := batchOperationOptions{resource: "weir://mongo/" + target + "/s:shared", action: "read", index: uint64(i + 8)}
		work, failure := adapter.prepareRecord(batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	replies, signal := adapter.executeRecords(context.Background(), plans)
	for i, reply := range replies {
		want := int32(1)
		if i == 1 {
			want = 2
		}
		document := reply.GetRead().GetDocument()
		if document == nil || reply.Index != uint64(i+8) || bson.Raw(document.Data).Lookup("n").Int32() != want {
			t.Fatal("namespace/id correspondence lost", i, reply)
		}
	}
	if signal != execution.Healthy {
		t.Fatal(signal)
	}
}

func TestMongoNativeTargetMismatchDoesNotAccessClient(t *testing.T) {
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	descriptor := &pb.Document{MediaType: NativeDescriptor}
	open := &pb.NativeOpen{Resource: "weir://mongo/db/records", Descriptor_: descriptor, BodyMediaType: "application/bson"}
	work, failure := adapter.prepareNative(open)
	if failure != nil {
		t.Fatal(failure)
	}
	command := bson.D{{Key: "count", Value: "other"}}
	raw := expressionBSON(t, command)
	exchange := &execution.NativeExchange{Source: io.NopCloser(bytes.NewReader(raw)), Sink: &nativeCapture{}}
	end, signal := adapter.executeNative(context.Background(), work, exchange)
	if end.Completion != pb.NativeCompletion_NATIVE_NOT_STARTED || end.Failure == nil || signal != execution.Neutral {
		t.Fatal(end, signal)
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
			opts := batchOperationOptions{resource: "weir://mongo/db/records/s:id", action: "put", document: document}
			work, failure := adapter.prepareRecord(batchOperation(t, opts))
			if failure != nil {
				t.Fatal(failure)
			}
			results, _ := adapter.executeRecords(context.Background(), []*execution.Plan{work})
			if results[0].GetMutation().Outcome != pb.MutationOutcome_NOT_STARTED || results[0].GetMutation().Failure == nil || len(commands) != 1 || commands[0] != "listCollections" {
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
			opts := batchOperationOptions{resource: "weir://mongo/db/records/s:id", action: action, document: document}
			work, failure := adapter.prepareRecord(batchOperation(t, opts))
			if failure != nil {
				t.Fatal(failure)
			}
			work.Context = caller
			results, signal := adapter.executeRecords(context.Background(), []*execution.Plan{work})
			code := results[0].GetRead().GetFailure().GetCode()
			if action == "put" {
				code = results[0].GetMutation().GetFailure().GetCode()
				if results[0].GetMutation().Outcome != pb.MutationOutcome_NOT_STARTED {
					t.Fatal(results)
				}
			}
			if code != pb.FailureCode_CANCELLED || signal != execution.Neutral || len(commands) != 1 || commands[0] != "listCollections" {
				t.Fatal("canceled caller dispatched after metadata I/O", results, signal, commands)
			}
		})
	}
}

func TestMongoCanceledTargetsDoNotContactBackend(t *testing.T) {
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, action := range []string{"read", "put", "program"} {
		document := bson.D{{Key: "_id", Value: "same"}}
		opts := batchOperationOptions{resource: "weir://mongo/db/records/s:same", action: action, document: document, program: "return weir.keep()"}
		work, failure := adapter.prepareRecord(batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		results, signal := adapter.executeRecords(ctx, []*execution.Plan{work})
		if signal != execution.Neutral || results[0] == nil {
			t.Fatal(results, signal)
		}
	}
	request := &pb.ScanRequest{Resource: "weir://mongo/db/records"}
	scan, failure := adapter.prepareScan(request)
	if failure != nil {
		t.Fatal(failure)
	}
	page, _ := adapter.fetchScan(ctx, scan)
	if page.Failure.GetCode() != pb.FailureCode_CANCELLED {
		t.Fatal(page)
	}
	descriptor := &pb.Document{MediaType: NativeDescriptor}
	open := &pb.NativeOpen{Resource: "weir://mongo/db/records", Descriptor_: descriptor, BodyMediaType: "application/bson"}
	native, failure := adapter.prepareNative(open)
	if failure != nil {
		t.Fatal(failure)
	}
	command := bson.D{{Key: "count", Value: "records"}}
	raw := expressionBSON(t, command)
	capture := &nativeCapture{}
	exchange := &execution.NativeExchange{Source: io.NopCloser(bytes.NewReader(raw)), Sink: capture}
	end, _ := adapter.executeNative(ctx, native, exchange)
	if end.Completion != pb.NativeCompletion_NATIVE_NOT_STARTED || end.Failure.GetCode() != pb.FailureCode_CANCELLED {
		t.Fatal(end)
	}
}
