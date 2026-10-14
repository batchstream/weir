package mongodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
)

type nativeCapture struct {
	head              *pb.NativeHead
	body              bytes.Buffer
	chunks            int
	headErr, chunkErr error
}

func (c *nativeCapture) Head(h *pb.NativeHead) error {
	if c.headErr != nil {
		return c.headErr
	}
	c.head = h
	return nil
}
func (c *nativeCapture) Chunk(b []byte) error {
	if c.chunkErr != nil {
		return c.chunkErr
	}
	c.chunks++
	_, err := c.body.Write(b)
	return err
}
func (c *nativeCapture) Emit(_ *execution.Plan, event *pb.Event) error {
	if head := event.GetHead(); head != nil {
		return c.Head(head)
	}
	return c.Chunk(event.GetChunk())
}

func TestMongoNativeExplicitCongestion(t *testing.T) {
	for _, test := range []struct {
		name             string
		code             int32
		failHead         bool
		failChunk        bool
		largeReply       bool
		cancelAfterReply bool
		invalidOK        bool
		completion       pb.NativeCompletion
	}{
		{name: "shutdown", code: 91, completion: pb.NativeCompletion_RESPONSE_COMPLETE},
		{name: "stepdown", code: 189, completion: pb.NativeCompletion_RESPONSE_COMPLETE},
		{name: "capacity", code: 16500, completion: pb.NativeCompletion_RESPONSE_COMPLETE},
		{name: "deterministic_error", code: 2, completion: pb.NativeCompletion_RESPONSE_COMPLETE},
		{name: "success", completion: pb.NativeCompletion_RESPONSE_COMPLETE},
		{name: "canceled_after_reply", cancelAfterReply: true, completion: pb.NativeCompletion_RESPONSE_COMPLETE},
		{name: "invalid_ok", invalidOK: true, completion: pb.NativeCompletion_RESPONSE_COMPLETE},
		{name: "success_head_failure", failHead: true, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE},
		{name: "success_chunk_failure", failChunk: true, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE},
		{name: "head_failure", code: 16500, failHead: true, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE},
		{name: "chunk_failure", code: 16500, failChunk: true, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE},
		{name: "large_reply", code: 16500, largeReply: true, completion: pb.NativeCompletion_RESPONSE_COMPLETE},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 0}}
			if test.invalidOK {
				response[0].Value = int32(2)
			}
			if test.code != 0 {
				message := "native error"
				if test.largeReply {
					message = strings.Repeat("x", 5<<20)
				}
				response = bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: test.code}, {Key: "errmsg", Value: message}}
			}
			var calls int
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			monitor := &event.CommandMonitor{
				Started: func(_ context.Context, _ *event.CommandStartedEvent) { calls++ },
				Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
					if test.cancelAfterReply && e.CommandName == "count" {
						cancel()
					}
				},
			}
			deployment := drivertest.NewMockDeployment(response)
			opts := options.Client().SetRetryWrites(false).SetRetryReads(false).SetMaxAdaptiveRetries(0).SetMonitor(monitor)
			opts.Deployment = deployment
			client, err := mongo.Connect(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Disconnect(context.Background())
			cfg := Config{Store: "mongo"}
			a := &Adapter{client: client, deployment: deployment, commandMonitor: monitor, config: cfg}
			nativeBody := &pb.Document{ContentType: "application/bson", Data: []byte{5, 0, 0, 0, 0}}
			open := &pb.NativeRequest{Resource: "db", Request: nativeBody}
			plan, failure := a.prepareNative(open)
			if failure != nil {
				t.Fatal(failure)
			}
			capture := &nativeCapture{}
			if test.failHead {
				capture.headErr = errors.New("output unavailable")
			}
			if test.failChunk {
				capture.chunkErr = errors.New("output unavailable")
			}
			command := bson.D{{Key: "count", Value: "records"}}
			raw, err := bson.Marshal(command)
			if err != nil {
				t.Fatal(err)
			}
			open.Request.Data = raw
			plan.Command = testutil.NativeCommand(open)
			end := a.executeNative(ctx, plan, capture.Emit)
			if end.Completion != test.completion || calls != 1 {
				t.Fatal(end, calls)
			}
			if test.completion == pb.NativeCompletion_RESPONSE_COMPLETE {
				want, err := bson.Marshal(response)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(capture.body.Bytes(), want) || capture.head.BodyContentType != "application/bson" || end.Failure != nil {
					t.Fatal("native reply changed", bson.Raw(capture.body.Bytes()), end)
				}
			}
		})
	}
}
func TestMongoNativeCommandAndOptionsAreOpaque(t *testing.T) {
	adapter := &Adapter{}
	for _, name := range []string{"find", "aggregate", "getMore", "killCursors", "drop", "insert", "eval", "startSession", "createIndexes", "futureCommand"} {
		command := bson.D{{Key: name, Value: "other"}}
		raw, err := bson.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		if failure := adapter.nativeCommand(raw); failure != nil {
			t.Fatal(name, failure)
		}
	}
	for _, key := range []string{"$db", "lsid", "txnNumber", "autocommit", "startTransaction", "writeConcern", "readConcern", "pipeline", "let", "$where", "$function", "$accumulator"} {
		command := bson.D{{Key: "findAndModify", Value: "other"}, {Key: key, Value: bson.JavaScript("return 1")}}
		raw, err := bson.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		if failure := adapter.nativeCommand(raw); failure != nil {
			t.Fatal(key, failure)
		}
	}
	update := bson.A{bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: 1}}}}}
	commands := []bson.D{
		{{Key: "findAndModify", Value: "records"}, {Key: "update", Value: update}},
		{{Key: "count", Value: "records"}, {Key: "count", Value: "other"}},
	}
	for _, command := range commands {
		raw, err := bson.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		if failure := adapter.nativeCommand(raw); failure != nil {
			t.Fatal(command, failure)
		}
	}
}

func TestMongoNativeResourceSelectsDatabaseWithoutCollectionQualification(t *testing.T) {
	for _, resource := range []string{"db", ""} {
		command := bson.D{{Key: "create", Value: "new_collection"}}
		if resource == "" {
			database := bson.E{Key: "$db", Value: "db"}
			command = append(command, database)
		}
		response := bson.D{{Key: "ok", Value: 1}}
		raw, err := bson.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		var calls int
		monitor := &event.CommandMonitor{Started: func(_ context.Context, call *event.CommandStartedEvent) {
			if call.CommandName == "endSessions" {
				return
			}
			calls++
			if call.DatabaseName != "db" || call.CommandName != "create" {
				t.Error("Native performed qualification or changed database", call)
			}
		}}
		responses := []bson.D{response}
		adapter := batchMockAdapter(t, responses, monitor)
		body := &pb.Document{ContentType: "application/bson", Data: raw}
		request := &pb.NativeRequest{Resource: resource, Request: body}
		work, failure := adapter.prepareNative(request)
		if failure != nil {
			t.Fatal(resource, failure)
		}
		work.Command = testutil.NativeCommand(request)
		capture := &nativeCapture{}
		end := adapter.executeNative(t.Context(), work, capture.Emit)
		if calls != 1 || end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE {
			t.Fatal(resource, calls, end)
		}
	}
}

func TestMongoNativeCommandsBeyondFormerCapacityAreOpaque(t *testing.T) {
	adapter := &Adapter{}
	for _, size := range []int{(4 << 20) + 1, (8 << 20) + 1} {
		query := bson.D{{Key: "pad", Value: strings.Repeat("x", size)}}
		command := bson.D{{Key: "count", Value: "records"}, {Key: "query", Value: query}}
		raw, err := bson.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		if failure := adapter.nativeCommand(raw); failure != nil {
			t.Fatal(size, failure)
		}
	}
}

func TestMongoNativeAdapterOwnsRequestFormat(t *testing.T) {
	adapter := &Adapter{}
	for _, contentType := range []string{"application/http", "application/vnd.future.store"} {
		document := &pb.Document{ContentType: contentType, Data: []byte("GET /_doc/x HTTP/1.1\r\n\r\n")}
		request := &pb.NativeRequest{Resource: "db", Request: document}
		if _, failure := adapter.prepareNative(request); failure.GetCode() != pb.FailureCode_UNSUPPORTED {
			t.Fatal("shared envelope interpreted adapter payload", contentType, failure)
		}
	}
	// An empty opaque document is a valid envelope, but an invalid BSON command.
	document := &pb.Document{ContentType: "application/bson"}
	request := &pb.NativeRequest{Resource: "db", Request: document}
	work, failure := adapter.prepareNative(request)
	if failure != nil {
		t.Fatal("generic envelope unexpectedly requires a body", failure)
	}
	work.Command = testutil.NativeCommand(request)
	capture := &nativeCapture{}
	end := adapter.executeNative(context.Background(), work, capture.Emit)
	if end.Completion != pb.NativeCompletion_NATIVE_NOT_STARTED || end.GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT || capture.head != nil || capture.body.Len() != 0 {
		t.Fatal("invalid BSON reached backend", end)
	}
}

func TestMongoNativeSmallRequestPublishesLargeReply(t *testing.T) {
	items := make(bson.A, 1000)
	for i := range items {
		items[i] = int32(i)
	}
	document := bson.D{{Key: "_id", Value: "item"}, {Key: "items", Value: items}}
	response := bson.D{{Key: "ok", Value: 1}, {Key: "value", Value: document}}
	responses := []bson.D{response}
	adapter := batchMockAdapter(t, responses, nil)
	command := bson.D{{Key: "findAndModify", Value: "records"}, {Key: "remove", Value: true}}
	raw, err := bson.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	body := &pb.Document{ContentType: "application/bson", Data: raw}
	request := &pb.NativeRequest{Resource: "db", Request: body}
	work, failure := adapter.prepareNative(request)
	if failure != nil {
		t.Fatal(failure)
	}
	work.Command = testutil.NativeCommand(request)
	capture := &nativeCapture{}
	end := adapter.executeNative(t.Context(), work, capture.Emit)
	expected, err := bson.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil || !bytes.Equal(expected, capture.body.Bytes()) {
		t.Fatal("small request constrained native response", end, capture.body.Len())
	}
}

func TestMongoNativePreservesCallerSessionAndDeadlineOptions(t *testing.T) {
	for _, test := range []struct {
		name string
		max  any
	}{
		{name: "no_server_deadline"},
		{name: "explicit_long_deadline", max: int32(9000)},
		{name: "explicit_zero_deadline", max: int64(0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			id := bson.Binary{Subtype: 4, Data: bytes.Repeat([]byte{1}, 16)}
			session := bson.D{{Key: "id", Value: id}}
			preference := bson.D{{Key: "mode", Value: "primary"}}
			command := bson.D{
				{Key: "getMore", Value: int64(123)}, {Key: "collection", Value: "records"},
				{Key: "lsid", Value: session}, {Key: "txnNumber", Value: int64(7)}, {Key: "autocommit", Value: false},
				{Key: "$readPreference", Value: preference}, {Key: "$db", Value: "caller_database"},
			}
			if test.max != nil {
				option := bson.E{Key: "maxTimeMS", Value: test.max}
				command = append(command, option)
			}
			raw, err := bson.Marshal(command)
			if err != nil {
				t.Fatal(err)
			}
			var calls int
			monitor := &event.CommandMonitor{Started: func(ctx context.Context, call *event.CommandStartedEvent) {
				calls++
				if _, hasDeadline := ctx.Deadline(); hasDeadline || call.DatabaseName != "caller_database" {
					t.Error("Native driver received deadline or changed database", hasDeadline, call.DatabaseName)
				}
				fields, err := call.Command.Elements()
				if err != nil || len(fields) != len(command) {
					t.Error("Native driver injected command options", call.Command, err)
				}
				for _, field := range command {
					want := bson.Raw(raw).Lookup(field.Key)
					actual := call.Command.Lookup(field.Key)
					if want.Type != actual.Type || !bytes.Equal(want.Value, actual.Value) {
						t.Error("Native caller field changed", field.Key, want, actual)
					}
				}
			}}
			cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "caller_database.records"}, {Key: "nextBatch", Value: bson.A{}}}
			response := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: cursor}}
			responses := []bson.D{response}
			adapter := batchMockAdapter(t, responses, monitor)
			body := &pb.Document{ContentType: "application/bson", Data: raw}
			request := &pb.NativeRequest{Request: body}
			work, failure := adapter.prepareNative(request)
			if failure != nil {
				t.Fatal(failure)
			}
			work.Command = testutil.NativeCommand(request)
			capture := &nativeCapture{}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			end := adapter.executeNative(ctx, work, capture.Emit)
			if calls != 1 || end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil {
				t.Fatal(calls, end)
			}
		})
	}
}

func TestMongoNativeDatabaseRoutingRejectsAmbiguousEnvelopes(t *testing.T) {
	commands := []bson.D{
		{{Key: "ping", Value: 1}},
		{{Key: "ping", Value: 1}, {Key: "$db", Value: ""}},
		{{Key: "ping", Value: 1}, {Key: "$db", Value: 1}},
		{{Key: "ping", Value: 1}, {Key: "$db", Value: "db"}, {Key: "$db", Value: "db"}},
	}
	adapter := &Adapter{}
	for _, command := range commands {
		raw, err := bson.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		body := &pb.Document{ContentType: "application/bson", Data: raw}
		request := &pb.NativeRequest{Request: body}
		work, failure := adapter.prepareNative(request)
		if failure != nil {
			t.Fatal(failure)
		}
		work.Command = testutil.NativeCommand(request)
		capture := &nativeCapture{}
		end := adapter.executeNative(t.Context(), work, capture.Emit)
		if end.Completion != pb.NativeCompletion_NATIVE_NOT_STARTED || end.GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT || capture.head != nil || capture.body.Len() != 0 {
			t.Fatal("invalid routing touched backend", command, end)
		}
	}
}

func TestMongoNativeLargeTopLevelDataArraysRemainOpaque(t *testing.T) {
	databases := make(bson.A, 2000)
	for i := range databases {
		database := bson.D{{Key: "name", Value: fmt.Sprintf("weir_database_%d", i)}, {Key: "sizeOnDisk", Value: int64(i)}, {Key: "empty", Value: false}}
		databases[i] = database
	}
	response := bson.D{{Key: "ok", Value: 1}, {Key: "databases", Value: databases}}
	responses := []bson.D{response}
	adapter := batchMockAdapter(t, responses, nil)
	command := bson.D{{Key: "listDatabases", Value: 1}, {Key: "$db", Value: "admin"}}
	raw, err := bson.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	body := &pb.Document{ContentType: "application/bson", Data: raw}
	request := &pb.NativeRequest{Request: body}
	work, failure := adapter.prepareNative(request)
	if failure != nil {
		t.Fatal(failure)
	}
	work.Command = testutil.NativeCommand(request)
	capture := &nativeCapture{}
	end := adapter.executeNative(t.Context(), work, capture.Emit)
	want, err := bson.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil || !bytes.Equal(capture.body.Bytes(), want) || capture.chunks < 2 {
		t.Fatal("large Native data array changed", end, capture.chunks)
	}
}

func TestMongoNativeRejectsCollectionResourceShapes(t *testing.T) {
	adapter := &Adapter{}
	command := bson.D{{Key: "ping", Value: 1}, {Key: "$db", Value: "db"}}
	raw, err := bson.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	for _, resource := range []string{"db/records", "db/system.views", "db/records/s:id"} {
		body := &pb.Document{ContentType: "application/bson", Data: raw}
		request := &pb.NativeRequest{Resource: resource, Request: body}
		work, failure := adapter.prepareNative(request)
		if work != nil || failure.GetCode() != pb.FailureCode_INVALID_ARGUMENT {
			t.Fatal("Native accepted a collection resource even with explicit $db", resource, work, failure)
		}
	}
}
