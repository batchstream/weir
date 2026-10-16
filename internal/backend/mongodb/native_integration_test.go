//go:build integration

package mongodb

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoNativeRealErrorsBoundsAndReplyLoss(t *testing.T) {
	for _, mode := range []string{"query", "write", "native_error", "multiframe", "large_command", "drop", "reauth", "overload", "large_reply"} {
		t.Run(mode, func(t *testing.T) {
			backend := testmongo.Open(t)
			native, db := backend.Admin, backend.DB
			proxy := testmongo.StartProxy(t, backend)
			// Reconnect verification reads the fixture's complete 150KiB document.
			cfg := Config{URI: proxy.URI(), Store: "mongo"}
			cfg = mongoFixtureConfig(t, cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			a, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			padding := 150 << 10
			if mode == "large_reply" {
				padding = 5 << 20
			}
			doc := bson.D{{Key: "_id", Value: "x"}, {Key: "n", Value: 1}, {Key: "pad", Value: strings.Repeat("x", padding)}}
			if _, err := native.Database(db).Collection("records").InsertOne(ctx, doc); err != nil {
				t.Fatal(err)
			}
			command := bson.D{{Key: "count", Value: "records"}}
			if mode != "query" {
				query := bson.D{{Key: "_id", Value: "x"}}
				increment := bson.D{{Key: "n", Value: 1}}
				update := bson.D{{Key: "$inc", Value: increment}}
				command = bson.D{{Key: "findAndModify", Value: "records"}, {Key: "query", Value: query}, {Key: "update", Value: update}, {Key: "new", Value: true}}
			}
			if mode == "native_error" {
				update := bson.D{{Key: "$notAnOperator", Value: 1}}
				command[2].Value = update
			}
			if mode == "large_command" {
				query := bson.D{{Key: "pad", Value: strings.Repeat("x", 5<<20)}}
				command[1].Value = query
			}
			if mode == "drop" {
				proxy.DropCommand = "findAndModify"
				proxy.DropRemaining.Store(1)
			}
			if mode == "reauth" || mode == "overload" {
				code := int32(391)
				if mode == "overload" {
					code = 16500
				}
				data := bson.D{{Key: "failCommands", Value: bson.A{"findAndModify"}}, {Key: "errorCode", Value: code}, {Key: "appName", Value: "weir:mongo"}}
				testmongo.FailCommand(t, native, data, 1)
			}
			raw, _ := bson.Marshal(command)
			nativeBody := &pb.Document{ContentType: "application/bson", Data: []byte{5, 0, 0, 0, 0}}
			open := &pb.NativeRequest{Resource: db, Request: nativeBody}
			plan, f := a.prepareNative(open)
			if f != nil {
				t.Fatal(f)
			}
			capture := &nativeCapture{}
			open.Request.Data = raw
			plan.Command = testutil.NativeCommand(open)
			end := a.executeNative(ctx, plan, capture.Emit)
			expected := pb.NativeCompletion_RESPONSE_COMPLETE
			if mode == "drop" {
				expected = pb.NativeCompletion_RESPONSE_INCOMPLETE
			}
			if end.Completion != expected || (end.Failure == nil) != (expected == pb.NativeCompletion_RESPONSE_COMPLETE) {
				t.Fatal(end)
			}
			if mode == "drop" {
				verifyReconnectRead(t, a, proxy, db+"/records/s:x")
			}
			count := 0
			for _, event := range proxy.Events() {
				if event.Command == "count" || event.Command == "findAndModify" {
					count++
				}
			}
			expectedCalls := 1
			if count != expectedCalls {
				t.Fatal("replayed or sent invalid command", count)
			}
			if expected == pb.NativeCompletion_RESPONSE_COMPLETE {
				for _, event := range proxy.Events() {
					if event.Command == "count" || event.Command == "findAndModify" {
						if event.ReplyDigest != sha256.Sum256(capture.body.Bytes()) {
							t.Fatal("native response changed")
						}
					}
				}
				reply := bson.Raw(capture.body.Bytes())
				if reply.Validate() != nil {
					t.Fatal("invalid raw BSON")
				}
				if mode == "native_error" && bsonCommandOK(reply.Lookup("ok")) {
					t.Fatal("lost native error", reply)
				}
				if mode == "multiframe" && capture.chunks < 2 {
					t.Fatal("not chunked")
				}
			}
			if mode == "drop" || mode == "write" {
				filter := bson.D{{Key: "_id", Value: "x"}}
				result := native.Database(db).Collection("records").FindOne(ctx, filter)
				raw, err := result.Raw()
				if err != nil || raw.Lookup("n").Int32() != 2 {
					t.Fatal("write effect absent/replayed", err, raw)
				}
				t.Log("one findAndModify; independent native read n=2", end.Completion)
			}
		})
	}
}

func runMongoNativeCommand(t *testing.T, adapter *Adapter, resource string, command bson.D) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	body := &pb.Document{ContentType: "application/bson", Data: raw}
	request := &pb.NativeRequest{Resource: resource, Request: body}
	work, failure := adapter.prepareNative(request)
	if failure != nil {
		t.Fatal(failure)
	}
	work.Command = testutil.NativeCommand(request)
	capture := &nativeCapture{}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	end := adapter.executeNative(ctx, work, capture.Emit)
	if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil || capture.head == nil || capture.head.BodyContentType != "application/bson" {
		t.Fatal("native command incomplete", command, end)
	}
	reply := bson.Raw(capture.body.Bytes())
	if reply.Validate() != nil || !bsonCommandOK(reply.Lookup("ok")) {
		t.Fatal("native backend command failed", command, reply)
	}
	return reply
}

func TestMongoNativeQueriesIndexesPipelinesAndCursors(t *testing.T) {
	backend := testmongo.Open(t)
	cfg := Config{Store: "mongo", URI: backend.URI}
	cfg = mongoFixtureConfig(t, cfg)
	adapter, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	database := backend.DB
	collection := "native_records"
	indexKey := bson.D{{Key: "n", Value: 1}}
	index := bson.D{{Key: "key", Value: indexKey}, {Key: "name", Value: "n_1"}}
	createIndexes := bson.D{{Key: "createIndexes", Value: collection}, {Key: "indexes", Value: bson.A{index}}}
	// The collection does not exist yet. Native must not qualify it first.
	runMongoNativeCommand(t, adapter, database, createIndexes)
	documents := bson.A{
		bson.D{{Key: "_id", Value: "c"}, {Key: "n", Value: 3}},
		bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: 1}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "n", Value: 2}},
	}
	insert := bson.D{{Key: "insert", Value: collection}, {Key: "documents", Value: documents}, {Key: "ordered", Value: true}}
	runMongoNativeCommand(t, adapter, database, insert)
	filter := bson.D{{Key: "n", Value: bson.D{{Key: "$gte", Value: 1}}}}
	sort := bson.D{{Key: "n", Value: 1}}
	find := bson.D{{Key: "find", Value: collection}, {Key: "filter", Value: filter}, {Key: "sort", Value: sort}, {Key: "skip", Value: 1}, {Key: "limit", Value: 1}, {Key: "hint", Value: "n_1"}}
	reply := runMongoNativeCommand(t, adapter, database, find)
	batch, err := reply.Lookup("cursor", "firstBatch").Array().Values()
	if err != nil || len(batch) != 1 || batch[0].Document().Lookup("_id").StringValue() != "b" {
		t.Fatal("Native find pagination or sorting changed", reply, err)
	}
	group := bson.D{{Key: "_id", Value: nil}, {Key: "sum", Value: bson.D{{Key: "$sum", Value: "$n"}}}}
	pipeline := bson.A{bson.D{{Key: "$group", Value: group}}}
	aggregate := bson.D{{Key: "aggregate", Value: collection}, {Key: "pipeline", Value: pipeline}, {Key: "cursor", Value: bson.D{}}}
	reply = runMongoNativeCommand(t, adapter, database, aggregate)
	batch, err = reply.Lookup("cursor", "firstBatch").Array().Values()
	if err != nil || len(batch) != 1 || batch[0].Document().Lookup("sum").Int32() != 6 {
		t.Fatal("Native aggregate changed", reply, err)
	}
	set := bson.D{{Key: "n", Value: bson.D{{Key: "$add", Value: bson.A{"$n", 10}}}}}
	updatePipeline := bson.A{bson.D{{Key: "$set", Value: set}}}
	update := bson.D{{Key: "q", Value: bson.D{}}, {Key: "u", Value: updatePipeline}, {Key: "multi", Value: true}}
	updates := bson.D{{Key: "update", Value: collection}, {Key: "updates", Value: bson.A{update}}}
	reply = runMongoNativeCommand(t, adapter, database, updates)
	if reply.Lookup("nModified").Int32() != 3 {
		t.Fatal("Native update pipeline changed", reply)
	}
	find = bson.D{{Key: "find", Value: collection}, {Key: "sort", Value: sort}, {Key: "batchSize", Value: 1}}
	reply = runMongoNativeCommand(t, adapter, database, find)
	cursorID := reply.Lookup("cursor", "id").Int64()
	if cursorID == 0 {
		t.Fatal("Native lost cursor", reply)
	}
	seen := 1
	for cursorID != 0 {
		getMore := bson.D{{Key: "getMore", Value: cursorID}, {Key: "collection", Value: collection}, {Key: "batchSize", Value: 1}}
		reply = runMongoNativeCommand(t, adapter, database, getMore)
		batch, err = reply.Lookup("cursor", "nextBatch").Array().Values()
		if err != nil {
			t.Fatal(err)
		}
		seen += len(batch)
		cursorID = reply.Lookup("cursor", "id").Int64()
	}
	if seen != 3 {
		t.Fatal("Native cursor did not preserve all records", seen)
	}
	indexes := bson.D{{Key: "listIndexes", Value: collection}}
	reply = runMongoNativeCommand(t, adapter, database, indexes)
	batch, err = reply.Lookup("cursor", "firstBatch").Array().Values()
	if err != nil || len(batch) != 2 {
		t.Fatal("Native index creation did not persist", reply, err)
	}
}

func TestMongoNativeExplicitSessionCursorsAndTransactions(t *testing.T) {
	backend := testmongo.Open(t)
	config := Config{Store: "mongo", URI: backend.URI}
	config = mongoFixtureConfig(t, config)
	adapter, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		t.Fatal(err)
	}
	id := bson.Binary{Subtype: 4, Data: idBytes}
	session := bson.D{{Key: "id", Value: id}}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		command := bson.D{{Key: "endSessions", Value: bson.A{session}}}
		if err := backend.Admin.Database("admin").RunCommand(ctx, command).Err(); err != nil {
			t.Error("caller session cleanup failed", err)
		}
	}()
	seeds := bson.A{
		bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: 1}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "n", Value: 2}},
	}
	insert := bson.D{{Key: "insert", Value: "records"}, {Key: "documents", Value: seeds}}
	runMongoNativeCommand(t, adapter, backend.DB, insert)
	sort := bson.D{{Key: "_id", Value: 1}}
	find := bson.D{{Key: "find", Value: "records"}, {Key: "sort", Value: sort}, {Key: "batchSize", Value: 1}, {Key: "lsid", Value: session}, {Key: "$db", Value: backend.DB}}
	reply := runMongoNativeCommand(t, adapter, "", find)
	cursorID := reply.Lookup("cursor", "id").Int64()
	if cursorID == 0 {
		t.Fatal("caller session cursor disappeared", reply)
	}
	// Interleaved commands may have different sessions and backend pool sockets.
	// Cursor identity must come from the caller, independent of implicit pools.
	count := bson.D{{Key: "count", Value: "records"}}
	runMongoNativeCommand(t, adapter, backend.DB, count)
	getMore := bson.D{{Key: "getMore", Value: cursorID}, {Key: "collection", Value: "records"}, {Key: "lsid", Value: session}, {Key: "$db", Value: backend.DB}}
	reply = runMongoNativeCommand(t, adapter, "unused_database", getMore)
	batch, err := reply.Lookup("cursor", "nextBatch").Array().Values()
	if err != nil || len(batch) != 1 || batch[0].Document().Lookup("_id").StringValue() != "b" {
		t.Fatal("caller session getMore changed", reply, err)
	}
	cursorID = reply.Lookup("cursor", "id").Int64()
	if cursorID != 0 {
		kill := bson.D{{Key: "killCursors", Value: "records"}, {Key: "cursors", Value: bson.A{cursorID}}, {Key: "lsid", Value: session}}
		runMongoNativeCommand(t, adapter, backend.DB, kill)
	}
	for _, test := range []struct {
		name     string
		number   int64
		terminal string
		want     int64
	}{
		{name: "committed", number: 1, terminal: "commitTransaction", want: 1},
		{name: "aborted", number: 2, terminal: "abortTransaction"},
	} {
		document := bson.D{{Key: "_id", Value: test.name}, {Key: "n", Value: 7}}
		transactionInsert := bson.D{
			{Key: "insert", Value: "records"}, {Key: "documents", Value: bson.A{document}},
			{Key: "lsid", Value: session}, {Key: "txnNumber", Value: test.number},
			{Key: "startTransaction", Value: true}, {Key: "autocommit", Value: false},
		}
		runMongoNativeCommand(t, adapter, backend.DB, transactionInsert)
		terminal := bson.D{{Key: test.terminal, Value: 1}, {Key: "lsid", Value: session}, {Key: "txnNumber", Value: test.number}, {Key: "autocommit", Value: false}, {Key: "$db", Value: "admin"}}
		runMongoNativeCommand(t, adapter, backend.DB, terminal)
		filter := bson.D{{Key: "_id", Value: test.name}}
		actual, err := backend.Admin.Database(backend.DB).Collection("records").CountDocuments(t.Context(), filter)
		if err != nil || actual != test.want {
			t.Fatal("native caller transaction effect changed", test.name, actual, err)
		}
	}
}

func TestMongoNativeCallerDeadlineCancelsBackendWithoutReplay(t *testing.T) {
	backend := testmongo.Open(t)
	proxy := testmongo.StartProxy(t, backend)
	var calls atomic.Int32
	proxy.Monitor = &event.CommandMonitor{Started: func(_ context.Context, call *event.CommandStartedEvent) {
		if call.CommandName == "count" {
			calls.Add(1)
		}
	}}
	config := Config{Store: "mongo", URI: proxy.URI()}
	config = mongoFixtureConfig(t, config)
	adapter, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	data := bson.D{{Key: "failCommands", Value: bson.A{"count"}}, {Key: "appName", Value: "weir:mongo"}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 500}}
	testmongo.FailCommand(t, backend.Admin, data, 1)
	command := bson.D{{Key: "count", Value: "records"}}
	raw, err := bson.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	body := &pb.Document{ContentType: "application/bson", Data: raw}
	request := &pb.NativeRequest{Resource: backend.DB, Request: body}
	work, failure := adapter.prepareNative(request)
	if failure != nil {
		t.Fatal(failure)
	}
	work.Command = testutil.NativeCommand(request)
	capture := &nativeCapture{}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	end := adapter.executeNative(ctx, work, capture.Emit)
	elapsed := time.Since(started)
	if end.Completion != pb.NativeCompletion_RESPONSE_INCOMPLETE || end.GetFailure().GetCode() != pb.FailureCode_DEADLINE_EXCEEDED || capture.head != nil || calls.Load() != 1 || elapsed > 300*time.Millisecond {
		t.Fatal("Native caller deadline lost or command replayed", end, calls.Load(), elapsed)
	}
}
