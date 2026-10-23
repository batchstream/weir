//go:build integration

package mongodb

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func nativeBusinessReply(ctx context.Context, adapter *Adapter, database string, command bson.D) (bson.Raw, error) {
	raw, err := bson.Marshal(command)
	if err != nil {
		return nil, err
	}
	body := &pb.Document{ContentType: "application/bson", Data: raw}
	request := &pb.NativeRequest{Resource: database, Request: body}
	work, failure := adapter.prepareNative(request)
	if failure != nil {
		return nil, fmt.Errorf("prepare Native: %v", failure)
	}
	work.Command = testutil.NativeCommand(request)
	capture := &nativeCapture{}
	end := adapter.executeNative(ctx, work, capture.Emit)
	if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil {
		return nil, fmt.Errorf("incomplete Native reply: %v", end)
	}
	reply := bson.Raw(capture.body.Bytes())
	if err := reply.Validate(); err != nil {
		return nil, err
	}
	return reply, nil
}

func adSnapshotCommand(id string, incoming bson.D) bson.D {
	literal := bson.D{{Key: "$literal", Value: incoming}}
	creation := bson.D{{Key: "$ifNull", Value: bson.A{"$created_at", "$$NOW"}}}
	metadata := bson.D{{Key: "_id", Value: "$_id"}, {Key: "created_at", Value: creation}, {Key: "updated_at", Value: "$$NOW"}}
	replacement := bson.D{{Key: "$mergeObjects", Value: bson.A{literal, metadata}}}
	pipeline := bson.A{bson.D{{Key: "$replaceWith", Value: replacement}}}
	query := bson.D{{Key: "_id", Value: id}}
	concern := bson.D{{Key: "w", Value: "majority"}}
	command := bson.D{{Key: "findAndModify", Value: "ads"}, {Key: "query", Value: query}, {Key: "update", Value: pipeline}, {Key: "upsert", Value: true}, {Key: "new", Value: true}, {Key: "writeConcern", Value: concern}}
	return command
}

func TestMongoNativeADSnapshotDatesAndConcurrentCreation(t *testing.T) {
	fixture := testmongo.Open(t)
	config := Config{Store: "mongo", URI: fixture.URI}
	config = mongoFixtureConfig(t, config)
	adapter, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close(context.Background())
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	before := time.Now().Add(-time.Second).UnixMilli()
	initial := bson.D{{Key: "_id", Value: "ad"}, {Key: "description", Value: "$literal text"}, {Key: "url", Value: "https://example.test/old"}, {Key: "started_at", Value: time.UnixMilli(12345)}}
	command := adSnapshotCommand("ad", initial)
	reply, err := nativeBusinessReply(ctx, adapter, fixture.DB, command)
	if err != nil || !bsonCommandOK(reply.Lookup("ok")) {
		t.Fatal("initial AD snapshot", err, reply)
	}
	first := reply.Lookup("value").Document()
	created := first.Lookup("created_at")
	updated := first.Lookup("updated_at")
	if created.Type != bson.TypeDateTime || updated.Type != bson.TypeDateTime || created.DateTime() != updated.DateTime() || created.DateTime() < before || created.DateTime() > time.Now().Add(time.Second).UnixMilli() || first.Lookup("started_at").Type != bson.TypeDateTime || first.Lookup("description").StringValue() != "$literal text" {
		t.Fatal("server generated time or literal BSON changed", first)
	}
	replacement := bson.D{{Key: "_id", Value: "ad"}, {Key: "description", Value: "updated"}}
	command = adSnapshotCommand("ad", replacement)
	reply, err = nativeBusinessReply(ctx, adapter, fixture.DB, command)
	if err != nil || !bsonCommandOK(reply.Lookup("ok")) {
		t.Fatal("replacement AD snapshot", err, reply)
	}
	second := reply.Lookup("value").Document()
	if second.Lookup("created_at").Type != bson.TypeDateTime || second.Lookup("created_at").DateTime() != created.DateTime() || second.Lookup("updated_at").Type != bson.TypeDateTime || second.Lookup("updated_at").DateTime() < updated.DateTime() || second.Lookup("url").Type != 0 || second.Lookup("started_at").Type != 0 {
		t.Fatal("replacement lost creation date or retained omitted fields", second)
	}
	var workers sync.WaitGroup
	start := make(chan struct{})
	errors := make(chan error, 16)
	dates := make(chan int64, 16)
	for i := range 16 {
		workers.Go(func() {
			<-start
			incoming := bson.D{{Key: "_id", Value: "new-ad"}, {Key: "description", Value: fmt.Sprint(i)}}
			mutation := adSnapshotCommand("new-ad", incoming)
			response, err := nativeBusinessReply(ctx, adapter, fixture.DB, mutation)
			if err != nil || !bsonCommandOK(response.Lookup("ok")) {
				errors <- fmt.Errorf("concurrent first snapshot: %v %v", err, response)
				return
			}
			document := response.Lookup("value").Document()
			date := document.Lookup("created_at")
			if date.Type != bson.TypeDateTime {
				errors <- fmt.Errorf("concurrent creation changed date type: %v", document)
				return
			}
			dates <- date.DateTime()
		})
	}
	close(start)
	workers.Wait()
	close(errors)
	close(dates)
	for err := range errors {
		t.Error(err)
	}
	var creation int64
	count := 0
	for date := range dates {
		if count != 0 && date != creation {
			t.Error("concurrent first snapshots changed created_at", creation, date)
		}
		creation = date
		count++
	}
	if count != 16 {
		t.Fatal("not every concurrent snapshot completed", count)
	}
}

func TestMongoNativeJobUniquenessValidationAndSignaturePatch(t *testing.T) {
	fixture := testmongo.Open(t)
	config := Config{Store: "mongo", URI: fixture.URI}
	config = mongoFixtureConfig(t, config)
	adapter, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close(context.Background())
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	properties := bson.D{{Key: "signature", Value: bson.D{{Key: "bsonType", Value: "string"}}}}
	schema := bson.D{{Key: "bsonType", Value: "object"}, {Key: "required", Value: bson.A{"signature"}}, {Key: "properties", Value: properties}}
	validator := bson.D{{Key: "$jsonSchema", Value: schema}}
	create := bson.D{{Key: "create", Value: "jobs"}, {Key: "validator", Value: validator}}
	runMongoNativeCommand(t, adapter, fixture.DB, create)
	index := bson.D{{Key: "key", Value: bson.D{{Key: "signature", Value: 1}}}, {Key: "name", Value: "signature_1"}, {Key: "unique", Value: true}}
	indexes := bson.D{{Key: "createIndexes", Value: "jobs"}, {Key: "indexes", Value: bson.A{index}}}
	runMongoNativeCommand(t, adapter, fixture.DB, indexes)
	concern := bson.D{{Key: "w", Value: "majority"}}
	job := bson.D{{Key: "_id", Value: "first-id"}, {Key: "signature", Value: "task+url"}, {Key: "status", Value: "queued"}}
	insert := bson.D{{Key: "insert", Value: "jobs"}, {Key: "documents", Value: bson.A{job}}, {Key: "writeConcern", Value: concern}}
	reply := runMongoNativeCommand(t, adapter, fixture.DB, insert)
	if reply.Lookup("writeErrors").Type != 0 || reply.Lookup("writeConcernError").Type != 0 {
		t.Fatal("new job rejected", reply)
	}
	for _, test := range []struct {
		name      string
		signature any
		code      int32
	}{
		{name: "different-id-same-signature", signature: "task+url", code: 11000},
		{name: "schema-invalid", signature: int32(7), code: 121},
	} {
		document := bson.D{{Key: "_id", Value: test.name}, {Key: "signature", Value: test.signature}}
		insert[1].Value = bson.A{document}
		reply = runMongoNativeCommand(t, adapter, fixture.DB, insert)
		failures, err := reply.Lookup("writeErrors").Array().Values()
		if err != nil || len(failures) != 1 || failures[0].Document().Lookup("code").Int32() != test.code {
			t.Fatal("job duplicate and schema failure were conflated", test.name, reply, err)
		}
	}
	missing := bson.D{{Key: "_id", Value: "missing"}, {Key: "signature", Value: ""}, {Key: "status", Value: "queued"}}
	insert[1].Value = bson.A{missing}
	runMongoNativeCommand(t, adapter, fixture.DB, insert)
	missingSignature := bson.A{bson.D{{Key: "signature", Value: ""}}, bson.D{{Key: "signature", Value: nil}}}
	filter := bson.D{{Key: "_id", Value: "missing"}, {Key: "$or", Value: missingSignature}}
	fields := bson.D{{Key: "signature", Value: "fixed"}}
	patch := bson.D{{Key: "q", Value: filter}, {Key: "u", Value: bson.D{{Key: "$set", Value: fields}}}}
	signature := bson.D{{Key: "update", Value: "jobs"}, {Key: "updates", Value: bson.A{patch}}, {Key: "writeConcern", Value: concern}}
	lifecycleFields := bson.D{{Key: "status", Value: "done"}, {Key: "completed_at", Value: time.Now()}}
	lifecycle := bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "missing"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: lifecycleFields}}}}
	save := bson.D{{Key: "update", Value: "jobs"}, {Key: "updates", Value: bson.A{lifecycle}}, {Key: "writeConcern", Value: concern}}
	var workers sync.WaitGroup
	for _, command := range []bson.D{signature, save} {
		workers.Go(func() {
			reply, err := nativeBusinessReply(ctx, adapter, fixture.DB, command)
			if err != nil || !bsonCommandOK(reply.Lookup("ok")) || reply.Lookup("writeErrors").Type != 0 || reply.Lookup("writeConcernError").Type != 0 {
				t.Error("concurrent lifecycle/signature update failed", err, reply)
			}
		})
	}
	workers.Wait()
	find := bson.D{{Key: "find", Value: "jobs"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "missing"}}}}
	reply = runMongoNativeCommand(t, adapter, fixture.DB, find)
	documents, err := reply.Lookup("cursor", "firstBatch").Array().Values()
	if err != nil || len(documents) != 1 || documents[0].Document().Lookup("signature").StringValue() != "fixed" || documents[0].Document().Lookup("status").StringValue() != "done" || documents[0].Document().Lookup("completed_at").Type != bson.TypeDateTime {
		t.Fatal("signature patch overwrote lifecycle fields", reply, err)
	}
	reply = runMongoNativeCommand(t, adapter, fixture.DB, signature)
	if reply.Lookup("nModified").Int32() != 0 {
		t.Fatal("signature patch did not preserve its missing-only condition", reply)
	}
	invalid := make(bson.A, 512)
	for i := range invalid {
		invalid[i] = bson.D{{Key: "_id", Value: fmt.Sprint("invalid-", i)}, {Key: "signature", Value: int32(i)}}
	}
	insert[1].Value = invalid
	ordered := bson.E{Key: "ordered", Value: false}
	insert = append(insert, ordered)
	reply = runMongoNativeCommand(t, adapter, fixture.DB, insert)
	failures, err := reply.Lookup("writeErrors").Array().Values()
	if err != nil || len(failures) != len(invalid) {
		t.Fatal("large diagnostic reply lost item failures", len(failures), err)
	}
	for _, failure := range failures {
		if failure.Document().Lookup("code").Int32() != 121 {
			t.Fatal("validation failure was suppressed or changed", failure)
		}
	}
}

func TestMongoNativeLargeDocumentsPaginateAndReleaseCursor(t *testing.T) {
	fixture := testmongo.Open(t)
	config := Config{Store: "mongo", URI: fixture.URI}
	config = mongoFixtureConfig(t, config)
	adapter, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close(context.Background())
	padding := strings.Repeat("x", 10<<20)
	documents := bson.A{bson.D{{Key: "_id", Value: 1}, {Key: "pad", Value: padding}}}
	insert := bson.D{{Key: "insert", Value: "records"}, {Key: "documents", Value: documents}}
	runMongoNativeCommand(t, adapter, fixture.DB, insert)
	more := bson.A{bson.D{{Key: "_id", Value: 2}}, bson.D{{Key: "_id", Value: 3}}, bson.D{{Key: "_id", Value: 4}}}
	insert[1].Value = more
	runMongoNativeCommand(t, adapter, fixture.DB, insert)
	find := bson.D{{Key: "find", Value: "records"}, {Key: "sort", Value: bson.D{{Key: "_id", Value: 1}}}, {Key: "skip", Value: 0}, {Key: "limit", Value: 3}, {Key: "batchSize", Value: 1}}
	reply := runMongoNativeCommand(t, adapter, fixture.DB, find)
	if len(reply) <= 8<<20 {
		t.Fatal("large Native reply did not cross prior server/protocol limits", len(reply))
	}
	batch, err := reply.Lookup("cursor", "firstBatch").Array().Values()
	if err != nil || len(batch) != 1 || batch[0].Document().Lookup("pad").StringValue() != padding {
		t.Fatal("large Native document changed", err)
	}
	seen := []int32{batch[0].Document().Lookup("_id").Int32()}
	cursor := reply.Lookup("cursor", "id").Int64()
	for cursor != 0 {
		getMore := bson.D{{Key: "getMore", Value: cursor}, {Key: "collection", Value: "records"}, {Key: "batchSize", Value: 1}}
		reply = runMongoNativeCommand(t, adapter, fixture.DB, getMore)
		batch, err = reply.Lookup("cursor", "nextBatch").Array().Values()
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range batch {
			seen = append(seen, item.Document().Lookup("_id").Int32())
		}
		cursor = reply.Lookup("cursor", "id").Int64()
	}
	if fmt.Sprint(seen) != "[1 2 3]" {
		t.Fatal("page assembly did not honor sort/limit", seen)
	}
	find[2].Value = 1
	find[3].Value = 0
	reply = runMongoNativeCommand(t, adapter, fixture.DB, find)
	cursor = reply.Lookup("cursor", "id").Int64()
	if cursor == 0 {
		t.Fatal("early-stop cursor fixture already exhausted")
	}
	kill := bson.D{{Key: "killCursors", Value: "records"}, {Key: "cursors", Value: bson.A{cursor}}}
	reply = runMongoNativeCommand(t, adapter, fixture.DB, kill)
	killed, err := reply.Lookup("cursorsKilled").Array().Values()
	if err != nil || len(killed) != 1 || killed[0].Int64() != cursor {
		t.Fatal("early-stop cursor not reclaimed", reply, err)
	}
	getMore := bson.D{{Key: "getMore", Value: cursor}, {Key: "collection", Value: "records"}}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	reply, err = nativeBusinessReply(ctx, adapter, fixture.DB, getMore)
	if err != nil || bsonCommandOK(reply.Lookup("ok")) || reply.Lookup("code").Int32() != 43 {
		t.Fatal("reclaimed cursor remained usable or native error was hidden", err, reply)
	}
}

func TestMongoLargeTypedRecordsBeyondByteWindow(t *testing.T) {
	fixture := testmongo.Open(t)
	settings := backend.DefaultOptions()
	settings.ExchangeBytes = 4 << 20
	config := Config{Store: "mongo", URI: fixture.URI, Options: &settings}
	config = mongoFixtureConfig(t, config)
	adapter, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close(context.Background())
	document := bson.D{{Key: "_id", Value: "a"}, {Key: "pad", Value: strings.Repeat("x", 5<<20)}}
	options := batchOperationOptions{resource: fixture.DB + "/records/s:a", action: "put", document: document}
	operation := batchOperation(t, options)
	work, failure := prepareTestRecord(adapter, operation)
	if failure != nil {
		t.Fatal("valid BSON could not enter a small exchange window", failure)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	results := adapter.executeRecords(ctx, []*execution.Plan{work})
	if result := results[0].GetMutationResult(); result.Outcome != pb.MutationOutcome_APPLIED || result.Failure != nil {
		t.Fatal("single valid BSON could not cross the exchange byte target", result)
	}
	options.action = "read"
	operation = batchOperation(t, options)
	work, failure = prepareTestRecord(adapter, operation)
	if failure != nil {
		t.Fatal(failure)
	}
	results = adapter.executeRecords(ctx, []*execution.Plan{work})
	read := results[0].GetReadResult()
	if read.GetFailure() != nil || len(read.GetDocument().Data) <= 4<<20 || bson.Raw(read.GetDocument().Data).Lookup("pad").StringValue() != document[1].Value {
		t.Fatal("large typed read failed", read.GetFailure())
	}
}
