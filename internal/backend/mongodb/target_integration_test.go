//go:build integration

package mongodb

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type targetFixture struct {
	adapter *Adapter
	fixture *testmongo.Fixture
	targets []namespace
}

func openTargetFixture(t *testing.T) targetFixture {
	t.Helper()
	fixture := testmongo.Open(t)
	second := fixture.DB + "_second"
	targets := []namespace{
		{database: fixture.DB, collection: "records"},
		{database: fixture.DB, collection: "alternate"},
		{database: second, collection: "records"},
		{database: second, collection: "alternate"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := fixture.Admin.Database(second).Drop(cleanup); err != nil {
			t.Error(err)
		}
	})
	for _, target := range targets[1:] {
		if err := fixture.Admin.Database(target.database).CreateCollection(ctx, target.collection); err != nil {
			t.Fatal(err)
		}
	}
	config := Config{URI: fixture.URI, Store: "mongo", Pool: 4}
	config = mongoFixtureConfig(t, config)
	if config.Username != "" {
		// Expand only this temporary user's access to these owned test databases.
		roles := bson.A{
			bson.D{{Key: "role", Value: "readWrite"}, {Key: "db", Value: fixture.DB}},
			bson.D{{Key: "role", Value: "readWrite"}, {Key: "db", Value: second}},
		}
		command := bson.D{{Key: "grantRolesToUser", Value: config.Username}, {Key: "roles", Value: roles}}
		if err := fixture.Admin.Database("admin").RunCommand(ctx, command).Err(); err != nil {
			t.Fatal("cannot grant owned target fixture access", err)
		}
	}
	adapter, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := adapter.Close(); err != nil {
			t.Error(err)
		}
	})
	result := targetFixture{adapter: adapter, fixture: fixture, targets: targets}
	return result
}

func targetResource(target namespace) string {
	return "weir://mongo/" + target.database + "/" + target.collection
}

func targetNative(t *testing.T, adapter *Adapter, target namespace, command bson.D) bson.Raw {
	t.Helper()
	descriptor := &pb.Document{MediaType: NativeDescriptor}
	open := &pb.NativeOpen{Resource: targetResource(target), Descriptor_: descriptor, BodyMediaType: "application/bson"}
	work, failure := adapter.prepareNative(open)
	if failure != nil {
		t.Fatal(failure)
	}
	raw := expressionBSON(t, command)
	capture := &nativeCapture{}
	exchange := &execution.NativeExchange{Source: io.NopCloser(bytes.NewReader(raw)), Sink: capture}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	end, _ := adapter.executeNative(ctx, work, exchange)
	if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil {
		t.Fatal(end)
	}
	return capture.body.Bytes()
}

func TestMongoResourceTargetsBulkLuaScanNative(t *testing.T) {
	fixture := openTargetFixture(t)
	adapter := fixture.adapter
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var plans []*execution.Plan
	for i, target := range fixture.targets {
		document := bson.D{{Key: "_id", Value: "shared"}, {Key: "n", Value: int32(i + 1)}}
		opts := batchOperationOptions{resource: targetResource(target) + "/s:shared", action: "create", index: uint64(i + 12), document: document}
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	results, _ := adapter.executeRecords(ctx, plans)
	for i, result := range results {
		if result.Index != uint64(i+12) || result.GetMutation().Outcome != pb.MutationOutcome_APPLIED {
			t.Fatal("mixed target create failed", i, result)
		}
	}
	plans = nil
	for i, target := range fixture.targets {
		work := prepareCounter(t, adapter, targetResource(target)+"/s:shared")
		work.Operation.Index = uint64(30 + i)
		plans = append(plans, work)
	}
	results, _ = adapter.executeRecords(ctx, plans)
	for i, result := range results {
		document := result.GetRead().GetDocument()
		if document == nil || result.Index != uint64(30+i) || bson.Raw(document.Data).Lookup("n").Int32() != int32(i+1) {
			t.Fatal("same id crossed a namespace", i, result)
		}
	}
	plans = nil
	for i, target := range fixture.targets {
		for j, action := range []string{"read", "create"} {
			document := bson.D{{Key: "_id", Value: "shared"}, {Key: "n", Value: int32(999)}}
			opts := batchOperationOptions{resource: targetResource(target) + "/s:shared", action: action, index: uint64(70 + i*2 + j), document: document}
			work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
			if failure != nil {
				t.Fatal(failure)
			}
			plans = append(plans, work)
		}
	}
	results, _ = adapter.executeRecords(ctx, plans)
	for i, result := range results {
		if result.Index != uint64(70+i) {
			t.Fatal("mixed target result index changed", result)
		}
		if i%2 == 0 {
			document := result.GetRead().GetDocument()
			if document == nil || bson.Raw(document.Data).Lookup("n").Int32() != int32(i/2+1) {
				t.Fatal("mixed target point read changed", result)
			}
		} else if result.GetMutation().Outcome != pb.MutationOutcome_NOT_APPLIED || result.GetMutation().Failure.GetCode() != pb.FailureCode_PRECONDITION_FAILED {
			t.Fatal("mixed target duplicate acknowledgement changed", result)
		}
	}
	plans = nil
	for i, target := range fixture.targets {
		opts := batchOperationOptions{
			resource: targetResource(target) + "/s:shared",
			action:   "program",
			index:    uint64(50 + i),
			program:  `return weir.replace(weir.set(current, "n", weir.add(weir.get(current, "n"), weir.i32("10"))))`,
		}
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	results, _ = adapter.executeRecords(ctx, plans)
	for i, result := range results {
		if result.Index != uint64(50+i) || result.GetMutation().Outcome != pb.MutationOutcome_APPLIED {
			t.Fatal("mixed target Lua failed", i, result)
		}
	}
	for i, target := range fixture.targets {
		collection := fixture.fixture.Admin.Database(target.database).Collection(target.collection)
		for _, id := range []string{"page-two", "page-three"} {
			document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(i + 11)}}
			if _, err := collection.InsertOne(ctx, document); err != nil {
				t.Fatal(err)
			}
		}
		request := &pb.ScanRequest{Resource: targetResource(target)}
		work, failure := adapter.prepareScan(request)
		if failure != nil {
			t.Fatal(failure)
		}
		seen := make(map[string]bool)
		for pageNumber := 0; pageNumber < 5; pageNumber++ {
			page, _ := adapter.fetchScan(ctx, work)
			if page.Failure != nil {
				t.Fatal(target, page.Failure)
			}
			for _, document := range page.Documents {
				raw := bson.Raw(document.Data)
				id := raw.Lookup("_id").StringValue()
				if seen[id] || raw.Lookup("n").Int32() != int32(i+11) {
					t.Fatal("Scan target changed across pages", target, raw)
				}
				seen[id] = true
			}
			if page.Exhausted {
				break
			}
		}
		if failure := adapter.closeScan(ctx, work); failure != nil || len(seen) != 3 {
			t.Fatal("Scan did not traverse target", target, failure, seen)
		}
		command := bson.D{{Key: "count", Value: target.collection}}
		reply := targetNative(t, adapter, target, command)
		if reply.Lookup("n").AsInt64() != 3 {
			t.Fatal("Native count used another target", target, reply)
		}
		query := bson.D{{Key: "_id", Value: "shared"}}
		update := bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(100)}}}}
		command = bson.D{{Key: "findAndModify", Value: target.collection}, {Key: "query", Value: query}, {Key: "update", Value: update}, {Key: "new", Value: true}}
		reply = targetNative(t, adapter, target, command)
		if reply.Lookup("value").Document().Lookup("n").Int32() != int32(i+111) {
			t.Fatal("Native write used another target", target, reply)
		}
	}
	for i, target := range fixture.targets {
		filter := bson.D{{Key: "_id", Value: "shared"}}
		raw, err := fixture.fixture.Admin.Database(target.database).Collection(target.collection).FindOne(ctx, filter).Raw()
		if err != nil || raw.Lookup("n").Int32() != int32(i+111) {
			t.Fatal("target effect crossed namespace", target, err, raw)
		}
	}
	t.Log("one client/pool: 2 databases, 2 collections each, same id isolated across Bulk, Lua, Scan and Native")
}

func TestMongoResourceTargetsConcurrent(t *testing.T) {
	fixture := openTargetFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var wait sync.WaitGroup
	for _, target := range fixture.targets {
		wait.Go(func() {
			for i := 0; i < 8; i++ {
				document := bson.D{{Key: "_id", Value: "same"}, {Key: "n", Value: int32(i)}}
				opts := batchOperationOptions{resource: targetResource(target) + "/s:same", action: "put", document: document}
				work, failure := prepareTestRecord(fixture.adapter, batchOperation(t, opts))
				if failure != nil {
					t.Error(failure)
					return
				}
				results, _ := fixture.adapter.executeRecords(ctx, []*execution.Plan{work})
				if results[0].GetMutation().Outcome != pb.MutationOutcome_APPLIED {
					t.Error(target, results)
					return
				}
				read := prepareCounter(t, fixture.adapter, targetResource(target)+"/s:same")
				results, _ = fixture.adapter.executeRecords(ctx, []*execution.Plan{read})
				readDocument := results[0].GetRead().GetDocument()
				if readDocument == nil || bson.Raw(readDocument.Data).Lookup("n").Int32() != int32(i) {
					t.Error("concurrent target state crossed namespace", target, results)
					return
				}
			}
		})
	}
	wait.Wait()
}

func TestMongoResourceTargetsRejectUnqualifiedCollections(t *testing.T) {
	fixture := openTargetFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, kind := range []string{"missing", "view", "capped", "collation"} {
		target := namespace{database: fixture.fixture.DB, collection: "target_" + kind}
		if kind != "missing" {
			command := bson.D{{Key: "create", Value: target.collection}}
			switch kind {
			case "view":
				view := bson.E{Key: "viewOn", Value: "records"}
				pipeline := bson.E{Key: "pipeline", Value: bson.A{}}
				command = append(command, view, pipeline)
			case "capped":
				capped := bson.E{Key: "capped", Value: true}
				size := bson.E{Key: "size", Value: 4096}
				command = append(command, capped, size)
			case "collation":
				collation := bson.D{{Key: "locale", Value: "en"}}
				option := bson.E{Key: "collation", Value: collation}
				command = append(command, option)
			}
			if err := fixture.fixture.Admin.Database(target.database).RunCommand(ctx, command).Err(); err != nil {
				t.Fatal(err)
			}
		}
		for _, action := range []string{"read", "put", "program", "scan", "native"} {
			t.Run(kind+"/"+action, func(t *testing.T) {
				resource := targetResource(target)
				switch action {
				case "scan":
					request := &pb.ScanRequest{Resource: resource}
					work, failure := fixture.adapter.prepareScan(request)
					if failure != nil {
						t.Fatal(failure)
					}
					page, _ := fixture.adapter.fetchScan(ctx, work)
					if page.Failure == nil || len(page.Documents) != 0 || page.Exhausted {
						t.Fatal("unqualified Scan target accepted", page)
					}
					if failure = fixture.adapter.closeScan(ctx, work); failure != nil {
						t.Fatal(failure)
					}
				case "native":
					descriptor := &pb.Document{MediaType: NativeDescriptor}
					open := &pb.NativeOpen{Resource: resource, Descriptor_: descriptor, BodyMediaType: "application/bson"}
					work, failure := fixture.adapter.prepareNative(open)
					if failure != nil {
						t.Fatal(failure)
					}
					query := bson.D{{Key: "_id", Value: "new"}}
					update := bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: 1}}}}
					command := bson.D{{Key: "findAndModify", Value: target.collection}, {Key: "query", Value: query}, {Key: "update", Value: update}, {Key: "upsert", Value: true}}
					raw := expressionBSON(t, command)
					capture := &nativeCapture{}
					exchange := &execution.NativeExchange{Source: io.NopCloser(bytes.NewReader(raw)), Sink: capture}
					end, _ := fixture.adapter.executeNative(ctx, work, exchange)
					if end.Completion != pb.NativeCompletion_NATIVE_NOT_STARTED || end.Failure == nil || capture.head != nil {
						t.Fatal("unqualified Native target accepted", end)
					}
				default:
					document := bson.D{{Key: "_id", Value: "new"}, {Key: "n", Value: 1}}
					opts := batchOperationOptions{
						resource: resource + "/s:new",
						action:   action,
						document: document,
						program:  `return weir.replace(weir.object("n", weir.i32("1")))`,
					}
					work, failure := prepareTestRecord(fixture.adapter, batchOperation(t, opts))
					if failure != nil {
						t.Fatal(failure)
					}
					results, _ := fixture.adapter.executeRecords(ctx, []*execution.Plan{work})
					if action == "read" {
						if results[0].GetRead().GetFailure() == nil || results[0].GetRead().GetMissing() != nil {
							t.Fatal("unqualified point read target accepted", results)
						}
					} else if results[0].GetMutation().Outcome != pb.MutationOutcome_NOT_STARTED || results[0].GetMutation().Failure == nil {
						t.Fatal("unqualified mutation target attempted", results)
					}
				}
			})
		}
		filter := bson.D{{Key: "name", Value: target.collection}}
		specifications, err := fixture.fixture.Admin.Database(target.database).ListCollectionSpecifications(ctx, filter)
		if err != nil {
			t.Fatal(err)
		}
		if kind == "missing" && len(specifications) != 0 {
			t.Fatal("absent target was implicitly created", specifications)
		}
		if kind != "missing" {
			count, err := fixture.fixture.Admin.Database(target.database).Collection(target.collection).CountDocuments(ctx, bson.D{})
			if err != nil || count != 0 {
				t.Fatal("rejected target was modified", kind, count, err)
			}
		}
	}
}
