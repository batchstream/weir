//go:build integration

package mongodb

import (
	"context"
	"sync/atomic"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoCrossNamespaceNativeBulkPersistsAndIsolatesFailure(t *testing.T) {
	fixture := testmongo.Open(t)
	if err := fixture.Admin.Database(fixture.DB).CreateCollection(t.Context(), "other"); err != nil {
		t.Fatal(err)
	}
	seed := bson.D{{Key: "_id", Value: "same"}, {Key: "n", Value: 1}}
	if _, err := fixture.Admin.Database(fixture.DB).Collection("records").InsertOne(t.Context(), seed); err != nil {
		t.Fatal(err)
	}
	var writes atomic.Int32
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "bulkWrite" {
			writes.Add(1)
			namespaces, _ := e.Command.Lookup("nsInfo").Array().Values()
			if len(namespaces) != 2 {
				t.Error("native batch did not include both qualified namespaces", e.Command)
			}
		}
	}}
	opts := adapterTestOptions{fixture: fixture, monitor: monitor}
	adapter := testAdapter(t, opts)
	var plans []*execution.Plan
	for _, collection := range []string{"records", "other", "missing"} {
		document := bson.D{{Key: "_id", Value: "same"}, {Key: "n", Value: 2}}
		operation := batchOperationOptions{resource: fixture.DB + "/" + collection + "/s:same", action: "create", document: document}
		work, failure := prepareTestRecord(adapter, batchOperation(t, operation))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	results := adapter.executeRecords(t.Context(), plans)
	if results[0].GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_APPLIED || results[0].GetMutationResult().GetFailure().GetCode() != pb.FailureCode_PRECONDITION_FAILED || results[1].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || results[2].GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_STARTED || results[2].GetMutationResult().GetFailure().GetCode() != pb.FailureCode_TARGET_NOT_FOUND {
		t.Fatal("cross-namespace partial failure lost mutation evidence", results)
	}
	for i, collection := range []string{"records", "other"} {
		filter := bson.D{{Key: "_id", Value: "same"}}
		raw, err := fixture.Admin.Database(fixture.DB).Collection(collection).FindOne(t.Context(), filter).Raw()
		if err != nil || raw.Lookup("n").Int32() != int32(i+1) {
			t.Fatal("same ID crossed namespaces or acknowledged data was not persisted", collection, raw, err)
		}
	}
	if writes.Load() != 1 {
		t.Fatal("cross-namespace writes split or replayed", writes.Load())
	}
	t.Log("three namespace callers -> one native bulkWrite for two qualified collections; duplicate-key and missing-target failures isolated; persisted values independently verified")
}
