//go:build integration

package mongodb

import (
	"bytes"
	"context"
	"net/url"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoScanNativeBSONProjectionAndConcreteNames(t *testing.T) {
	fixture := testmongo.Open(t)
	collectionName := "1记录.events"
	if err := fixture.Admin.Database(fixture.DB).CreateCollection(t.Context(), collectionName); err != nil {
		t.Fatal(err)
	}
	collection := fixture.Admin.Database(fixture.DB).Collection(collectionName)
	timestamp := bson.Timestamp{T: 99, I: 3}
	document := bson.D{{Key: "_id", Value: "original"}, {Key: "n", Value: int32(7)}, {Key: "timestamp", Value: timestamp}, {Key: "regex", Value: bson.Regex{Pattern: "^x", Options: "i"}}}
	if _, err := collection.InsertOne(t.Context(), document); err != nil {
		t.Fatal(err)
	}
	adapterOptions := adapterTestOptions{fixture: fixture}
	adapter := testAdapter(t, adapterOptions)
	filter := bson.D{{Key: "timestamp", Value: timestamp}}
	raw, err := bson.Marshal(filter)
	if err != nil {
		t.Fatal(err)
	}
	condition := &pb.Document{ContentType: "application/bson", Data: raw}
	for _, mode := range []pb.ProjectionMode{pb.ProjectionMode_INCLUDE, pb.ProjectionMode_EXCLUDE} {
		fields := []string{"_id"}
		expected := document[1:]
		if mode == pb.ProjectionMode_INCLUDE {
			fields = []string{"timestamp"}
			expected = bson.D{{Key: "timestamp", Value: timestamp}}
		}
		projection := &pb.Projection{Mode: mode, Fields: fields}
		request := &pb.ScanRequest{Resource: fixture.DB + "/" + url.PathEscape(collectionName), Filter: condition, Projection: projection, PageSize: 1}
		work, failure := adapter.prepareScan(request)
		if failure != nil {
			t.Fatal(failure)
		}
		page, _ := adapter.fetchScan(context.Background(), work)
		want, _ := bson.Marshal(expected)
		if page.Failure != nil || len(page.Documents) != 1 || !bytes.Equal(page.Documents[0].Data, want) || !page.Complete || len(page.NextContinuationToken) == 0 {
			t.Fatal("native BSON projection or concrete namespace changed", mode, page.Failure)
		}
		request.ContinuationToken = page.NextContinuationToken
		resumed, failure := adapter.prepareScan(request)
		if failure != nil {
			t.Fatal(failure)
		}
		empty, _ := adapter.fetchScan(t.Context(), resumed)
		if empty.Failure != nil || !empty.Exhausted || len(empty.Documents) != 0 {
			t.Fatal("hidden identity broke resume", empty.Failure)
		}
	}
}

func TestMongoLuaStandardMergeCreatesMissingRecord(t *testing.T) {
	fixture := testmongo.Open(t)
	adapterOptions := adapterTestOptions{fixture: fixture}
	adapter := testAdapter(t, adapterOptions)
	operation := batchOperationOptions{resource: fixture.DB + "/records/s:merge", program: `return weir.replace(weir.merge(current,input))`}
	operation.action = "program"
	request := batchOperation(t, operation)
	patch := bson.D{{Key: "n", Value: int32(7)}, {Key: "business", Value: true}}
	raw, err := bson.Marshal(patch)
	if err != nil {
		t.Fatal(err)
	}
	input := &pb.Document{ContentType: "application/bson", Data: raw}
	request.Command.GetMutate().GetAtomicTransform().GetLua().Input = input
	// Prepare from the complete public request so the Lua input is decoded normally.
	record, err := execution.NewRecord("mongo", 1, request.Command)
	if err != nil {
		t.Fatal(err)
	}
	work, failure := adapter.PrepareRecord(record)
	if failure != nil {
		t.Fatal(failure)
	}
	results, _ := adapter.executePrograms(t.Context(), []*execution.Plan{work})
	if len(results) != 1 || results[0].GetMutationResult().Outcome != pb.MutationOutcome_APPLIED || results[0].GetMutationResult().Failure != nil {
		t.Fatal("standard merge did not create", results)
	}
	filter := bson.D{{Key: "_id", Value: "merge"}}
	stored, err := fixture.Admin.Database(fixture.DB).Collection("records").FindOne(t.Context(), filter).Raw()
	if err != nil || stored.Lookup("n").Int32() != 7 || !stored.Lookup("business").Boolean() {
		t.Fatal("created business document changed", err)
	}
}
