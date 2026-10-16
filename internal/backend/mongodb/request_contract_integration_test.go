//go:build integration

package mongodb

import (
	"context"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoLuaBusinessFunctionCreatesMissingRecord(t *testing.T) {
	fixture := testmongo.Open(t)
	adapterOptions := adapterTestOptions{fixture: fixture}
	adapter := testAdapter(t, adapterOptions)
	operation := batchOperationOptions{resource: fixture.DB + "/records/s:merge", program: `return function(current, incoming) return incoming end`}
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
	work, failure := adapter.PrepareRecord(context.Background(), record)
	if failure != nil {
		t.Fatal(failure)
	}
	results := adapter.executePrograms(t.Context(), []*execution.Plan{work})
	if len(results) != 1 || results[0].GetMutationResult().Outcome != pb.MutationOutcome_APPLIED || results[0].GetMutationResult().Failure != nil {
		t.Fatal("business function did not create", results)
	}
	filter := bson.D{{Key: "_id", Value: "merge"}}
	stored, err := fixture.Admin.Database(fixture.DB).Collection("records").FindOne(t.Context(), filter).Raw()
	if err != nil || stored.Lookup("n").Int32() != 7 || !stored.Lookup("business").Boolean() {
		t.Fatal("created business document changed", err)
	}
}
