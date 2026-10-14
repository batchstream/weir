//go:build integration

package mongodb

import (
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestMongoVisibleMutationsAreImmediatelyQueryableIncludingNoops(t *testing.T) {
	fixture := testmongo.Open(t)
	opts := adapterTestOptions{fixture: fixture}
	adapter := testAdapter(t, opts)
	collection := fixture.Admin.Database(fixture.DB).Collection("records")
	cases := []struct {
		id, action, source string
		seed, missing      bool
		want               int64
	}{
		{id: "put", action: "put", want: 2},
		{id: "create", action: "create", want: 2},
		{id: "replace", action: "replace", seed: true, want: 2},
		{id: "delete", action: "delete", seed: true, missing: true},
		{id: "delete_missing", action: "delete", missing: true},
		{id: "expression", action: "expression", seed: true, want: 2},
		{id: "lua_write", action: "program", seed: true, want: 2, source: `return function(current, incoming) current.n = current.n + 1; return current end`},
		{id: "lua_keep", action: "program", seed: true, want: 1, source: `return function(current, incoming) return weir.keep() end`},
		{id: "lua_delete_missing", action: "program", missing: true, source: `return function(current, incoming) return weir.delete() end`},
	}
	for _, test := range cases {
		t.Run(test.id, func(t *testing.T) {
			if test.seed {
				document := bson.D{{Key: "_id", Value: test.id}, {Key: "n", Value: int32(1)}}
				if _, err := collection.InsertOne(t.Context(), document); err != nil {
					t.Fatal("fixture seed", err)
				}
			}
			document := bson.D{{Key: "_id", Value: test.id}, {Key: "n", Value: int32(2)}}
			if test.action == "expression" {
				update := bson.D{{Key: "n", Value: int32(2)}}
				document = bson.D{{Key: "$set", Value: update}}
			}
			options := batchOperationOptions{resource: fixture.DB + "/records/s:" + test.id, action: test.action, document: document, program: test.source}
			request := batchOperation(t, options)
			mutationOptions := &pb.MutationOptions{Visibility: pb.MutationVisibility_QUERY_VISIBLE}
			request.Command.GetMutate().Options = mutationOptions
			work, failure := prepareTestRecord(adapter, request)
			if failure != nil {
				t.Fatal(failure)
			}
			works := []*execution.Plan{work}
			mutation := adapter.executeRecords(t.Context(), works)[0].GetMutationResult()
			if mutation.GetOutcome() != pb.MutationOutcome_APPLIED || mutation.GetFailure() != nil {
				t.Fatal("mutation was not synchronously visible", mutation)
			}
			filter := bson.D{{Key: "_id", Value: test.id}}
			raw, err := collection.FindOne(t.Context(), filter).Raw()
			if test.missing {
				if err != mongo.ErrNoDocuments {
					t.Fatal("acknowledged deletion still queryable", err, raw)
				}
			} else if err != nil || raw.Lookup("n").AsInt64() != test.want {
				t.Fatal("acknowledged exact document not immediately queryable", err, raw)
			}
		})
	}
}
