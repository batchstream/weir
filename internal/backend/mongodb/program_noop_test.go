package mongodb

import (
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestLuaUnchangedBSONSkipsMongoWrite(t *testing.T) {
	cases := []struct {
		name, source string
		document     bson.D
		unchanged    bool
	}{
		{name: "current", source: `return function(current) return current end`, document: bson.D{{Key: "_id", Value: "item"}, {Key: "n", Value: int32(1)}}, unchanged: true},
		{name: "nested values", source: `return function(current) return current end`, document: bson.D{{Key: "_id", Value: "item"}, {Key: "nested", Value: bson.D{{Key: "wide", Value: int64(1)}, {Key: "number", Value: 1.0}, {Key: "values", Value: bson.A{true, nil}}}}}, unchanged: true},
		{name: "same assigned value", source: `return function(current) current.n = current.n; return current end`, document: bson.D{{Key: "_id", Value: "item"}, {Key: "n", Value: int32(1)}}, unchanged: true},
		{name: "different value", source: `return function(current) current.n = 2; return current end`, document: bson.D{{Key: "_id", Value: "item"}, {Key: "n", Value: int32(1)}}},
		{name: "same number different type", source: `return function(current) current.n = 1.0; return current end`, document: bson.D{{Key: "_id", Value: "item"}, {Key: "n", Value: int32(1)}}},
		{name: "removed field", source: `return function(current) current.n = nil; return current end`, document: bson.D{{Key: "_id", Value: "item"}, {Key: "n", Value: int32(1)}}},
		{name: "missing empty object", source: `return function() return {} end`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var raw bson.Raw
			if tc.document != nil {
				encoded, err := bson.Marshal(tc.document)
				if err != nil {
					t.Fatal(err)
				}
				raw = encoded
			}
			program := &luaengine.Program{Source: tc.source}
			native := &plan{id: "item", program: program}
			work := &execution.Plan{Backend: native, Context: t.Context()}
			write, result := transformProgram(t.Context(), work, raw)
			if tc.unchanged {
				if write != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
					t.Fatal("unchanged BSON was not a successful no-op", write, result)
				}
			} else if write == nil || result != nil {
				t.Fatal("changed, differently typed or missing BSON lost its write", write, result)
			}
		})
	}
}
