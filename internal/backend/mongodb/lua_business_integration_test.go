//go:build integration

package mongodb

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type mongoBusinessProgramOptions struct {
	resource   string
	source     string
	input      bson.D
	observedAt time.Time
}

func prepareMongoBusinessProgram(t *testing.T, adapter *Adapter, opts mongoBusinessProgramOptions) *execution.Plan {
	t.Helper()
	raw, err := bson.Marshal(opts.input)
	if err != nil {
		t.Fatal(err)
	}
	operationOpts := batchOperationOptions{resource: opts.resource, action: "program", program: opts.source}
	request := batchOperation(t, operationOpts)
	input := &pb.Document{ContentType: "application/bson", Data: raw}
	request.Command.GetMutate().GetAtomicTransform().GetLua().Input = input
	work, failure := prepareTestRecord(adapter, request)
	if failure != nil {
		t.Fatal(failure)
	}
	work.Backend.(*plan).program.ObservedAt = opts.observedAt
	return work
}

func TestMongoLuaProductFunctionCreatesAndPreservesBSON(t *testing.T) {
	source, err := os.ReadFile("../../../examples/lua/product.lua")
	if err != nil {
		t.Fatal(err)
	}
	fixture := testmongo.Open(t)
	adapterOpts := adapterTestOptions{fixture: fixture}
	adapter := testAdapter(t, adapterOpts)
	collection := fixture.Admin.Database(fixture.DB).Collection("records")
	objectID := bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	decimal, err := bson.ParseDecimal128("123456789.0123456789")
	if err != nil {
		t.Fatal(err)
	}
	binary := bson.Binary{Subtype: 0x80, Data: []byte{0, 1, 255}}
	timestamp := bson.Timestamp{T: 99, I: 3}
	regex := bson.Regex{Pattern: "^x", Options: "i"}
	date := bson.DateTime(1725000000123)
	current := bson.D{{Key: "_id", Value: objectID}, {Key: "title", Value: "old"}, {Key: "tags", Value: bson.A{"a", "b"}}, {Key: "small", Value: int32(3)}, {Key: "wide", Value: int64(3)}, {Key: "big", Value: int64(9007199254740993)}, {Key: "float", Value: float64(3)}, {Key: "decimal", Value: decimal}, {Key: "binary", Value: binary}, {Key: "timestamp", Value: timestamp}, {Key: "regex", Value: regex}, {Key: "date", Value: date}, {Key: "null", Value: nil}, {Key: "empty_array", Value: bson.A{}}, {Key: "empty_object", Value: bson.D{}}}
	if _, err := collection.InsertOne(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	before, err := bson.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	observedAt := time.Date(2026, time.October, 7, 1, 2, 3, 456789000, time.UTC)
	cases := []struct {
		name, resource, title string
		id                    any
		input                 bson.D
		tags                  bson.A
	}{
		{name: "create", resource: "s:new", id: "new", title: "new", input: bson.D{{Key: "title", Value: "new"}, {Key: "tags", Value: bson.A{"a", "a", "b"}}}, tags: bson.A{"a", "b"}},
		{name: "update", resource: "oid:" + objectID.Hex(), id: objectID, title: "old", input: bson.D{{Key: "title", Value: ""}, {Key: "tags", Value: bson.A{"b", "c"}}}, tags: bson.A{"a", "b", "c"}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			opts := mongoBusinessProgramOptions{resource: fixture.DB + "/records/" + testCase.resource, source: string(source), input: testCase.input, observedAt: observedAt}
			work := prepareMongoBusinessProgram(t, adapter, opts)
			results := adapter.executePrograms(t.Context(), []*execution.Plan{work})
			if len(results) != 1 || results[0].GetMutationResult().Outcome != pb.MutationOutcome_APPLIED || results[0].GetMutationResult().Failure != nil {
				t.Fatal("product function did not write", results)
			}
			filter := bson.D{{Key: "_id", Value: testCase.id}}
			stored, err := collection.FindOne(t.Context(), filter).Raw()
			if err != nil || stored.Lookup("title").StringValue() != testCase.title || stored.Lookup("updated_at").Type != bson.TypeString || stored.Lookup("updated_at").StringValue() != observedAt.Format(time.RFC3339Nano) {
				t.Fatal("product title or fixed timestamp changed", stored, err)
			}
			wantTagsDocument := bson.D{{Key: "tags", Value: testCase.tags}}
			wantTags, err := bson.Marshal(wantTagsDocument)
			if err != nil || !bytes.Equal(stored.Lookup("tags").Value, bson.Raw(wantTags).Lookup("tags").Value) {
				t.Fatal("product tags lost stable union", stored, err)
			}
			if testCase.name == "update" {
				for _, field := range current {
					if field.Key == "title" || field.Key == "tags" {
						continue
					}
					want, got := bson.Raw(before).Lookup(field.Key), stored.Lookup(field.Key)
					if got.Type != want.Type || !bytes.Equal(got.Value, want.Value) {
						t.Errorf("product merge changed untouched BSON %s: got=%v want=%v", field.Key, got, want)
					}
				}
			}
		})
	}
}

func TestMongoLuaComplexProductKeepsBusinessRules(t *testing.T) {
	source, err := os.ReadFile("../../luaengine/testdata/product_merge.lua")
	if err != nil {
		t.Fatal(err)
	}
	fixture := testmongo.Open(t)
	adapterOpts := adapterTestOptions{fixture: fixture}
	adapter := testAdapter(t, adapterOpts)
	collection := fixture.Admin.Database(fixture.DB).Collection("records")
	comments := make([]string, 0, 23)
	for i := 0; i < 23; i++ {
		comments = append(comments, fmt.Sprintf(`{"id":%d}`, i))
	}
	offers := make([]string, 0, 51)
	for i := 0; i < 51; i++ {
		offers = append(offers, fmt.Sprintf(`{"uid":"new%02d"}`, i))
	}
	cases := []struct {
		name, current, incoming string
	}{
		{name: "content", current: `{"title":"old","brand":"OLD","uid":"old","uids":["legacy"],"gallery":["old"],"allowed_countries":["US"],"first_found_at":"first","comment_count":4,"stocks":[{"stock":3,"variables":{"b":[1,2],"a":{"x":true}}}],"comments":[{"b":[{"z":0},{"z":1}],"a":{"x":"y"}}],"solds":[{"sold":1,"period_hours":24,"record_at":"2026-10-07T01:00:00Z"}],"offers":[{"uid":"same","amount":1},{"uid":"old","amount":0}]}`, incoming: `{"title":"","brand":"new brand","uid":"new","uids":["legacy","","new"],"gallery":[],"allowed_countries":["US","JP"],"first_found_at":"later","last_found_at":"last","comment_count":0,"rating":0,"available":true,"stocks":[{"variables":{"a":{"x":true},"b":[1,2]},"stock":3}],"comments":[{"a":{"x":"y"},"b":[{"z":0},{"z":1}]}],"solds":[{"sold":1,"period_hours":24,"record_at":"2026-10-07T20:00:00Z"}],"offers":[{"uid":"same","amount":2},{"uid":"new","amount":3}]}`},
		{name: "history20", current: `{"comments":[` + strings.Join(comments, ",") + `]}`, incoming: `{"comments":[{"id":22},{"id":23}]}`},
		{name: "offers50", current: `{"offers":[{"uid":"old"}]}`, incoming: `{"offers":[` + strings.Join(offers, ",") + `]}`},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var current, incoming bson.D
			if err := bson.UnmarshalExtJSON([]byte(testCase.current), false, &current); err != nil {
				t.Fatal(err)
			}
			identity := bson.E{Key: "_id", Value: testCase.name}
			current = append(current, identity)
			if _, err := collection.InsertOne(t.Context(), current); err != nil {
				t.Fatal(err)
			}
			if err := bson.UnmarshalExtJSON([]byte(testCase.incoming), false, &incoming); err != nil {
				t.Fatal(err)
			}
			opts := mongoBusinessProgramOptions{resource: fixture.DB + "/records/s:" + testCase.name, source: string(source), input: incoming}
			work := prepareMongoBusinessProgram(t, adapter, opts)
			results := adapter.executePrograms(t.Context(), []*execution.Plan{work})
			if len(results) != 1 || results[0].GetMutationResult().Outcome != pb.MutationOutcome_APPLIED || results[0].GetMutationResult().Failure != nil {
				t.Fatal("complex product function did not write", results)
			}
			filter := bson.D{{Key: "_id", Value: testCase.name}}
			stored, err := collection.FindOne(t.Context(), filter).Raw()
			if err != nil || stored.Lookup("_id").StringValue() != testCase.name {
				t.Fatal("product identity changed", stored, err)
			}
			switch testCase.name {
			case "content":
				if stored.Lookup("title").StringValue() != "old" || stored.Lookup("brand").StringValue() != "NEW BRAND" || stored.Lookup("uid").StringValue() != "new" || stored.Lookup("first_found_at").StringValue() != "first" || stored.Lookup("last_found_at").StringValue() != "last" || stored.Lookup("comment_count").AsInt64() != 4 || stored.Lookup("rating").AsInt64() != 0 || !stored.Lookup("available").Boolean() {
					t.Fatal("scalar product rules changed", stored)
				}
				for field, want := range map[string]int{"uids": 3, "gallery": 1, "allowed_countries": 2, "stocks": 1, "comments": 1, "solds": 1, "offers": 3} {
					items, err := stored.Lookup(field).Array().Values()
					if err != nil || len(items) != want {
						t.Errorf("%s deduplication/empty-array policy changed: %v %v", field, items, err)
					}
				}
				if stored.Lookup("offers").Array().Index(0).Document().Lookup("amount").AsInt64() != 2 {
					t.Fatal("incoming offer did not win duplicate uid", stored)
				}
			case "history20":
				items, err := stored.Lookup("comments").Array().Values()
				if err != nil || len(items) != 20 || items[0].Document().Lookup("id").AsInt64() != 4 || items[19].Document().Lookup("id").AsInt64() != 23 {
					t.Fatal("history did not deduplicate and retain last 20", stored, err)
				}
			case "offers50":
				items, err := stored.Lookup("offers").Array().Values()
				if err != nil || len(items) != 50 || items[0].Document().Lookup("uid").StringValue() != "new02" || items[49].Document().Lookup("uid").StringValue() != "old" {
					t.Fatal("original incoming-first/keep-tail offer policy changed", stored, err)
				}
			}
		})
	}
}
