package mongodb

import (
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/value"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestPrepareProgramTransformUsesBuiltInLuaAndBSONInput(t *testing.T) {
	config := Config{Store: "mongo"}
	a := &Adapter{config: config}
	program := &pb.ProgramTransform{Runtime: "lua.v1", Source: []byte("return weir.keep()")}
	form := &pb.Transform_Program{Program: program}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation := &pb.MutateRequest{Resource: "db/records/s:item", Action: action}
	operation := &execution.Operation{Mutate: mutation}
	work, failure := prepareTestRecord(a, operation)
	if failure != nil {
		t.Fatal(failure)
	}
	if work.Backend.(*plan).action != "program" || work.Backend.(*plan).program.Input.Kind != value.Missing {
		t.Fatalf("unexpected program plan: %#v", work)
	}

	program.Input = &pb.Document{MediaType: "application/json", Data: []byte(`{"step":1}`)}
	if _, failure := prepareTestRecord(a, operation); failure == nil || failure.Code != pb.FailureCode_UNSUPPORTED {
		t.Fatal("JSON input accepted by MongoDB adapter", failure)
	}

}

func TestMongoProgramReplacementPreservesResourceIdentity(t *testing.T) {
	objectID := bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	cases := []struct {
		id any
	}{
		{id: "item"},
		{id: int64(42)},
		{id: objectID},
	}
	for _, testCase := range cases {
		identity, err := mongoIdentity(testCase.id)
		if err != nil {
			t.Fatal(err)
		}
		document := value.Value{Kind: value.Object, Fields: []value.Field{{Name: "count", Value: value.Value{Kind: value.Int32, Integer: 1}}}}
		replacement, valid := withMongoIdentity(document, identity, testCase.id)
		if !valid || len(replacement.Fields) != 2 || replacement.Fields[0].Name != "_id" {
			t.Fatalf("identity was not preserved for %T: %#v", testCase.id, replacement)
		}
		encodedID, err := replacement.Lookup("_id")
		if err != nil || !equalID(encodedID, testCase.id) {
			t.Fatalf("wrong identity for %T: %#v %v", testCase.id, encodedID, err)
		}
	}

	identity, err := mongoIdentity("item")
	if err != nil {
		t.Fatal(err)
	}
	wrongIdentity := value.Value{Kind: value.Object, Fields: []value.Field{{Name: "_id", Value: value.Value{Kind: value.String, Text: "other"}}}}
	if _, valid := withMongoIdentity(wrongIdentity, identity, "item"); valid {
		t.Fatal("Lua replacement changed the resource identity")
	}
}
