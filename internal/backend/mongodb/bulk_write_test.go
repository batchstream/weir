package mongodb

import (
	"bytes"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestBulkWriteEncodingMatchesTheNativeCommand(t *testing.T) {
	records := namespace{database: "db", collection: "records"}
	audit := namespace{database: "db", collection: "audit"}
	fields := bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: int32(1)}}
	document := bson.Raw(bsonDocument(t, fields))
	set := bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int32(2)}}}}
	mods := bson.Raw(bsonDocument(t, set))
	oid := bson.NewObjectID()
	writes := []bulkWrite{
		{plan: &plan{target: records, id: "a", action: "create", document: document}, namespace: 0},
		{plan: &plan{target: records, id: "a", action: "put", document: document}, namespace: 0},
		{plan: &plan{target: audit, id: int64(7), action: "replace", document: document}, namespace: 1},
		{plan: &plan{target: audit, id: oid, action: "expression", document: mods}, namespace: 1},
		{plan: &plan{target: records, id: oid, action: "delete"}, namespace: 0},
	}
	ops := bson.A{
		bson.D{{Key: "insert", Value: int32(0)}, {Key: "document", Value: document}},
		bson.D{{Key: "update", Value: int32(0)}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "updateMods", Value: document}, {Key: "multi", Value: false}, {Key: "upsert", Value: true}},
		bson.D{{Key: "update", Value: int32(1)}, {Key: "filter", Value: bson.D{{Key: "_id", Value: int64(7)}}}, {Key: "updateMods", Value: document}, {Key: "multi", Value: false}, {Key: "upsert", Value: false}},
		bson.D{{Key: "update", Value: int32(1)}, {Key: "filter", Value: bson.D{{Key: "_id", Value: oid}}}, {Key: "updateMods", Value: mods}, {Key: "multi", Value: false}, {Key: "upsert", Value: false}},
		bson.D{{Key: "delete", Value: int32(0)}, {Key: "filter", Value: bson.D{{Key: "_id", Value: oid}}}, {Key: "multi", Value: false}},
	}
	infos := bson.A{bson.D{{Key: "ns", Value: "db.records"}}, bson.D{{Key: "ns", Value: "db.audit"}}}
	command := bson.D{
		{Key: "bulkWrite", Value: int32(1)},
		{Key: "ops", Value: ops},
		{Key: "nsInfo", Value: infos},
		{Key: "ordered", Value: false},
		{Key: "errorsOnly", Value: false},
		{Key: "cursor", Value: bson.D{{Key: "batchSize", Value: int32(len(writes))}}},
		{Key: "writeConcern", Value: bson.D{{Key: "w", Value: "majority"}}},
	}
	want := bsonDocument(t, command)
	namespaces := []namespace{records, audit}
	got := encodeBulkWrite(writes, namespaces)
	if !bytes.Equal(got, want) {
		t.Fatalf("encoded bulkWrite differs\n got %v\nwant %v", bson.Raw(got), bson.Raw(want))
	}
}

func bsonDocument(t *testing.T, document bson.D) []byte {
	t.Helper()
	encoded, err := bson.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
