//go:build integration

package mongodb

import (
	"bytes"
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func mongoFinitePage(t *testing.T, adapter *Adapter, request *pb.ScanRequest) ([]*pb.Document, *pb.ScanEnd) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	work, failure := adapter.prepareScan(request)
	if failure != nil {
		t.Fatal(failure)
	}
	defer adapter.closeScan(ctx, work)
	var documents []*pb.Document
	var end *pb.ScanEnd
	emit := func(_ *execution.Plan, event *pb.Event) error {
		if document := event.GetDocument(); document != nil {
			documents = append(documents, document)
		}
		if value := event.GetScanEnd(); value != nil {
			end = value
		}
		return nil
	}
	for step := uint64(0); step <= uint64(request.PageSize)+2; step++ {
		adapter.streamScan(ctx, work, emit)
		if !work.Continue {
			return documents, end
		}
	}
	t.Fatal("finite page did not end")
	return nil, nil
}

func TestMongoScanResumesMixedIDsWithoutSessionOrPreviousRecord(t *testing.T) {
	backend := testmongo.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	nested := bson.D{{Key: "nested", Value: int32(1)}}
	binary := bson.Binary{Subtype: 0, Data: []byte{1, 2}}
	ids := []any{int32(1), int64(9223372036854775807), "$first", "z", nested, binary, bson.NewObjectID(), true, bson.DateTime(1), nil}
	collection := backend.Admin.Database(backend.DB).Collection("records")
	for i, id := range ids {
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(i)}}
		if _, err := collection.InsertOne(ctx, document); err != nil {
			t.Fatal(err)
		}
	}
	order := bson.D{{Key: "_id", Value: int32(1)}}
	filter := bson.D{}
	find := options.Find().SetSort(order)
	cursor, err := collection.Find(ctx, filter, find)
	if err != nil {
		t.Fatal(err)
	}
	var expected []bson.Raw
	if err := cursor.All(ctx, &expected); err != nil {
		t.Fatal(err)
	}
	config := Config{URI: backend.URI, Store: "mongo", Pool: 1}
	config = mongoFixtureConfig(t, config)
	request := &pb.ScanRequest{Resource: "weir://mongo/" + backend.DB + "/records", PageSize: 2}
	var seen []*pb.Document
	for page := 0; page < len(ids)+1; page++ {
		adapter, err := Open(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		documents, end := mongoFinitePage(t, adapter, request)
		if err := adapter.Close(); err != nil {
			t.Fatal(err)
		}
		if end == nil || end.Failure != nil || len(documents) > 2 || int(end.DocumentCount) != len(documents) {
			t.Fatal("invalid finite page", end, len(documents))
		}
		if page == 1 {
			replay, err := Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			again, repeated := mongoFinitePage(t, replay, request)
			_ = replay.Close()
			if repeated.Failure != nil || len(again) != len(documents) {
				t.Fatal("page replay failed", repeated)
			}
			for i := range again {
				if !bytes.Equal(again[i].Data, documents[i].Data) {
					t.Fatal("page replay changed records")
				}
			}
		}
		seen = append(seen, documents...)
		if page == 0 {
			last := bson.Raw(documents[len(documents)-1].Data).Lookup("_id")
			remove := bson.D{{Key: "_id", Value: last}}
			if _, err := collection.DeleteOne(ctx, remove); err != nil {
				t.Fatal(err)
			}
		}
		if end.Exhausted {
			break
		}
		if len(end.NextContinuationToken) == 0 {
			t.Fatal("missing continuation")
		}
		request.ContinuationToken = end.NextContinuationToken
	}
	if len(seen) != len(expected) {
		t.Fatal("mixed BSON type omission", len(seen), len(expected))
	}
	for i, document := range seen {
		if !bytes.Equal(document.Data, expected[i]) {
			t.Fatal("mixed BSON type order/fidelity changed", i)
		}
	}
	t.Logf("%d mixed BSON IDs, every page on a new Adapter, preceding ID deleted, replay exact", len(seen))
}
