//go:build integration

package mongodb

import (
	"bytes"
	"context"
	"math"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
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
	request := &pb.ScanRequest{Resource: backend.DB + "/records", PageSize: 2}
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

func TestMongoScanBSONExtremesWithFilterAndProjection(t *testing.T) {
	backend := testmongo.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	decimal, err := bson.ParseDecimal128("9007199254740993.25")
	if err != nil {
		t.Fatal(err)
	}
	timestamp := bson.Timestamp{T: 123, I: 7}
	binary := bson.Binary{Subtype: 0x80, Data: []byte{0, 255}}
	firstDocument := bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(2)}}
	secondDocument := bson.D{{Key: "b", Value: int32(2)}, {Key: "a", Value: int32(1)}}
	ids := []any{bson.MinKey{}, bson.MaxKey{}, math.NaN(), math.Inf(-1), math.Inf(1), decimal, timestamp, binary, firstDocument, secondDocument, bson.Symbol("symbol"), bson.JavaScript("return 1;")}
	collection := backend.Admin.Database(backend.DB).Collection("records")
	for i, id := range ids {
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(i)}, {Key: "ignored", Value: "value"}}
		if _, err := collection.InsertOne(ctx, document); err != nil {
			t.Fatalf("insert BSON type %T: %v", id, err)
		}
	}
	excluded := bson.D{{Key: "_id", Value: "$excluded"}, {Key: "n", Value: int32(-1)}, {Key: "ignored", Value: "value"}}
	if _, err := collection.InsertOne(ctx, excluded); err != nil {
		t.Fatal(err)
	}
	lowerBound := bson.D{{Key: "$gte", Value: int32(0)}}
	filter := bson.D{{Key: "n", Value: lowerBound}}
	projection := bson.D{{Key: "n", Value: int32(1)}}
	order := bson.D{{Key: "_id", Value: int32(1)}}
	find := options.Find().SetSort(order).SetProjection(projection)
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
	selector := bson.D{{Key: "filter", Value: filter}, {Key: "projection", Value: projection}}
	raw, err := bson.Marshal(selector)
	if err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{ContentType: "application/bson", Data: raw}
	request := &pb.ScanRequest{Resource: backend.DB + "/records", PageSize: 1, Selector: document}
	var seen []*pb.Document
	for page := 0; page <= len(ids); page++ {
		adapter, err := Open(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		documents, end := mongoFinitePage(t, adapter, request)
		if err := adapter.Close(); err != nil {
			t.Fatal(err)
		}
		if end == nil || end.Failure != nil || len(documents) > 1 || int(end.DocumentCount) != len(documents) {
			t.Fatal("invalid finite page", end, len(documents))
		}
		seen = append(seen, documents...)
		if end.Exhausted {
			break
		}
		request.ContinuationToken = end.NextContinuationToken
	}
	if len(seen) != len(expected) {
		t.Fatal("BSON extremes omitted", len(seen), len(expected))
	}
	for i, document := range seen {
		if !bytes.Equal(document.Data, expected[i]) {
			t.Fatal("BSON extreme order or projected bytes changed", i)
		}
	}
	if bson.Raw(seen[len(seen)-1].Data).Lookup("_id").Type != bson.TypeMaxKey {
		t.Fatal("MaxKey endpoint missing")
	}
	t.Logf("%d native BSON extremes match Find byte for byte across new instances with filter/projection", len(seen))
}

func TestMongoScanDeepKeysetUsesIDIndexAndIncludesMaxKey(t *testing.T) {
	backend := testmongo.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	collection := backend.Admin.Database(backend.DB).Collection("records")
	documents := make([]any, 0, 1002)
	for i := int32(1); i <= 1000; i++ {
		document := bson.D{{Key: "_id", Value: i}, {Key: "n", Value: i}}
		documents = append(documents, document)
	}
	minimum := bson.D{{Key: "_id", Value: bson.MinKey{}}, {Key: "n", Value: int32(0)}}
	maximum := bson.D{{Key: "_id", Value: bson.MaxKey{}}, {Key: "n", Value: int32(1001)}}
	documents = append(documents, minimum, maximum)
	if _, err := collection.InsertMany(ctx, documents); err != nil {
		t.Fatal(err)
	}
	lowerBound := bson.D{{Key: "$gte", Value: int32(0)}}
	filter := bson.D{{Key: "n", Value: lowerBound}}
	projection := bson.D{{Key: "n", Value: int32(1)}}
	selector := bson.D{{Key: "filter", Value: filter}, {Key: "projection", Value: projection}}
	selectorBytes, err := bson.Marshal(selector)
	if err != nil {
		t.Fatal(err)
	}
	selectorDocument := &pb.Document{ContentType: "application/bson", Data: selectorBytes}
	request := &pb.ScanRequest{Resource: backend.DB + "/records", PageSize: 1, Selector: selectorDocument}
	config := Config{URI: backend.URI, Store: "mongo", Pool: 1}
	config = mongoFixtureConfig(t, config)
	adapter, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	work, failure := adapter.prepareScan(request)
	if failure != nil {
		t.Fatal(failure)
	}
	state := work.Backend.(*scanPlan)
	identity := bson.D{{Key: "_id", Value: int32(999)}}
	state.last, err = bson.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	command := scanFindCommand(state)
	explain := bson.D{{Key: "explain", Value: command}, {Key: "verbosity", Value: "executionStats"}}
	var statistics struct {
		QueryPlanner struct {
			WinningPlan bson.Raw `bson:"winningPlan"`
		} `bson:"queryPlanner"`
		ExecutionStats struct {
			Returned  int64 `bson:"nReturned"`
			Documents int64 `bson:"totalDocsExamined"`
			Keys      int64 `bson:"totalKeysExamined"`
		} `bson:"executionStats"`
	}
	if err := backend.Admin.Database(backend.DB).RunCommand(ctx, explain).Decode(&statistics); err != nil {
		t.Fatal(err)
	}
	stats := statistics.ExecutionStats
	plan := statistics.QueryPlanner.WinningPlan.String()
	if !strings.Contains(plan, "IXSCAN") || !strings.Contains(plan, "_id_") || !strings.Contains(plan, "(999, MaxKey]") {
		t.Fatal("keyset did not use the complete exclusive/inclusive _id index bound", plan)
	}
	if stats.Returned != 1 || stats.Documents > 2 || stats.Keys > 2 {
		t.Fatal("deep keyset scanned earlier records", stats)
	}
	page, _ := adapter.fetchScan(ctx, work)
	if page.Failure != nil || len(page.Documents) != 1 || bson.Raw(page.Documents[0].Data).Lookup("_id").Int32() != 1000 || !page.Complete {
		t.Fatal("deep page", page)
	}
	request.ContinuationToken = page.NextContinuationToken
	tail, end := mongoFinitePage(t, adapter, request)
	if end.Failure != nil || end.Exhausted || len(tail) != 1 || bson.Raw(tail[0].Data).Lookup("_id").Type != bson.TypeMaxKey {
		t.Fatal("keyset omitted maximal endpoint", end, len(tail))
	}
	request.ContinuationToken = end.NextContinuationToken
	tail, end = mongoFinitePage(t, adapter, request)
	if end.Failure != nil || !end.Exhausted || len(tail) != 0 {
		t.Fatal("keyset did not exhaust after MaxKey", end)
	}
	t.Logf("deep page after 999 earlier keys: documents examined=%d, keys examined=%d", stats.Documents, stats.Keys)
}
