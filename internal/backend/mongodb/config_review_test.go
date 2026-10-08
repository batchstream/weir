package mongodb

import (
	"context"
	"fmt"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/value"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"strings"
	"testing"
)

func TestOrdinaryBSONUsesNativeBounds(t *testing.T) {
	settings := backend.DefaultOptions()
	settings.Lua.Values.MaxNodes = 32768
	settings.Lua.Values.MaxDepth = 64
	config := Config{Store: "mongo", Options: &settings}
	adapter := &Adapter{config: config}
	array := make(bson.A, 5000)
	for i := range array {
		array[i] = int32(i)
	}
	document := bson.D{{Key: "_id", Value: "a"}, {Key: "items", Value: array}}
	options := batchOperationOptions{resource: "db/records/s:a", action: "put", document: document}
	_, failure := prepareTestRecord(adapter, batchOperation(t, options))
	if failure != nil {
		t.Fatal("configured values rejected valid ordinary BSON", failure)
	}
}

func TestBSONEncodingUsesConfiguredNodesAndActualBytes(t *testing.T) {
	items := make([]value.Value, 140000)
	for i := range items {
		item := value.Value{Kind: value.Int32, Integer: int64(i)}
		items[i] = item
	}
	array := value.Value{Kind: value.Array, Items: items}
	field := value.Field{Name: "items", Value: array}
	document := value.Value{Kind: value.Object, Fields: []value.Field{field}}
	limits := value.DefaultLimits()
	limits.MaxNodes = 200000
	if _, err := Encode(document, limits); err != nil {
		t.Fatal("configured 200000-node BSON encode still failed hidden 4 MiB allocation budget", err)
	}
}

func TestLargeReadSplitsAtMongoNativeCommandBoundary(t *testing.T) {
	var largest int
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "find" {
			largest = max(largest, len(e.Command))
		}
	}}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{}}}
	responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor), readCursorResponse(cursor)}
	adapter := batchMockAdapter(t, responses, monitor)
	settings := backend.DefaultOptions()
	adapter.config.Options = &settings
	var plans []*execution.Plan
	for i := range 5000 {
		id := fmt.Sprintf("%04d", i) + strings.Repeat("x", 3996)
		options := batchOperationOptions{resource: "db/records/s:" + id, action: "read", index: uint64(i + 1)}
		work, failure := prepareTestRecord(adapter, batchOperation(t, options))
		if failure != nil {
			t.Fatal("invalid fixture", failure)
		}
		plans = append(plans, work)
	}
	results := adapter.executeRecords(t.Context(), plans)
	for _, result := range results {
		if result.GetReadResult().GetFailure() != nil {
			t.Fatalf("5000 reads not split into valid native commands; largest observed find=%d; failure=%v", largest, result.GetReadResult().GetFailure())
		}
	}
	if largest > 16<<20 {
		t.Fatalf("native find command exceeds 16 MiB: %d", largest)
	}
}
