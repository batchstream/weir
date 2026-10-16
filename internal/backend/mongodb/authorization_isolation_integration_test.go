//go:build integration

package mongodb

import (
	"context"
	"os"
	"reflect"
	"sync"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoCrossNamespaceAuthorizationIsolationPersistsAllowedWrites(t *testing.T) {
	if os.Getenv("WEIR_MONGO_SECURE_INTEGRATION") != "1" {
		t.Skip("secure profile opt-in")
	}
	fixture := testmongo.OpenSecure(t)
	if err := fixture.Admin.Database(fixture.DB).CreateCollection(t.Context(), "denied"); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var batches [][]string
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName != "bulkWrite" {
			return
		}
		namespaces, _ := e.Command.Lookup("nsInfo").Array().Values()
		targets := make([]string, len(namespaces))
		for i, namespace := range namespaces {
			targets[i] = namespace.Document().Lookup("ns").StringValue()
		}
		mu.Lock()
		batches = append(batches, targets)
		mu.Unlock()
	}}
	opts := adapterTestOptions{fixture: &fixture.Fixture, monitor: monitor}
	adapter := testAdapter(t, opts)
	resources := []string{fixture.DB + "/records/s:first", fixture.DB + "/denied/s:first", fixture.DB + "/records/s:second"}
	plans := authorizationPlans(t, adapter, resources)
	results := adapter.executeRecords(t.Context(), plans)
	for i, result := range results {
		want := pb.MutationOutcome_APPLIED
		if i == 1 {
			want = pb.MutationOutcome_NOT_APPLIED
			if result.GetMutationResult().GetFailure().GetCode() != pb.FailureCode_PERMISSION_DENIED {
				t.Fatal("denied namespace lost its own permission result", results)
			}
		}
		if result.GetMutationResult().GetOutcome() != want {
			t.Fatal("namespace permission rejection poisoned healthy writes", results)
		}
	}
	for _, id := range []string{"first", "second"} {
		filter := bson.D{{Key: "_id", Value: id}}
		raw, err := fixture.Admin.Database(fixture.DB).Collection("records").FindOne(t.Context(), filter).Raw()
		if err != nil || raw.Lookup("n").Int32() != 1 {
			t.Fatal("acknowledged allowed write was not persisted once", id, err)
		}
	}
	filter := bson.D{}
	count, err := fixture.Admin.Database(fixture.DB).Collection("denied").CountDocuments(t.Context(), filter)
	if err != nil || count != 0 {
		t.Fatal("denied namespace was mutated", count, err)
	}
	wantBatches := [][]string{{fixture.DB + ".records", fixture.DB + ".denied"}, {fixture.DB + ".records"}, {fixture.DB + ".denied"}}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(batches, wantBatches) {
		t.Fatal("authorization isolation replayed or split healthy namespace writes", batches)
	}
	t.Log("real TLS/SCRAM permissions: one rejected cross-namespace command, one allowed namespace batch persisted two records, one denied namespace returned PERMISSION_DENIED, zero denied documents")
}
