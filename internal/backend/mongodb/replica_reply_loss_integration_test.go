//go:build integration

package mongodb

import (
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

type replicaHeldOptions struct {
	adapter *Adapter
	fixture *testmongo.Replica
	proxy   *testmongo.ReplicaProxy
	primary int
}

// Run inside the existing discovery fixture lifetime so no extra replica set is
// needed. This package can inspect the real driver, unlike the external CLI.
func replicaStableReplyLoss(t *testing.T, opts replicaHeldOptions) []*testmongo.OperationWatch {
	t.Helper()
	a, f, p := opts.adapter, opts.fixture, opts.proxy
	watches := make([]*testmongo.OperationWatch, 0, 2)
	for _, kind := range []string{"bulkWrite", "findAndModify"} {
		id := "backend-held-" + kind
		ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
		collectionOptions := options.Collection().SetWriteConcern(writeconcern.Majority()).SetReadConcern(readconcern.Majority()).SetReadPreference(readpref.Primary())
		collection := f.Admin.Database(f.DB).Collection("records", collectionOptions)
		seed := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(0)}}
		_, err := collection.InsertOne(ctx, seed)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		filter := bson.D{{Key: "_id", Value: id}}
		var seeded struct{ N int32 }
		err = collection.FindOne(ctx, filter).Decode(&seeded)
		if err != nil || seeded.N != 0 {
			cancel()
			t.Fatal("independent primary-majority seed", seeded.N, err)
		}
		spec := testmongo.OwnedOperation{Namespace: f.DB + ".records", ID: id, Kind: kind}
		watch, err := p.WatchOperation(spec)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		watches = append(watches, watch)
		if err := p.ArmReplyLoss(opts.primary, watch); err != nil {
			cancel()
			t.Fatal(err)
		}
		baseline := a.client.NumberSessionsInProgress()
		if baseline != 0 {
			cancel()
			t.Fatal("session baseline", baseline)
		}
		amount := bson.D{{Key: "n", Value: int32(1)}}
		update := bson.D{{Key: "$inc", Value: amount}}
		done := make(chan bool, 1)
		if kind == "bulkWrite" {
			raw := expressionBSON(t, update)
			request := expressionOperation(f.DB+"/records/s:"+id, raw)
			plan, failure := prepareTestRecord(a, request)
			if failure != nil {
				cancel()
				t.Fatal(failure)
			}
			plans := []*execution.Plan{plan}
			go func() {
				results := a.executeRecords(ctx, plans)
				valid := len(results) == 1 && results[0].GetMutationResult().GetOutcome() == pb.MutationOutcome_UNKNOWN && results[0].GetMutationResult().GetFailure() != nil
				done <- valid
			}()
		} else {
			filter := bson.D{{Key: "_id", Value: id}}
			concern := bson.D{{Key: "w", Value: "majority"}}
			command := bson.D{{Key: "findAndModify", Value: "records"}, {Key: "query", Value: filter}, {Key: "update", Value: update}, {Key: "new", Value: true}, {Key: "writeConcern", Value: concern}}
			raw, err := bson.Marshal(command)
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			body := &pb.Document{ContentType: "application/bson", Data: raw}
			request := &pb.NativeRequest{Resource: f.DB, Request: body}
			plan, failure := a.prepareNative(request)
			if failure != nil {
				cancel()
				t.Fatal(failure)
			}
			plan.Command = testutil.NativeCommand(request)
			capture := &nativeCapture{}
			go func() {
				end := a.executeNative(ctx, plan, capture.Emit)
				done <- end.GetCompletion() == pb.NativeCompletion_RESPONSE_INCOMPLETE && end.Failure != nil
			}()
		}
		select {
		case <-watch.Held():
		case result := <-done:
			cancel()
			t.Fatal("caller returned before hold", result, watch.Snapshot())
		case <-ctx.Done():
			cancel()
			t.Fatal("held barrier", watch.Snapshot())
		}
		replicaProxyCoverage(t, a, p, opts.primary)
		held := a.client.NumberSessionsInProgress()
		snapshot := watch.Snapshot()
		if snapshot.MatchedRequests != 1 || snapshot.ForwardedItems != 1 || !snapshot.RequestValid || !snapshot.ReplyValid || !snapshot.Held || snapshot.DropPerformed || snapshot.Error != "" {
			cancel()
			t.Fatal("actual held majority facts", snapshot)
		}
		if kind == "bulkWrite" && (held != baseline+1 || !snapshot.SessionPresent) || kind == "findAndModify" && (held != baseline || snapshot.SessionPresent || snapshot.TransactionPresent) {
			cancel()
			t.Fatal("actual held session ownership", kind, baseline, held, snapshot)
		}
		var persisted struct{ N int32 }
		err = collection.FindOne(ctx, filter).Decode(&persisted)
		if err != nil || persisted.N != 1 {
			cancel()
			t.Fatal("independent held majority effect", persisted, err)
		}
		select {
		case <-done:
			cancel()
			t.Fatal("caller not alive at held effect")
		default:
		}
		if err := watch.DropReply(); err != nil {
			cancel()
			t.Fatal(err)
		}
		select {
		case <-watch.Dropped():
		case <-ctx.Done():
			cancel()
			t.Fatal("drop not performed", watch.Snapshot())
		}
		select {
		case valid := <-done:
			if !valid || ctx.Err() != nil {
				cancel()
				t.Fatal("actual uncancelled lost reply receipt", valid, ctx.Err())
			}
		case <-ctx.Done():
			cancel()
			t.Fatal("receipt missing")
		}
		returned := a.client.NumberSessionsInProgress()
		if returned != baseline {
			cancel()
			t.Fatal("session not returned before adapter.Close", returned)
		}
		replicaProxyCoverage(t, a, p, opts.primary)
		err = collection.FindOne(ctx, filter).Decode(&persisted)
		if err != nil || persisted.N != 1 {
			cancel()
			t.Fatal("after-drop effect", persisted, err)
		}
		t.Logf("backend_stable kind=%s actual_topology=%v selected_frontend=%s sessions_baseline=%d held=%d returned_before_Close=%d independent_majority_n=%d observation=%+v", kind, replicaProxyDiscovered(t, a), p.Addresses()[opts.primary], baseline, held, returned, persisted.N, watch.Snapshot())
		cancel()
	}
	return watches
}

func replicaFinalReplyLoss(t *testing.T, opts replicaHeldOptions, watches []*testmongo.OperationWatch) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	collectionOptions := options.Collection().SetReadConcern(readconcern.Majority()).SetReadPreference(readpref.Primary())
	collection := opts.fixture.Admin.Database(opts.fixture.DB).Collection("records", collectionOptions)
	for i, kind := range []string{"bulkWrite", "findAndModify"} {
		filter := bson.D{{Key: "_id", Value: "backend-held-" + kind}}
		var value struct{ N int32 }
		err := collection.FindOne(ctx, filter).Decode(&value)
		s := watches[i].Snapshot()
		if err != nil || value.N != 1 || s.MatchedRequests != 1 || s.ForwardedItems != 1 || !s.DropPerformed || s.Error != "" {
			t.Fatal("persistent backend old-operation oracle", kind, value, err, s)
		}
		t.Logf("backend_final kind=%s majority_n=%d observation=%+v", kind, value.N, s)
	}
}
