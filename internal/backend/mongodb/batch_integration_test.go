//go:build integration

package mongodb

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoMixedRecordBatchUsesPointReadAndVerboseBulkWrite(t *testing.T) {
	fixture := testmongo.Open(t)
	collection := fixture.Admin.Database(fixture.DB).Collection("records")
	for _, id := range []string{"read", "replace", "expression", "duplicate", "delete", "program"} {
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(1)}}
		if _, err := collection.InsertOne(context.Background(), document); err != nil {
			t.Fatal(err)
		}
	}
	var finds, bulkWrites, transactionWrites, commits atomic.Int32
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		switch e.CommandName {
		case "find":
			finds.Add(1)
		case "bulkWrite":
			bulkWrites.Add(1)
		case "update":
			transactionWrites.Add(1)
		case "commitTransaction":
			commits.Add(1)
		}
	}}
	opts := adapterTestOptions{fixture: fixture, monitor: monitor}
	a := testAdapter(t, opts)
	operations := []struct {
		id, action string
		outcome    pb.MutationOutcome
	}{
		{id: "read", action: "read"},
		{id: "put-new", action: "put", outcome: pb.MutationOutcome_APPLIED},
		{id: "read-missing", action: "read"},
		{id: "replace", action: "replace", outcome: pb.MutationOutcome_APPLIED},
		{id: "replace-missing", action: "replace", outcome: pb.MutationOutcome_NOT_APPLIED},
		{id: "expression", action: "expression", outcome: pb.MutationOutcome_APPLIED},
		{id: "expression-missing", action: "expression", outcome: pb.MutationOutcome_NOT_APPLIED},
		{id: "create-new", action: "create", outcome: pb.MutationOutcome_APPLIED},
		{id: "duplicate", action: "create", outcome: pb.MutationOutcome_NOT_APPLIED},
		{id: "delete", action: "delete", outcome: pb.MutationOutcome_APPLIED},
		{id: "delete-missing", action: "delete", outcome: pb.MutationOutcome_APPLIED},
		{id: "program", action: "program", outcome: pb.MutationOutcome_APPLIED},
	}
	var plans []*execution.Plan
	for i, operation := range operations {
		document := bson.D{{Key: "_id", Value: operation.id}, {Key: "n", Value: int32(2)}}
		if operation.action == "expression" {
			document = bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}
		}
		options := batchOperationOptions{resource: fixture.DB + "/records/s:" + operation.id, action: operation.action, index: uint64(i + 3), document: document, program: `return function(current, incoming) current.n = current.n + 1; return current end`}
		work, failure := prepareTestRecord(a, batchOperation(t, options))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	replies := a.executeRecords(context.Background(), plans)
	for i, reply := range replies {
		operation := operations[i]
		if operation.action == "read" {
			if operation.id == "read" && reply.GetReadResult().GetDocument() == nil || operation.id == "read-missing" && reply.GetReadResult().GetMissing() == nil {
				t.Fatal("read result changed", reply)
			}
		} else if reply.GetMutationResult().Outcome != operation.outcome || operation.outcome == pb.MutationOutcome_NOT_APPLIED && reply.GetMutationResult().GetFailure().GetCode() != pb.FailureCode_PRECONDITION_FAILED {
			t.Fatal(operation.id, reply)
		}
	}
	if finds.Load() != 2 || bulkWrites.Load() != 2 || transactionWrites.Load() != 0 || commits.Load() != 1 {
		t.Fatal("mixed operations were not physically aggregated", finds.Load(), bulkWrites.Load(), transactionWrites.Load(), commits.Load())
	}
	for _, id := range []string{"put-new", "create-new", "replace", "expression", "program"} {
		filter := bson.D{{Key: "_id", Value: id}}
		raw, err := collection.FindOne(context.Background(), filter).Raw()
		if err != nil || raw.Lookup("n").Int32() != 2 {
			t.Fatal("effect absent", id, err, raw)
		}
	}
	t.Logf("12 mixed records: point read requests=%d physical bulk writes=%d Lua transaction commits=%d", finds.Load(), bulkWrites.Load(), commits.Load())
}

func TestMongoCallerCancellationBeforeWritePhaseDoesNotAffectPeers(t *testing.T) {
	fixture := testmongo.Open(t)
	caller, cancel := context.WithCancel(context.Background())
	defer cancel()
	var finds, writes atomic.Int32
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "find" {
			finds.Add(1)
			cancel()
		}
		if e.CommandName == "bulkWrite" {
			writes.Add(1)
		}
	}}
	opts := adapterTestOptions{fixture: fixture, monitor: monitor}
	a := testAdapter(t, opts)
	var plans []*execution.Plan
	for i, action := range []string{"read", "put", "put", "program"} {
		id := []string{"read-missing", "canceled-write", "peer-write", "canceled-program"}[i]
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(1)}}
		options := batchOperationOptions{resource: fixture.DB + "/records/s:" + id, action: action, index: uint64(i), document: document, program: "return function(current, incoming) return weir.keep() end"}
		p, failure := prepareTestRecord(a, batchOperation(t, options))
		if failure != nil {
			t.Fatal(failure)
		}
		if i == 1 || i == 3 {
			p.Context = caller
		}
		plans = append(plans, p)
	}
	replies := a.executeRecords(context.Background(), plans)
	if replies[0].GetReadResult().GetMissing() == nil || replies[1].GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_STARTED || replies[2].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || replies[3].GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_STARTED {
		t.Fatal(replies)
	}
	if finds.Load() != 1 || writes.Load() != 1 {
		t.Fatal("canceled caller affected peers or Lua was executed", finds.Load(), writes.Load())
	}
	filter := bson.D{}
	count, err := fixture.Admin.Database(fixture.DB).Collection("records").CountDocuments(context.Background(), filter)
	if err != nil || count != 1 {
		t.Fatal("canceled write started", count, err)
	}
}

func TestMongoMixedWriteLostReplyDoesNotReplayExpression(t *testing.T) {
	fixture := testmongo.Open(t)
	collection := fixture.Admin.Database(fixture.DB).Collection("records")
	document := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int32(0)}}
	if _, err := collection.InsertOne(context.Background(), document); err != nil {
		t.Fatal(err)
	}
	proxy := testmongo.StartProxy(t, fixture)
	proxy.DropCommand = "bulkWrite"
	proxy.DropRemaining.Store(1)
	opts := adapterTestOptions{fixture: fixture, uri: proxy.URI()}
	a := testAdapter(t, opts)
	var plans []*execution.Plan
	for i, action := range []string{"expression", "create"} {
		id := []string{"counter", "peer"}[i]
		doc := bson.D{{Key: "_id", Value: id}}
		if action == "expression" {
			doc = bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}
		}
		options := batchOperationOptions{resource: fixture.DB + "/records/s:" + id, action: action, index: uint64(i), document: doc}
		work, failure := prepareTestRecord(a, batchOperation(t, options))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	replies := a.executeRecords(context.Background(), plans)
	for _, reply := range replies {
		if reply.GetMutationResult().GetOutcome() != pb.MutationOutcome_UNKNOWN {
			t.Fatal("lost mixed-write acknowledgement became definite", replies)
		}
	}
	filter := bson.D{{Key: "_id", Value: "counter"}}
	raw, err := collection.FindOne(context.Background(), filter).Raw()
	if err != nil || raw.Lookup("n").Int32() != 1 {
		t.Fatal("expression replayed or effect was lost", err, raw)
	}
	filter = bson.D{}
	count, err := collection.CountDocuments(context.Background(), filter)
	if err != nil || count != 2 {
		t.Fatal("peer effect absent", count, err)
	}
	writes := 0
	for _, e := range proxy.Events() {
		if e.Command == "bulkWrite" {
			writes++
			if !e.Acknowledged || !e.Dropped {
				t.Fatal("proxy failed to drop an actual acknowledged write", e)
			}
		}
	}
	if writes != 1 {
		t.Fatal("mixed writes replayed", writes)
	}
}

func TestMongoIndependentRPCDuplicateReadsKeepBudgetAndCallerIsolation(t *testing.T) {
	for _, mode := range []string{"first exhausted", "both retained", "first canceled"} {
		t.Run(mode, func(t *testing.T) {
			fixture := testmongo.Open(t)
			document := bson.D{{Key: "_id", Value: "same"}, {Key: "value", Value: 123}}
			if _, err := fixture.Admin.Database(fixture.DB).Collection("records").InsertOne(t.Context(), document); err != nil {
				t.Fatal(err)
			}
			var finds atomic.Int32
			monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
				if e.CommandName == "find" {
					finds.Add(1)
				}
			}}
			opts := adapterTestOptions{fixture: fixture, monitor: monitor}
			adapter := testAdapter(t, opts)
			var plans []*execution.Plan
			caller, cancel := context.WithCancel(t.Context())
			defer cancel()
			for i := range 2 {
				opts := batchOperationOptions{resource: fixture.DB + "/records/s:same", action: "read", index: 1}
				work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
				if failure != nil {
					t.Fatal(failure)
				}
				work.Context = t.Context()
				if i == 0 {
					work.Context = caller
					if mode == "first exhausted" {
						work.ResultBytes = 1
					}
				}
				plans = append(plans, work)
			}
			if mode == "first canceled" {
				cancel()
			}
			replies := adapter.executeRecords(t.Context(), plans)
			raw := expressionBSON(t, document)
			if finds.Load() != 1 || len(replies) != 2 || !bytes.Equal(replies[1].GetReadResult().GetDocument().GetData(), raw) {
				t.Fatal("independent RPC reads lost identity or quota isolation", finds.Load(), replies)
			}
			if mode == "both retained" {
				if replies[0].GetReadResult().GetDocument() != replies[1].GetReadResult().GetDocument() {
					t.Fatal("same-ID responses did not share immutable data with independent reservations", replies)
				}
			} else {
				code := pb.FailureCode_RESOURCE_EXHAUSTED
				if mode == "first canceled" {
					code = pb.FailureCode_CANCELLED
				}
				if replies[0].GetReadResult().GetFailure().GetCode() != code {
					t.Fatal("failed owner retained peer's data charge", replies)
				}
			}
		})
	}
}

func TestMongoLargeBatchSplitsAtNativeBoundaryWithConfiguredConnectionWorkers(t *testing.T) {
	fixture := testmongo.Open(t)
	var calls, largest atomic.Int32
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "bulkWrite" {
			calls.Add(1)
			largest.Store(max(largest.Load(), int32(len(e.Command))))
		}
	}}
	proxy := testmongo.StartProxy(t, fixture)
	proxy.Monitor = monitor
	settings := backend.DefaultOptions()
	cfg := Config{URI: proxy.URI(), Store: "mongo", Options: &settings, MaxConnecting: 32}
	adapter, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := adapter.Close(); err != nil {
			t.Error(err)
		}
	})
	payload := strings.Repeat("x", 1<<20)
	var plans []*execution.Plan
	for i := range 40 {
		id := strconv.Itoa(i)
		document := bson.D{{Key: "_id", Value: id}, {Key: "payload", Value: payload}}
		opts := batchOperationOptions{resource: fixture.DB + "/records/s:" + id, action: "create", index: uint64(i + 1), document: document}
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	results := adapter.executeRecords(ctx, plans)
	for _, result := range results {
		if result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
			t.Fatal("legal large batch failed", result)
		}
	}
	if calls.Load() < 3 || int(largest.Load()) > adapter.commandBytes() {
		t.Fatal("large batch exceeded native boundary", calls.Load(), largest.Load())
	}
	filter := bson.D{}
	count, err := fixture.Admin.Database(fixture.DB).Collection("records").CountDocuments(ctx, filter)
	if err != nil || count != 40 {
		t.Fatal("large batch effects lost", count, err)
	}
	t.Logf("40 MiB legal input: %d bulk commands, maximum native command %d bytes, all 40 records persisted", calls.Load(), largest.Load())
}
