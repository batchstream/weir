//go:build integration

package mongodb

import (
	"context"
	"sync/atomic"
	"testing"

	pb "github.com/batchstream/weir/api/weir/v1"
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
		options := batchOperationOptions{resource: "weir://mongo/" + fixture.DB + "/records/s:" + operation.id, action: operation.action, index: uint64(i + 3), document: document, program: `return weir.replace(weir.set(current, "n", weir.add(weir.get(current, "n"), weir.i32("1"))))`}
		work, failure := a.Prepare(batchOperation(t, options))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	replies, _ := a.Execute(context.Background(), plans)
	for i, reply := range replies {
		operation := operations[i]
		if reply.Index != uint64(i+3) {
			t.Fatal("index changed", replies)
		}
		if operation.action == "read" {
			if operation.id == "read" && reply.GetRead().GetDocument() == nil || operation.id == "read-missing" && reply.GetRead().GetMissing() == nil {
				t.Fatal("read result changed", reply)
			}
		} else if reply.GetMutation().Outcome != operation.outcome || operation.outcome == pb.MutationOutcome_NOT_APPLIED && reply.GetMutation().GetFailure().GetCode() != pb.FailureCode_PRECONDITION_FAILED {
			t.Fatal(operation.id, reply)
		}
	}
	if finds.Load() != 2 || bulkWrites.Load() != 1 || transactionWrites.Load() != 1 || commits.Load() != 1 {
		t.Fatal("mixed operations were not physically aggregated", finds.Load(), bulkWrites.Load(), transactionWrites.Load(), commits.Load())
	}
	for _, id := range []string{"put-new", "create-new", "replace", "expression", "program"} {
		filter := bson.D{{Key: "_id", Value: id}}
		raw, err := collection.FindOne(context.Background(), filter).Raw()
		if err != nil || raw.Lookup("n").Int32() != 2 {
			t.Fatal("effect absent", id, err, raw)
		}
	}
	t.Logf("12 mixed records: point read requests=%d verbose bulk writes=%d separate Lua commits=%d", finds.Load()-1, bulkWrites.Load(), commits.Load())
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
		options := batchOperationOptions{resource: "weir://mongo/" + fixture.DB + "/records/s:" + id, action: action, index: uint64(i), document: document, program: "return weir.keep()"}
		p, failure := a.Prepare(batchOperation(t, options))
		if failure != nil {
			t.Fatal(failure)
		}
		if i == 1 || i == 3 {
			p.Context = caller
		}
		plans = append(plans, p)
	}
	replies, _ := a.Execute(context.Background(), plans)
	if replies[0].GetRead().GetMissing() == nil || replies[1].GetMutation().GetOutcome() != pb.MutationOutcome_NOT_STARTED || replies[2].GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED || replies[3].GetMutation().GetOutcome() != pb.MutationOutcome_NOT_STARTED {
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
		options := batchOperationOptions{resource: "weir://mongo/" + fixture.DB + "/records/s:" + id, action: action, index: uint64(i), document: doc}
		work, failure := a.Prepare(batchOperation(t, options))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	replies, _ := a.Execute(context.Background(), plans)
	for _, reply := range replies {
		if reply.GetMutation().GetOutcome() != pb.MutationOutcome_UNKNOWN {
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
