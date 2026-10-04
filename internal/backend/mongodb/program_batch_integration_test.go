//go:build integration

package mongodb

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const incrementProgram = `if weir.kind(current) == "missing" then return weir.replace(weir.object("n", weir.i32("1"))) end return weir.replace(weir.set(current, "n", weir.add(weir.get(current, "n"), weir.i32("1"))))`

func prepareBatchProgram(t *testing.T, adapter *Adapter, opts batchOperationOptions) *execution.Plan {
	t.Helper()
	opts.action = "program"
	work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
	if failure != nil {
		t.Fatal(failure)
	}
	work.Context = t.Context()
	return work
}

func TestMongoLuaBatchUsesOneReadWriteCommitAndPreservesDocuments(t *testing.T) {
	fixture := testmongo.Open(t)
	collection := fixture.Admin.Database(fixture.DB).Collection("records")
	objectID := bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	ids := []any{"a", int32(7), objectID, "delete", "keep", "reject", "invalid"}
	for _, id := range ids {
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(0)}, {Key: "business", Value: true}}
		if _, err := collection.InsertOne(t.Context(), document); err != nil {
			t.Fatal(err)
		}
	}
	var finds, writes, commits atomic.Int32
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		switch e.CommandName {
		case "find":
			finds.Add(1)
		case "bulkWrite":
			writes.Add(1)
			if e.Command.Lookup("ops").Array().Index(4).Document().Lookup("insert").Type == 0 {
				t.Error("replace/create/delete did not share one physical write command")
			}
			if e.Command.Lookup("writeConcern").Type != 0 || e.Command.Lookup("autocommit").Boolean() {
				t.Error("write escaped the shared transaction")
			}
		case "commitTransaction":
			commits.Add(1)
		}
	}}
	adapterOpts := adapterTestOptions{fixture: fixture, monitor: monitor}
	adapter := testAdapter(t, adapterOpts)
	resources := []string{"s:a", "i:7", "oid:" + objectID.Hex(), "s:delete", "s:keep", "s:reject", "s:invalid", "s:new", "s:missing-delete"}
	sources := []string{incrementProgram, incrementProgram, incrementProgram, `return weir.delete()`, `return weir.keep()`, `return weir.reject("business constraint")`, `return print("unavailable")`, incrementProgram, `return weir.delete()`}
	plans := make([]*execution.Plan, len(resources))
	for i, resource := range resources {
		opts := batchOperationOptions{resource: fixture.DB + "/records/" + resource, index: 1, program: sources[i]}
		plans[i] = prepareBatchProgram(t, adapter, opts)
	}
	results, _ := adapter.executePrograms(t.Context(), plans)
	for i, result := range results {
		want := pb.MutationOutcome_APPLIED
		if i == 5 || i == 6 {
			want = pb.MutationOutcome_NOT_APPLIED
		}
		if result.GetMutationResult().GetOutcome() != want {
			t.Fatal("caller association or isolated result changed", i, result)
		}
	}
	if results[5].GetMutationResult().GetFailure().GetCode() != pb.FailureCode_PRECONDITION_FAILED || results[6].GetMutationResult().GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT {
		t.Fatal("Lua failures did not remain independent", results)
	}
	if finds.Load() != 1 || writes.Load() != 1 || commits.Load() != 1 {
		t.Fatal("Lua operations were not physically aggregated", finds.Load(), writes.Load(), commits.Load())
	}
	for i, id := range ids {
		filter := bson.D{{Key: "_id", Value: id}}
		raw, err := collection.FindOne(t.Context(), filter).Raw()
		if i == 3 {
			if err != mongo.ErrNoDocuments {
				t.Fatal("Lua delete absent", raw, err)
			}
			continue
		}
		want := int64(0)
		if i < 3 {
			want = 1
		}
		fields, fieldErr := raw.Elements()
		if err != nil || fieldErr != nil || len(fields) != 3 || raw.Lookup("n").AsInt64() != want || !raw.Lookup("business").Boolean() {
			t.Fatal("business document changed outside the requested mutation", id, raw, err)
		}
		if i == 1 && raw.Lookup("_id").Type != bson.TypeInt32 {
			t.Fatal("numeric identity representation changed", raw)
		}
	}
	filter := bson.D{{Key: "_id", Value: "new"}}
	raw, err := collection.FindOne(t.Context(), filter).Raw()
	fields, fieldErr := raw.Elements()
	if err != nil || fieldErr != nil || len(fields) != 2 || raw.Lookup("n").Int32() != 1 {
		t.Fatal("missing record creation injected fields or failed", raw, err)
	}
	t.Log("9 independent Lua items: one snapshot find, five mixed writes in one bulkWrite, one commit, unchanged identity types and business fields")
}

func TestMongoLuaBatchNativeConflictRecomputesPeers(t *testing.T) {
	for _, mode := range []string{"update", "replace", "delete", "recreate", "missing insert"} {
		t.Run(mode, func(t *testing.T) {
			fixture := testmongo.Open(t)
			collection := fixture.Admin.Database(fixture.DB).Collection("records")
			for _, id := range []string{"counter", "peer"} {
				if id == "counter" && mode == "missing insert" {
					continue
				}
				document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(0)}, {Key: "business", Value: true}}
				if _, err := collection.InsertOne(t.Context(), document); err != nil {
					t.Fatal(err)
				}
			}
			var finds, writes, commits atomic.Int32
			monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
				switch e.CommandName {
				case "find":
					if finds.Add(1) != 1 {
						return
					}
					filter := bson.D{{Key: "_id", Value: "counter"}}
					document := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int32(20)}, {Key: "native", Value: true}, {Key: "business", Value: true}}
					var err error
					switch mode {
					case "missing insert":
						_, err = collection.InsertOne(t.Context(), document)
					case "replace":
						_, err = collection.ReplaceOne(t.Context(), filter, document)
					case "update":
						fields := bson.D{{Key: "n", Value: int32(20)}, {Key: "native", Value: true}}
						update := bson.D{{Key: "$set", Value: fields}}
						_, err = collection.UpdateOne(t.Context(), filter, update)
					case "delete", "recreate":
						_, err = collection.DeleteOne(t.Context(), filter)
						if err == nil && mode == "recreate" {
							_, err = collection.InsertOne(t.Context(), document)
						}
					}
					if err != nil {
						t.Error("external writer", err)
					}
				case "bulkWrite":
					writes.Add(1)
				case "commitTransaction":
					commits.Add(1)
				}
			}}
			adapterOpts := adapterTestOptions{fixture: fixture, monitor: monitor}
			adapter := testAdapter(t, adapterOpts)
			var plans []*execution.Plan
			for _, id := range []string{"counter", "peer"} {
				opts := batchOperationOptions{resource: fixture.DB + "/records/s:" + id, program: incrementProgram}
				plans = append(plans, prepareBatchProgram(t, adapter, opts))
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			results, _ := adapter.executePrograms(ctx, plans)
			for _, result := range results {
				if result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
					t.Fatal("confirmed conflict was not recomputed", result)
				}
			}
			if finds.Load() < 2 || writes.Load() < 1 || commits.Load() != 1 {
				t.Fatal("fresh transaction did not reread the batch", finds.Load(), writes.Load(), commits.Load())
			}
			for _, id := range []string{"counter", "peer"} {
				filter := bson.D{{Key: "_id", Value: id}}
				raw, err := collection.FindOne(t.Context(), filter).Raw()
				want := int64(1)
				if id == "counter" && mode != "delete" {
					want = 21
				}
				if err != nil || raw.Lookup("n").AsInt64() != want {
					t.Fatal("external change lost or peer replayed", id, raw, err)
				}
				if id == "peer" || mode != "delete" {
					if !raw.Lookup("business").Boolean() || id == "counter" && !raw.Lookup("native").Boolean() {
						t.Fatal("Lua replaced unrelated native fields", id, raw)
					}
				}
			}
		})
	}
}

func TestMongoLuaBatchCommitReplyLossNeverReapplies(t *testing.T) {
	for _, mode := range []string{"once", "all"} {
		t.Run(mode, func(t *testing.T) {
			fixture := testmongo.Open(t)
			collection := fixture.Admin.Database(fixture.DB).Collection("records")
			for _, id := range []string{"a", "b"} {
				document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(0)}}
				if _, err := collection.InsertOne(t.Context(), document); err != nil {
					t.Fatal(err)
				}
			}
			proxy := testmongo.StartProxy(t, fixture)
			proxy.DropCommand = "commitTransaction"
			proxy.DropRemaining.Store(1)
			if mode == "all" {
				proxy.DropRemaining.Store(1 << 60)
			}
			adapterOpts := adapterTestOptions{fixture: fixture, uri: proxy.URI()}
			adapter := testAdapter(t, adapterOpts)
			var plans []*execution.Plan
			for _, id := range []string{"a", "b"} {
				opts := batchOperationOptions{resource: fixture.DB + "/records/s:" + id, program: incrementProgram}
				plans = append(plans, prepareBatchProgram(t, adapter, opts))
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			results, _ := adapter.executePrograms(ctx, plans)
			want := pb.MutationOutcome_APPLIED
			if mode == "all" {
				want = pb.MutationOutcome_UNKNOWN
			}
			for _, result := range results {
				if result.GetMutationResult().GetOutcome() != want {
					t.Fatal("commit ambiguity misclassified", mode, result)
				}
			}
			finds, writes, commits, dropped := 0, 0, 0, 0
			var transaction int64
			session := ""
			for _, e := range proxy.Events() {
				switch e.Command {
				case "find":
					finds++
				case "bulkWrite":
					writes++
				case "commitTransaction":
					commits++
					if e.Dropped && e.Acknowledged {
						dropped++
					}
					if session == "" {
						session, transaction = e.Session, e.Transaction
					} else if session != e.Session || transaction != e.Transaction {
						t.Fatal("ambiguous commit switched transaction", e)
					}
				}
			}
			if finds != 1 || writes != 1 || commits < 2 || commits > 2*programAttempts || dropped == 0 {
				t.Fatal("transform/write replayed or ambiguity was not exercised", finds, writes, commits, dropped)
			}
			for _, id := range []string{"a", "b"} {
				filter := bson.D{{Key: "_id", Value: id}}
				raw, err := collection.FindOne(t.Context(), filter).Raw()
				if err != nil || raw.Lookup("n").Int32() != 1 {
					t.Fatal("committed batch was reapplied", raw, err)
				}
			}
			t.Logf("%s: find=%d bulkWrite=%d same-transaction commits=%d persisted increments=1 outcome=%s", mode, finds, writes, commits, want)
		})
	}
}

func TestMongoLuaBatchSchemaRejectionRollsBackThenIsolatesItem(t *testing.T) {
	fixture := testmongo.Open(t)
	collection := fixture.Admin.Database(fixture.DB).Collection("records")
	validator := bson.D{{Key: "n", Value: bson.D{{Key: "$gte", Value: 0}}}}
	command := bson.D{{Key: "collMod", Value: "records"}, {Key: "validator", Value: validator}}
	if err := fixture.Admin.Database(fixture.DB).RunCommand(t.Context(), command).Err(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"before", "invalid", "after"} {
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(0)}}
		if _, err := collection.InsertOne(t.Context(), document); err != nil {
			t.Fatal(err)
		}
	}
	adapterOpts := adapterTestOptions{fixture: fixture}
	adapter := testAdapter(t, adapterOpts)
	var plans []*execution.Plan
	for _, id := range []string{"before", "invalid", "after"} {
		source := incrementProgram
		if id == "invalid" {
			source = `return weir.replace(weir.set(current, "n", weir.i32("-1")))`
		}
		opts := batchOperationOptions{resource: fixture.DB + "/records/s:" + id, program: source}
		plans = append(plans, prepareBatchProgram(t, adapter, opts))
	}
	results, _ := adapter.executePrograms(t.Context(), plans)
	for i, id := range []string{"before", "invalid", "after"} {
		want := pb.MutationOutcome_APPLIED
		value := int32(1)
		if id == "invalid" {
			want, value = pb.MutationOutcome_NOT_APPLIED, 0
		}
		filter := bson.D{{Key: "_id", Value: id}}
		raw, err := collection.FindOne(t.Context(), filter).Raw()
		if results[i].GetMutationResult().GetOutcome() != want || err != nil || raw.Lookup("n").Int32() != value {
			t.Fatal("rollback/isolation lost a peer or replayed the preceding item", id, results[i], raw, err)
		}
	}
}

func TestMongoLuaBatchLostAbortAcknowledgementNeverRebuilds(t *testing.T) {
	fixture := testmongo.Open(t)
	collection := fixture.Admin.Database(fixture.DB).Collection("records")
	validator := bson.D{{Key: "n", Value: bson.D{{Key: "$gte", Value: 0}}}}
	command := bson.D{{Key: "collMod", Value: "records"}, {Key: "validator", Value: validator}}
	if err := fixture.Admin.Database(fixture.DB).RunCommand(t.Context(), command).Err(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"before", "invalid", "after"} {
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(0)}}
		if _, err := collection.InsertOne(t.Context(), document); err != nil {
			t.Fatal(err)
		}
	}
	proxy := testmongo.StartProxy(t, fixture)
	proxy.DropCommand = "abortTransaction"
	proxy.DropRemaining.Store(1)
	adapterOpts := adapterTestOptions{fixture: fixture, uri: proxy.URI()}
	adapter := testAdapter(t, adapterOpts)
	var plans []*execution.Plan
	for _, id := range []string{"before", "invalid", "after"} {
		source := incrementProgram
		if id == "invalid" {
			source = `return weir.replace(weir.set(current, "n", weir.i32("-1")))`
		}
		opts := batchOperationOptions{resource: fixture.DB + "/records/s:" + id, program: source}
		plans = append(plans, prepareBatchProgram(t, adapter, opts))
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	start := time.Now()
	results, _ := adapter.executePrograms(ctx, plans)
	if time.Since(start) > 1500*time.Millisecond {
		t.Fatal("unconfirmed rollback cleanup exceeded its bound", time.Since(start))
	}
	for _, result := range results {
		mutation := result.GetMutationResult()
		if mutation.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || mutation.GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE || mutation.GetFailure().GetMessage() != "MongoDB transaction rollback unconfirmed" {
			t.Fatal("lost rollback acknowledgement allowed transaction rebuild", mutation)
		}
	}
	finds, writes, aborts, commits := 0, 0, 0, 0
	var transaction int64
	session := ""
	for _, e := range proxy.Events() {
		switch e.Command {
		case "find":
			finds++
			session, transaction = e.Session, e.Transaction
		case "bulkWrite":
			writes++
		case "abortTransaction":
			aborts++
			if !e.Dropped || e.Session != session || e.Transaction != transaction {
				t.Fatal("fault did not drop the actual transaction abort reply", e)
			}
		case "commitTransaction":
			commits++
		}
	}
	if finds != 1 || writes != 1 || aborts != 1 || commits != 0 {
		t.Fatal("unconfirmed rollback caused a new transaction or duplicate abort", finds, writes, aborts, commits)
	}
	for _, id := range []string{"before", "invalid", "after"} {
		filter := bson.D{{Key: "_id", Value: id}}
		raw, err := collection.FindOne(t.Context(), filter).Raw()
		if err != nil || raw.Lookup("n").Int32() != 0 {
			t.Fatal("uncommitted transaction changed a business document", id, raw, err)
		}
	}
	t.Log("actual abort reply dropped: one read, one attempted bulkWrite, one same-transaction abort, zero commits, zero replays, all documents unchanged")
}

func TestMongoLuaBatchRetainedBoundSplitsWithoutChangingEffects(t *testing.T) {
	fixture := testmongo.Open(t)
	collection := fixture.Admin.Database(fixture.DB).Collection("records")
	padding := make([]byte, 220<<10)
	ids := make([]string, 40)
	for i := range ids {
		ids[i] = fmt.Sprintf("item-%02d", i)
	}
	for _, id := range ids {
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(0)}, {Key: "padding", Value: padding}}
		if _, err := collection.InsertOne(t.Context(), document); err != nil {
			t.Fatal(err)
		}
	}
	var finds, writes, commits atomic.Int32
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		switch e.CommandName {
		case "find":
			finds.Add(1)
		case "bulkWrite":
			writes.Add(1)
		case "commitTransaction":
			commits.Add(1)
		}
	}}
	adapterOpts := adapterTestOptions{fixture: fixture, monitor: monitor}
	adapter := testAdapter(t, adapterOpts)
	var plans []*execution.Plan
	for _, id := range ids {
		opts := batchOperationOptions{resource: fixture.DB + "/records/s:" + id, program: incrementProgram}
		plans = append(plans, prepareBatchProgram(t, adapter, opts))
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	results, _ := adapter.executePrograms(ctx, plans)
	for i, result := range results {
		if result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
			t.Fatal("bounded batch rejected a valid individual document", i, result)
		}
		filter := bson.D{{Key: "_id", Value: ids[i]}}
		raw, err := collection.FindOne(t.Context(), filter).Raw()
		if err != nil || raw.Lookup("n").Int32() != 1 || len(raw.Lookup("padding").Value) < len(padding) {
			t.Fatal("split changed the requested effect", i, raw.Lookup("n"), err)
		}
	}
	if finds.Load() != 3 || writes.Load() != 2 || commits.Load() != 2 {
		t.Fatal("retained source bound did not split the uncommitted batch", finds.Load(), writes.Load(), commits.Load())
	}
}

func TestMongoLuaConflictRetriesStopAtAttemptBound(t *testing.T) {
	fixture := testmongo.Open(t)
	collection := fixture.Admin.Database(fixture.DB).Collection("records")
	document := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int32(0)}}
	if _, err := collection.InsertOne(t.Context(), document); err != nil {
		t.Fatal(err)
	}
	filter := bson.D{{Key: "_id", Value: "counter"}}
	increment := bson.D{{Key: "n", Value: int32(1)}}
	update := bson.D{{Key: "$inc", Value: increment}}
	var reads, writes, commits, aborts atomic.Int32
	var transactions []int64
	monitor := &event.CommandMonitor{
		Started: func(_ context.Context, command *event.CommandStartedEvent) {
			switch command.CommandName {
			case "find":
				reads.Add(1)
				transactions = append(transactions, command.Command.Lookup("txnNumber").Int64())
			case "bulkWrite":
				writes.Add(1)
			case "commitTransaction":
				commits.Add(1)
			case "abortTransaction":
				aborts.Add(1)
			}
		},
		Succeeded: func(_ context.Context, command *event.CommandSucceededEvent) {
			if command.CommandName == "find" {
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				if _, err := collection.UpdateOne(ctx, filter, update); err != nil {
					t.Error("external conflicting write", err)
				}
			}
		},
	}
	adapterOptions := adapterTestOptions{fixture: fixture, monitor: monitor}
	adapter := testAdapter(t, adapterOptions)
	operation := batchOperationOptions{resource: fixture.DB + "/records/s:counter", program: incrementProgram}
	work := prepareBatchProgram(t, adapter, operation)
	plans := []*execution.Plan{work}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	results, _ := adapter.executePrograms(ctx, plans)
	result := results[0].GetMutationResult()
	if result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_CONFLICT || reads.Load() != programAttempts || writes.Load() != programAttempts || aborts.Load() != programAttempts || commits.Load() != 0 {
		t.Fatal("conflicts escaped the retry bound or committed a stale value", result, reads.Load(), writes.Load(), aborts.Load(), commits.Load())
	}
	for index := 1; index < len(transactions); index++ {
		if transactions[index] <= transactions[index-1] {
			t.Fatal("conflict reused a previous transaction snapshot", transactions)
		}
	}
	raw, err := collection.FindOne(ctx, filter).Raw()
	if err != nil || raw.Lookup("n").Int32() != programAttempts {
		t.Fatal("aborted Lua writes changed the native writer's counter", raw, err)
	}
	canceled, stop := context.WithCancel(t.Context())
	stop()
	results, _ = adapter.executePrograms(canceled, plans)
	if results[0].GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_STARTED || reads.Load() != programAttempts || writes.Load() != programAttempts {
		t.Fatal("cancelled caller started a new transaction", results[0], reads.Load(), writes.Load())
	}
}

func TestMongoLuaCommitCancellationKeepsOriginalDeadline(t *testing.T) {
	fixture := testmongo.Open(t)
	var reads, writes, commits atomic.Int32
	monitor := &event.CommandMonitor{Started: func(_ context.Context, command *event.CommandStartedEvent) {
		switch command.CommandName {
		case "find":
			reads.Add(1)
		case "bulkWrite":
			writes.Add(1)
		case "commitTransaction":
			commits.Add(1)
		}
	}}
	adapterOptions := adapterTestOptions{fixture: fixture, monitor: monitor}
	adapter := testAdapter(t, adapterOptions)
	operation := batchOperationOptions{resource: fixture.DB + "/records/s:deadline", program: incrementProgram}
	work := prepareBatchProgram(t, adapter, operation)
	data := bson.D{{Key: "failCommands", Value: bson.A{"commitTransaction"}}, {Key: "appName", Value: "weir:mongo"}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 250}}
	testmongo.FailCommand(t, fixture.Admin, data, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Millisecond)
	defer cancel()
	started := time.Now()
	plans := []*execution.Plan{work}
	results, _ := adapter.executePrograms(ctx, plans)
	elapsed := time.Since(started)
	result := results[0].GetMutationResult()
	if result.GetOutcome() != pb.MutationOutcome_UNKNOWN || reads.Load() != 1 || writes.Load() != 1 || commits.Load() != 1 || elapsed > 350*time.Millisecond {
		t.Fatal("commit cancellation changed the deadline or replayed Lua", result, reads.Load(), writes.Load(), commits.Load(), elapsed)
	}
}

func TestMongoLuaStableUniqueConflictDoesNotRecomputeIndefinitely(t *testing.T) {
	fixture := testmongo.Open(t)
	collection := fixture.Admin.Database(fixture.DB).Collection("records")
	keys := bson.D{{Key: "n", Value: 1}}
	index := mongo.IndexModel{Keys: keys, Options: options.Index().SetUnique(true)}
	if _, err := collection.Indexes().CreateOne(t.Context(), index); err != nil {
		t.Fatal(err)
	}
	document := bson.D{{Key: "_id", Value: "existing"}, {Key: "n", Value: int32(1)}, {Key: "business", Value: true}}
	if _, err := collection.InsertOne(t.Context(), document); err != nil {
		t.Fatal(err)
	}
	var reads, writes, commits atomic.Int32
	monitor := &event.CommandMonitor{Started: func(_ context.Context, command *event.CommandStartedEvent) {
		switch command.CommandName {
		case "find":
			reads.Add(1)
		case "bulkWrite":
			writes.Add(1)
		case "commitTransaction":
			commits.Add(1)
		}
	}}
	adapterOptions := adapterTestOptions{fixture: fixture, monitor: monitor}
	adapter := testAdapter(t, adapterOptions)
	operation := batchOperationOptions{resource: fixture.DB + "/records/s:new", program: incrementProgram}
	work := prepareBatchProgram(t, adapter, operation)
	plans := []*execution.Plan{work}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	results, _ := adapter.executePrograms(ctx, plans)
	result := results[0].GetMutationResult()
	if result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || reads.Load() < 1 || reads.Load() > 2 || writes.Load() != reads.Load() || commits.Load() != 0 {
		t.Fatal("stable nonidentity constraint kept rebuilding transactions", result, reads.Load(), writes.Load(), commits.Load())
	}
	filter := bson.D{{Key: "_id", Value: "existing"}}
	raw, err := collection.FindOne(ctx, filter).Raw()
	if err != nil || raw.Lookup("n").Int32() != 1 || !raw.Lookup("business").Boolean() {
		t.Fatal("rejected transform changed the conflicting document", raw, err)
	}
	filter = bson.D{{Key: "_id", Value: "new"}}
	if err := collection.FindOne(ctx, filter).Err(); err != mongo.ErrNoDocuments {
		t.Fatal("rejected missing insert persisted", err)
	}
}

func TestMongoLuaCloseDuringCommitKeepsUnknownOutcome(t *testing.T) {
	fixture := testmongo.Open(t)
	proxy := testmongo.StartProxy(t, fixture)
	proxy.DropCommand = "commitTransaction"
	proxy.DropRemaining.Store(1)
	gate := make(chan struct{})
	proxy.DropGate = gate
	defer close(gate)
	adapterOptions := adapterTestOptions{fixture: fixture, uri: proxy.URI()}
	adapter := testAdapter(t, adapterOptions)
	operation := batchOperationOptions{resource: fixture.DB + "/records/s:closing", program: incrementProgram}
	work := prepareBatchProgram(t, adapter, operation)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	plans := []*execution.Plan{work}
	done := make(chan []*pb.Event, 1)
	go func() {
		results, _ := adapter.executePrograms(ctx, plans)
		done <- results
	}()
	// Wait for a real successful server reply retained by the proxy. The driver
	// command-start callback alone does not prove a commit reached the database.
	committed := false
	for !committed {
		for _, event := range proxy.Events() {
			if event.Command == "commitTransaction" && event.Acknowledged && event.Dropped {
				committed = true
				break
			}
		}
		if !committed {
			select {
			case <-ctx.Done():
				t.Fatal("commit did not reach the server", ctx.Err())
			case <-time.After(time.Millisecond):
			}
		}
	}
	start := time.Now()
	cancel()
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case results := <-done:
		if result := results[0].GetMutationResult(); result.GetOutcome() != pb.MutationOutcome_UNKNOWN {
			t.Fatal("close inferred rollback from a missing commit reply", result)
		}
	case <-time.After(time.Second):
		t.Fatal("transaction execution survived close")
	}
	if time.Since(start) > 300*time.Millisecond {
		t.Fatal("adapter close did not reclaim transaction execution promptly", time.Since(start))
	}
	reads, writes, commits := 0, 0, 0
	var transaction int64
	session := ""
	for _, event := range proxy.Events() {
		switch event.Command {
		case "find":
			reads++
		case "bulkWrite":
			writes++
		case "commitTransaction":
			commits++
		default:
			continue
		}
		if session == "" {
			session, transaction = event.Session, event.Transaction
		} else if session != event.Session || transaction != event.Transaction {
			t.Fatal("close restarted the transaction", event)
		}
	}
	if reads != 1 || writes != 1 || commits != 1 {
		t.Fatal("close replayed Lua or commit", reads, writes, commits)
	}
	filter := bson.D{{Key: "_id", Value: "closing"}}
	raw, err := fixture.Admin.Database(fixture.DB).Collection("records").FindOne(t.Context(), filter).Raw()
	if err != nil || raw.Lookup("n").Int32() != 1 {
		t.Fatal("intercepted commit did not persist once", raw, err)
	}
	if err := adapter.client.Ping(t.Context(), nil); err != mongo.ErrClientDisconnected {
		t.Fatal("closed adapter retained its client", err)
	}
}
