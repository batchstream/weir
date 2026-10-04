package mongodb

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
)

func TestMongoProgramCommitRetries(t *testing.T) {
	transientLabels := bson.A{"TransientTransactionError"}
	transient := programCommitError(transientLabels)
	ambiguousLabels := bson.A{"UnknownTransactionCommitResult"}
	ambiguous := programCommitError(ambiguousLabels)
	bothLabels := bson.A{"TransientTransactionError", "UnknownTransactionCommitResult"}
	both := programCommitError(bothLabels)
	definite := bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 2}, {Key: "errmsg", Value: "definite commit rejection"}}
	success := bson.D{{Key: "ok", Value: 1}}
	writeItem := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
	writeReply := programBulkResponse(bson.A{writeItem}, 1)
	cases := []struct {
		name       string
		commits    []bson.D
		reevaluate bool
		outcome    pb.MutationOutcome
		failure    pb.FailureCode
	}{
		{name: "transient commit rereads", commits: []bson.D{transient, success}, reevaluate: true, outcome: pb.MutationOutcome_APPLIED},
		{name: "transient retry limit", commits: []bson.D{transient, transient, transient, transient, transient}, reevaluate: true, outcome: pb.MutationOutcome_NOT_APPLIED, failure: pb.FailureCode_CONFLICT},
		{name: "ambiguous commit retries only commit", commits: []bson.D{ambiguous, success}, outcome: pb.MutationOutcome_APPLIED},
		{name: "ambiguity remains after transient errors", commits: []bson.D{ambiguous, transient, transient, transient, transient}, outcome: pb.MutationOutcome_UNKNOWN, failure: pb.FailureCode_UNAVAILABLE},
		{name: "ambiguity remains after definite rejection", commits: []bson.D{ambiguous, definite, definite, definite, definite}, outcome: pb.MutationOutcome_UNKNOWN, failure: pb.FailureCode_UNAVAILABLE},
		{name: "ambiguity takes precedence over transient label", commits: []bson.D{both, success}, outcome: pb.MutationOutcome_APPLIED},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			qualification := collectionQualificationResponse("db", "records")
			responses := []bson.D{qualification}
			var wantCounts []int32
			for attempt, commit := range tc.commits {
				if attempt == 0 || tc.reevaluate {
					count := int32(1)
					if attempt > 0 {
						count = int32(attempt * 10)
					}
					document := bson.D{{Key: "_id", Value: "item"}, {Key: "n", Value: count}}
					batch := bson.A{document}
					cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: batch}}
					findReply := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: cursor}}
					responses = append(responses, findReply, writeReply)
					wantCounts = append(wantCounts, count+1)
				}
				responses = append(responses, commit)
			}
			var transactions, commitTransactions []int64
			var counts []int32
			aborts := 0
			monitor := &event.CommandMonitor{Started: func(ctx context.Context, ev *event.CommandStartedEvent) {
				switch ev.CommandName {
				case "find":
					transactions = append(transactions, ev.Command.Lookup("txnNumber").Int64())
				case "bulkWrite":
					replacement := ev.Command.Lookup("ops").Array().Index(0).Document().Lookup("updateMods").Document()
					counts = append(counts, replacement.Lookup("n").Int32())
				case "commitTransaction":
					commitTransactions = append(commitTransactions, ev.Command.Lookup("txnNumber").Int64())
				case "abortTransaction":
					aborts++
				}
			}}
			deployment := drivertest.NewMockDeployment(responses...)
			opts := options.Client().SetRetryWrites(false).SetRetryReads(false).SetMaxAdaptiveRetries(0).SetMonitor(monitor)
			opts.Deployment = deployment
			client, err := mongo.Connect(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Disconnect(context.Background())
			a := &Adapter{client: client}
			a.config.Store = "mongo"
			operationOpts := batchOperationOptions{resource: "db/records/s:item", action: "program", program: `return weir.replace(weir.set(current, "n", weir.add(weir.get(current, "n"), weir.i32("1"))))`}
			work, failure := prepareTestRecord(a, batchOperation(t, operationOpts))
			if failure != nil {
				t.Fatal(failure)
			}
			results, _ := a.executePrograms(t.Context(), []*execution.Plan{work})
			result := results[0].GetMutationResult()
			if result.GetOutcome() != tc.outcome || result.GetFailure().GetCode() != tc.failure {
				t.Fatalf("unexpected result: %v", result)
			}
			if len(transactions) != len(wantCounts) || len(counts) != len(wantCounts) || len(commitTransactions) != len(tc.commits) || aborts != 0 {
				t.Fatalf("unexpected retries: reads=%d replacements=%v commits=%d aborts=%d", len(transactions), counts, len(commitTransactions), aborts)
			}
			for i, count := range counts {
				if count != wantCounts[i] {
					t.Fatalf("attempt %d reused a stale value: got %d, want %d", i, count, wantCounts[i])
				}
				if i > 0 && transactions[i] <= transactions[i-1] {
					t.Fatal("retry reused the previous transaction")
				}
			}
			for i, transaction := range commitTransactions {
				want := transactions[0]
				if tc.reevaluate {
					want = transactions[i]
				}
				if transaction != want {
					t.Fatal("commit retry changed transaction identity")
				}
			}
		})
	}
}

func programCommitError(labels bson.A) bson.D {
	response := bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 251}, {Key: "codeName", Value: "NoSuchTransaction"}, {Key: "errmsg", Value: "transaction aborted"}, {Key: "errorLabels", Value: labels}}
	return response
}

func programBulkResponse(items bson.A, matched int) bson.D {
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: items}}
	response := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: cursor}, {Key: "nErrors", Value: 0}, {Key: "nInserted", Value: 0}, {Key: "nDeleted", Value: 0}, {Key: "nMatched", Value: matched}, {Key: "nModified", Value: matched}, {Key: "nUpserted", Value: 0}}
	return response
}

func TestMongoLuaBatchCallerCancellationDoesNotCancelPeers(t *testing.T) {
	for _, phase := range []string{"read", "write"} {
		t.Run(phase, func(t *testing.T) {
			caller, cancel := context.WithCancel(t.Context())
			defer cancel()
			documents := bson.A{
				bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: int32(0)}},
				bson.D{{Key: "_id", Value: "b"}, {Key: "n", Value: int32(0)}},
			}
			cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: documents}}
			items := bson.A{bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}}
			if phase == "write" {
				item := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 1}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
				items = append(items, item)
			}
			success := bson.D{{Key: "ok", Value: 1}}
			responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor), programBulkResponse(items, len(items)), success}
			commits := 0
			monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
				if phase == "read" && e.CommandName == "find" || phase == "write" && e.CommandName == "bulkWrite" {
					cancel()
				}
				if e.CommandName == "commitTransaction" {
					commits++
				}
			}}
			adapter := batchMockAdapter(t, responses, monitor)
			var plans []*execution.Plan
			for _, id := range []string{"a", "b"} {
				opts := batchOperationOptions{resource: "db/records/s:" + id, action: "program", program: `return weir.replace(weir.set(current, "n", weir.i32("1")))`}
				work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
				if failure != nil {
					t.Fatal(failure)
				}
				work.Context = t.Context()
				plans = append(plans, work)
			}
			plans[0].Context = caller
			results, _ := adapter.executePrograms(t.Context(), plans)
			want := pb.MutationOutcome_APPLIED
			if phase == "read" {
				want = pb.MutationOutcome_NOT_APPLIED
				if results[0].GetMutationResult().GetFailure().GetCode() != pb.FailureCode_CANCELLED {
					t.Fatal("pre-write cancellation was not isolated", results)
				}
			}
			if results[0].GetMutationResult().GetOutcome() != want || results[1].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || commits != 1 {
				t.Fatal("caller cancellation poisoned the shared transaction", phase, results, commits)
			}
		})
	}
}

func TestMongoLuaBatchMalformedWriteAcknowledgementNeverCommits(t *testing.T) {
	document := bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: int32(0)}}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document}}}
	item := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
	malformed := programBulkResponse(bson.A{item}, 0)
	success := bson.D{{Key: "ok", Value: 1}}
	responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor), malformed, success}
	commits, aborts := 0, 0
	monitor := &event.CommandMonitor{Started: func(ctx context.Context, e *event.CommandStartedEvent) {
		if ctx.Err() != nil {
			return
		}
		if e.CommandName == "commitTransaction" {
			commits++
		}
		if e.CommandName == "abortTransaction" {
			aborts++
		}
	}}
	adapter := batchMockAdapter(t, responses, monitor)
	opts := batchOperationOptions{resource: "db/records/s:a", action: "program", program: `return weir.replace(weir.set(current, "n", weir.i32("1")))`}
	work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
	if failure != nil {
		t.Fatal(failure)
	}
	results, _ := adapter.executePrograms(t.Context(), []*execution.Plan{work})
	if results[0].GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_APPLIED || commits != 0 || aborts != 1 {
		t.Fatal("inconsistent batch acknowledgement was committed", results, commits, aborts)
	}
}

func TestMongoLuaBatchCursorContinuationsKeepTransactionIdentity(t *testing.T) {
	firstDocument := bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: int32(0)}}
	secondDocument := bson.D{{Key: "_id", Value: "b"}, {Key: "n", Value: int32(0)}}
	readFirstCursor := bson.D{{Key: "id", Value: int64(123)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{firstDocument}}}
	readNextCursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "nextBatch", Value: bson.A{secondDocument}}}
	firstItem := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
	secondItem := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 1}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
	writeFirst := programBulkResponse(bson.A{firstItem}, 2)
	writeFirst[1].Value = bson.D{{Key: "id", Value: int64(456)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: bson.A{firstItem}}}
	writeNextCursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "nextBatch", Value: bson.A{secondItem}}}
	success := bson.D{{Key: "ok", Value: 1}}
	responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(readFirstCursor), readCursorResponse(readNextCursor), writeFirst, readCursorResponse(writeNextCursor), success}
	var session bson.Raw
	var transaction int64
	commands := make([]string, 0)
	monitor := &event.CommandMonitor{Started: func(ctx context.Context, e *event.CommandStartedEvent) {
		if ctx.Err() != nil {
			return
		}
		switch e.CommandName {
		case "find", "getMore", "bulkWrite", "commitTransaction":
		default:
			return
		}
		commands = append(commands, e.CommandName)
		id := e.Command.Lookup("lsid").Document()
		txn := e.Command.Lookup("txnNumber").Int64()
		if session == nil {
			session, transaction = append(bson.Raw(nil), id...), txn
		} else if !bytes.Equal(session, id) || transaction != txn {
			t.Error("cursor continuation changed the session or transaction", e.CommandName)
		}
		if e.CommandName == "getMore" {
			cursor := e.Command.Lookup("getMore").Int64()
			collection := e.Command.Lookup("collection").StringValue()
			if cursor == 123 && (e.DatabaseName != "db" || collection != "records") || cursor == 456 && (e.DatabaseName != "admin" || collection != "$cmd.bulkWrite") || cursor != 123 && cursor != 456 {
				t.Error("continuation targeted an unrelated cursor", e.DatabaseName, collection, cursor)
			}
		}
	}}
	adapter := batchMockAdapter(t, responses, monitor)
	var plans []*execution.Plan
	for _, id := range []string{"a", "b"} {
		opts := batchOperationOptions{resource: "db/records/s:" + id, action: "program", program: `return weir.replace(weir.set(current, "n", weir.i32("1")))`}
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	results, _ := adapter.executePrograms(t.Context(), plans)
	if fmt.Sprint(commands) != "[find getMore bulkWrite getMore commitTransaction]" {
		t.Fatal("read/write continuations were not completed before commit", commands)
	}
	for _, result := range results {
		if result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
			t.Fatal("continued transaction did not apply both independent items", results)
		}
	}
}

func TestMongoLuaBatchMissingWriteItemAcknowledgementAborts(t *testing.T) {
	documents := bson.A{
		bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: int32(0)}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "n", Value: int32(0)}},
	}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: documents}}
	item := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
	success := bson.D{{Key: "ok", Value: 1}}
	responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor), programBulkResponse(bson.A{item}, 1), success}
	commits, aborts := 0, 0
	monitor := &event.CommandMonitor{Started: func(ctx context.Context, e *event.CommandStartedEvent) {
		if ctx.Err() != nil {
			return
		}
		if e.CommandName == "commitTransaction" {
			commits++
		}
		if e.CommandName == "abortTransaction" {
			aborts++
		}
	}}
	adapter := batchMockAdapter(t, responses, monitor)
	var plans []*execution.Plan
	for _, id := range []string{"a", "b"} {
		opts := batchOperationOptions{resource: "db/records/s:" + id, action: "program", program: `return weir.replace(weir.set(current, "n", weir.i32("1")))`}
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	results, _ := adapter.executePrograms(t.Context(), plans)
	for _, result := range results {
		if result.GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_APPLIED {
			t.Fatal("incomplete verbose acknowledgement became applied", results)
		}
	}
	if commits != 0 || aborts != 1 {
		t.Fatal("incomplete write acknowledgement was committed or replayed", commits, aborts)
	}
}

func TestMongoLuaBatchUnconfirmedAbortNeverRebuilds(t *testing.T) {
	for _, mode := range []string{"lost reply", "rejected abort"} {
		t.Run(mode, func(t *testing.T) {
			documents := bson.A{
				bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: int32(0)}},
				bson.D{{Key: "_id", Value: "b"}, {Key: "n", Value: int32(0)}},
			}
			cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: documents}}
			item := bson.D{{Key: "ok", Value: 0}, {Key: "idx", Value: 0}, {Key: "code", Value: 121}, {Key: "errmsg", Value: "document rejected"}}
			write := programBulkResponse(bson.A{item}, 0)
			write[2].Value = 1
			responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor), write}
			if mode == "rejected abort" {
				rejection := bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 11600}, {Key: "errmsg", Value: "abort unavailable"}}
				responses = append(responses, rejection)
			}
			finds, writes, aborts, commits := 0, 0, 0, 0
			var session bson.Raw
			var transaction int64
			monitor := &event.CommandMonitor{Started: func(ctx context.Context, e *event.CommandStartedEvent) {
				if ctx.Err() != nil {
					return
				}
				switch e.CommandName {
				case "find":
					finds++
					session = append(bson.Raw(nil), e.Command.Lookup("lsid").Document()...)
					transaction = e.Command.Lookup("txnNumber").Int64()
				case "bulkWrite":
					writes++
				case "abortTransaction":
					aborts++
					if !bytes.Equal(session, e.Command.Lookup("lsid").Document()) || transaction != e.Command.Lookup("txnNumber").Int64() {
						t.Error("abort did not identify the transaction being rolled back")
					}
				case "commitTransaction":
					commits++
				}
			}}
			adapter := batchMockAdapter(t, responses, monitor)
			var plans []*execution.Plan
			for _, id := range []string{"a", "b"} {
				opts := batchOperationOptions{resource: "db/records/s:" + id, action: "program", program: `return weir.replace(weir.set(current, "n", weir.i32("1")))`}
				work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
				if failure != nil {
					t.Fatal(failure)
				}
				plans = append(plans, work)
			}
			results, _ := adapter.executePrograms(t.Context(), plans)
			for _, result := range results {
				mutation := result.GetMutationResult()
				if mutation.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || mutation.GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE || mutation.GetFailure().GetMessage() != "MongoDB transaction rollback unconfirmed" {
					t.Fatal("unobserved rollback became a confirmed abort", mode, mutation)
				}
			}
			if finds != 1 || writes != 1 || aborts != 1 || commits != 0 {
				t.Fatal("unconfirmed abort retried/rebuilt the transaction or sent duplicate aborts", finds, writes, aborts, commits)
			}
		})
	}
}
