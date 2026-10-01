package mongodb

import (
	"context"
	"testing"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/luaengine"
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
	success := bson.D{{Key: "ok", Value: 1}}
	writeReply := bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
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
				case "update":
					replacement := ev.Command.Lookup("updates").Array().Index(0).Document().Lookup("u").Document()
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
			program := &luaengine.Program{Source: `return weir.replace(weir.set(current, "n", weir.add(weir.get(current, "n"), weir.i32("1"))))`}
			target := namespace{database: "db", collection: "records"}
			native := &plan{target: target, id: "item", program: program}
			result, _ := a.runProgram(context.Background(), native)
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
