package mongodb

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

const (
	setOneProgram      = `return function(current, incoming) current.n = 1; return current end`
	rejectSetProgram   = `return function(current, incoming) if current.n > 0 then return weir.reject("already set") end; current.m = 1; return current end`
	loopProgramPattern = `return function(current, incoming) local text = "%s"; while true do local upper = string.upper(text) end end`
)

// mockPrograms prepares programs of records in db/records under the test's
// context.
func mockPrograms(t *testing.T, adapter *Adapter, programs [][2]string) []*execution.Plan {
	t.Helper()
	plans := make([]*execution.Plan, len(programs))
	for i, program := range programs {
		opts := batchOperationOptions{resource: "db/records/s:" + program[0], action: "program", program: program[1]}
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		work.Context = t.Context()
		plans[i] = work
	}
	return plans
}

func TestMongoFoldedPutsRejectedForAnotherReasonDoNotRunAlone(t *testing.T) {
	item := bson.D{{Key: "ok", Value: 0}, {Key: "idx", Value: 0}, {Key: "code", Value: 13}}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: bson.A{item}}}
	reply := bson.D{
		{Key: "ok", Value: 1}, {Key: "cursor", Value: cursor}, {Key: "nErrors", Value: 1},
		{Key: "nInserted", Value: 0}, {Key: "nDeleted", Value: 0}, {Key: "nMatched", Value: 0},
		{Key: "nModified", Value: 0}, {Key: "nUpserted", Value: 0},
	}
	responses := []bson.D{collectionQualificationResponse("db", "records"), reply}
	writes := 0
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "bulkWrite" {
			writes++
		}
	}}
	adapter := batchMockAdapter(t, responses, monitor)
	plans := make([]*execution.Plan, 2)
	for i := range plans {
		document := bson.D{{Key: "_id", Value: "x"}, {Key: "n", Value: int32(i)}}
		opts := batchOperationOptions{resource: "db/records/s:x", action: "put", document: document}
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		work.Context = t.Context()
		plans[i] = work
	}
	for i, event := range adapter.ExecuteRecords(t.Context(), plans) {
		result := event.GetMutationResult()
		if result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_PERMISSION_DENIED {
			t.Fatal("put", i, result)
		}
	}
	if writes != 1 {
		t.Fatal("a permission failure ran the folded puts again one at a time", writes)
	}
}

func TestMongoAnAmbiguousChainCommitFailsItsDependentProgramsOnly(t *testing.T) {
	ambiguousLabels := bson.A{"UnknownTransactionCommitResult"}
	ambiguous := programCommitError(ambiguousLabels)
	document := bson.D{{Key: "_id", Value: "x"}, {Key: "n", Value: int32(0)}}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document}}}
	item := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
	responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor), programBulkResponse(bson.A{item}, 1)}
	for range 5 {
		responses = append(responses, ambiguous)
	}
	adapter := batchMockAdapter(t, responses, nil)
	keep := `return function(current, incoming) return weir.keep() end`
	programs := [][2]string{{"x", keep}, {"x", setOneProgram}, {"x", rejectSetProgram}}
	results := adapter.executePrograms(t.Context(), mockPrograms(t, adapter, programs))
	// The keep was decided on the document as read, whatever the commit did.
	if kept := results[0].GetMutationResult(); kept.GetOutcome() != pb.MutationOutcome_APPLIED || kept.GetFailure() != nil {
		t.Fatal("the keep took the commit's result", kept)
	}
	first, second := results[1].GetMutationResult(), results[2].GetMutationResult()
	if first.GetOutcome() != pb.MutationOutcome_UNKNOWN || first.GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE {
		t.Fatal("the program the write carried", first)
	}
	// The second program was rejected on the document only the unknown
	// write would have made real.
	if second.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || second.GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE {
		t.Fatal("the dependent program kept its rejection", second)
	}
}

func TestMongoAChainWhoseProgramLeftDuringEvaluationRunsAgainWithoutIt(t *testing.T) {
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	initializeTestLifetime(adapter)
	loop := strings.Replace(loopProgramPattern, "%s", strings.Repeat("lowercase", 900), 1)
	programs := [][2]string{{"x", setOneProgram}, {"x", rejectSetProgram}, {"y", loop}}
	plans := mockPrograms(t, adapter, programs)
	// The first program's caller leaves while y's program runs, after x's
	// chain was evaluated.
	caller, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	plans[0].Context, plans[2].Context = caller, caller
	timer := time.AfterFunc(100*time.Millisecond, cancel)
	defer timer.Stop()
	stands, folds := foldPrograms(plans)
	defer folds.release()
	batch := &programBatch{plans: stands, results: make([]*pb.MutationResult, len(stands)), folds: folds}
	var documents []bson.Raw
	for _, id := range []string{"x", "y"} {
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(0)}}
		raw, err := bson.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		documents = append(documents, raw)
	}
	positions := []int{0, 1}
	writes, writePositions := adapter.transformPrograms(t.Context(), batch, positions, documents)
	if len(writes) != 1 || writePositions[0] != 0 {
		t.Fatal("writes", writes, writePositions)
	}
	native := writes[0].Backend.(*plan)
	if native.document.Lookup("n").AsInt64() != 0 || native.document.Lookup("m").AsInt64() != 1 {
		t.Fatal("the departed program's effect was written", native.document)
	}
	own := folds.own[stands[0]]
	if own[0].GetFailure().GetCode() != pb.FailureCode_CANCELLED || own[1] != nil {
		t.Fatal("chain results", own)
	}
}

func TestMongoProgramsWhoseCallersLeftBeforeTheBatchDoNotStart(t *testing.T) {
	document := bson.D{{Key: "_id", Value: "x"}, {Key: "n", Value: int32(0)}}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document}}}
	item := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
	items := bson.A{item}
	success := bson.D{{Key: "ok", Value: 1}}
	responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor), programBulkResponse(items, 1), success}
	var commands []string
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		commands = append(commands, e.CommandName)
	}}
	adapter := batchMockAdapter(t, responses, monitor)
	programs := [][2]string{{"x", setOneProgram}, {"x", setOneProgram}, {"y", setOneProgram}, {"y", setOneProgram}}
	plans := mockPrograms(t, adapter, programs)
	left, cancel := context.WithCancel(t.Context())
	cancel()
	// Every caller of y left, and one of x.
	plans[0].Context, plans[2].Context, plans[3].Context = left, left, left
	results := adapter.executePrograms(t.Context(), plans)
	for _, i := range []int{0, 2, 3} {
		result := results[i].GetMutationResult()
		if result.GetOutcome() != pb.MutationOutcome_NOT_STARTED || result.GetFailure().GetCode() != pb.FailureCode_CANCELLED {
			t.Fatal("program", i, result)
		}
	}
	if result := results[1].GetMutationResult(); result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("the remaining program", result)
	}
	// Only x is read: y has no caller left.
	finds := 0
	for _, name := range commands {
		if name == "find" {
			finds++
		}
	}
	if finds != 1 {
		t.Fatal("the departed chain reached MongoDB", commands)
	}
}

func TestAChainWhoseCallersAllLeftEndsAtOnce(t *testing.T) {
	left, cancel := context.WithCancel(t.Context())
	cancel()
	chain := []*execution.Plan{{Context: left}, {Context: left}}
	ctx, release := chainContext(chain)
	defer release()
	if ctx.Err() == nil {
		t.Fatal("a chain without a remaining caller is live")
	}
}
