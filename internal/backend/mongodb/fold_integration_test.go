//go:build integration

package mongodb

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const rejectProgram = `return function(current, incoming) return weir.reject("business rule") end`

func setProgram(n string) string {
	program := `return function(current, incoming) return {n = ` + n + `} end`
	return program
}

// foldCommands counts the commands a folded batch sends.
type foldCommands struct {
	finds, writes, commits, ops atomic.Int32
}

func (c *foldCommands) monitor() *event.CommandMonitor {
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		switch e.CommandName {
		case "find":
			c.finds.Add(1)
		case "bulkWrite":
			c.writes.Add(1)
			ops, _ := e.Command.Lookup("ops").Array().Values()
			c.ops.Add(int32(len(ops)))
		case "commitTransaction":
			c.commits.Add(1)
		}
	}}
	return monitor
}

// recordWrites prepares mutations of records of one collection.
type recordWrites struct {
	t       *testing.T
	adapter *Adapter
	prefix  string
}

func (r recordWrites) program(id, source string) *execution.Plan {
	opts := batchOperationOptions{resource: r.prefix + id, index: 1, program: source}
	return prepareBatchProgram(r.t, r.adapter, opts)
}

// write prepares a put or delete of the record id; a put stores n.
func (r recordWrites) write(id, action string, n int64) *execution.Plan {
	r.t.Helper()
	var document bson.D
	if action != "delete" {
		document = bson.D{{Key: "_id", Value: id}, {Key: "n", Value: n}}
	}
	opts := batchOperationOptions{resource: r.prefix + id, action: action, document: document}
	work, failure := prepareTestRecord(r.adapter, batchOperation(r.t, opts))
	if failure != nil {
		r.t.Fatal(failure)
	}
	work.Context = r.t.Context()
	return work
}

func checkOutcomes(t *testing.T, results []*pb.Event, want []pb.MutationOutcome) {
	t.Helper()
	for i, result := range results {
		if got := result.GetMutationResult().GetOutcome(); got != want[i] {
			t.Fatalf("result %d: %v, want %v", i, result.GetMutationResult(), want[i])
		}
	}
}

func storedN(t *testing.T, collection *mongo.Collection, id string) (int64, bool) {
	t.Helper()
	var document struct{ N int64 }
	filter := bson.D{{Key: "_id", Value: id}}
	err := collection.FindOne(t.Context(), filter).Decode(&document)
	if err == mongo.ErrNoDocuments {
		return 0, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return document.N, true
}

func TestMongoLuaProgramsOfOneRecordFoldIntoOneTransaction(t *testing.T) {
	fixture := testmongo.Open(t)
	collection := fixture.Admin.Database(fixture.DB).Collection("records")
	for _, id := range []string{"a", "gone"} {
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int64(1)}}
		if _, err := collection.InsertOne(t.Context(), document); err != nil {
			t.Fatal(err)
		}
	}
	commands := &foldCommands{}
	adapterOpts := adapterTestOptions{fixture: fixture, monitor: commands.monitor()}
	adapter := testAdapter(t, adapterOpts)
	records := recordWrites{t: t, adapter: adapter, prefix: fixture.DB + "/records/s:"}
	plans := []*execution.Plan{
		records.program("a", incrementProgram),
		records.program("new", incrementProgram),
		records.program("a", incrementProgram),
		records.program("gone", incrementProgram),
		records.program("a", rejectProgram),
		records.program("new", incrementProgram),
		records.program("a", incrementProgram),
		records.program("gone", `return function(current, incoming) return weir.delete() end`),
	}
	results := adapter.executePrograms(t.Context(), plans)
	applied, rejected := pb.MutationOutcome_APPLIED, pb.MutationOutcome_NOT_APPLIED
	want := []pb.MutationOutcome{applied, applied, applied, applied, rejected, applied, applied, applied}
	checkOutcomes(t, results, want)
	// One op for each program that changed its record, in one ordered write.
	if commands.finds.Load() != 1 || commands.writes.Load() != 1 || commands.ops.Load() != 7 || commands.commits.Load() != 1 {
		t.Fatal("finds", commands.finds.Load(), "writes", commands.writes.Load(), "ops", commands.ops.Load(), "commits", commands.commits.Load())
	}
	if n, found := storedN(t, collection, "a"); !found || n != 4 {
		t.Fatal("folded increments", n, found)
	}
	if n, found := storedN(t, collection, "new"); !found || n != 2 {
		t.Fatal("folded create", n, found)
	}
	if _, found := storedN(t, collection, "gone"); found {
		t.Fatal("folded delete left the record")
	}
}

func TestMongoRejectedFoldedProgramsRunOneAtATime(t *testing.T) {
	fixture := testmongo.Open(t)
	database := fixture.Admin.Database(fixture.DB)
	validator := bson.D{{Key: "n", Value: bson.D{{Key: "$lte", Value: 10}}}}
	create := options.CreateCollection().SetValidator(validator)
	if err := database.CreateCollection(t.Context(), "validated", create); err != nil {
		t.Fatal(err)
	}
	adapterOpts := adapterTestOptions{fixture: fixture}
	adapter := testAdapter(t, adapterOpts)
	records := recordWrites{t: t, adapter: adapter, prefix: fixture.DB + "/validated/s:"}
	plans := []*execution.Plan{
		records.program("v", setProgram("5")),
		records.program("v", setProgram("50")),
	}
	results := adapter.executePrograms(t.Context(), plans)
	want := []pb.MutationOutcome{pb.MutationOutcome_APPLIED, pb.MutationOutcome_NOT_APPLIED}
	checkOutcomes(t, results, want)
	if code := results[1].GetMutationResult().GetFailure().GetCode(); code != pb.FailureCode_PRECONDITION_FAILED {
		t.Fatal("rejected program failure", code)
	}
	if n, found := storedN(t, database.Collection("validated"), "v"); !found || n != 5 {
		t.Fatal("the program before the rejected one did not apply alone", n, found)
	}
}

func TestMongoPutsAndDeletesOfOneRecordFoldIntoTheLast(t *testing.T) {
	fixture := testmongo.Open(t)
	collection := fixture.Admin.Database(fixture.DB).Collection("records")
	document := bson.D{{Key: "_id", Value: "y"}, {Key: "n", Value: int64(1)}}
	if _, err := collection.InsertOne(t.Context(), document); err != nil {
		t.Fatal(err)
	}
	commands := &foldCommands{}
	adapterOpts := adapterTestOptions{fixture: fixture, monitor: commands.monitor()}
	adapter := testAdapter(t, adapterOpts)
	records := recordWrites{t: t, adapter: adapter, prefix: fixture.DB + "/records/s:"}
	plans := []*execution.Plan{
		records.write("x", "put", 1),
		records.write("y", "delete", 0),
		records.write("x", "put", 2),
		records.write("z", "delete", 0),
		records.write("y", "put", 3),
		records.write("z", "delete", 0),
	}
	results := adapter.ExecuteRecords(t.Context(), plans)
	applied := pb.MutationOutcome_APPLIED
	want := []pb.MutationOutcome{applied, applied, applied, applied, applied, applied}
	checkOutcomes(t, results, want)
	if commands.writes.Load() != 1 || commands.ops.Load() != 3 {
		t.Fatal("writes", commands.writes.Load(), "ops", commands.ops.Load())
	}
	if n, found := storedN(t, collection, "x"); !found || n != 2 {
		t.Fatal("folded puts", n, found)
	}
	if n, found := storedN(t, collection, "y"); !found || n != 3 {
		t.Fatal("folded delete and put", n, found)
	}
}

func TestMongoRejectedFoldedPutsRunOneAtATime(t *testing.T) {
	fixture := testmongo.Open(t)
	database := fixture.Admin.Database(fixture.DB)
	validator := bson.D{{Key: "n", Value: bson.D{{Key: "$lte", Value: 10}}}}
	create := options.CreateCollection().SetValidator(validator)
	if err := database.CreateCollection(t.Context(), "validated", create); err != nil {
		t.Fatal(err)
	}
	adapterOpts := adapterTestOptions{fixture: fixture}
	adapter := testAdapter(t, adapterOpts)
	records := recordWrites{t: t, adapter: adapter, prefix: fixture.DB + "/validated/s:"}
	plans := []*execution.Plan{
		records.write("v", "put", 1),
		records.write("v", "put", 50),
	}
	results := adapter.ExecuteRecords(t.Context(), plans)
	want := []pb.MutationOutcome{pb.MutationOutcome_APPLIED, pb.MutationOutcome_NOT_APPLIED}
	checkOutcomes(t, results, want)
	if n, found := storedN(t, database.Collection("validated"), "v"); !found || n != 1 {
		t.Fatal("the put before the rejected one did not apply alone", n, found)
	}
}

// validatedCollection creates a collection whose documents must keep n at
// most 10.
func validatedCollection(t *testing.T, fixture *testmongo.Fixture) *mongo.Collection {
	t.Helper()
	database := fixture.Admin.Database(fixture.DB)
	validator := bson.D{{Key: "n", Value: bson.D{{Key: "$lte", Value: 10}}}}
	create := options.CreateCollection().SetValidator(validator)
	if err := database.CreateCollection(t.Context(), "validated", create); err != nil {
		t.Fatal(err)
	}
	collection := database.Collection("validated")
	return collection
}

// cancelOnFirstWrite returns a caller context that ends once the first
// bulkWrite has been answered, and a monitor that ends it.
func cancelOnFirstWrite(t *testing.T) (context.Context, *event.CommandMonitor) {
	caller, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
		if e.CommandName == "bulkWrite" {
			cancel()
		}
	}}
	return caller, monitor
}

func TestMongoFoldedProgramsMeetEveryCheckAProgramAloneWould(t *testing.T) {
	fixture := testmongo.Open(t)
	collection := validatedCollection(t, fixture)
	document := bson.D{{Key: "_id", Value: "v"}, {Key: "n", Value: int64(1)}}
	if _, err := collection.InsertOne(t.Context(), document); err != nil {
		t.Fatal(err)
	}
	adapterOpts := adapterTestOptions{fixture: fixture}
	adapter := testAdapter(t, adapterOpts)
	records := recordWrites{t: t, adapter: adapter, prefix: fixture.DB + "/validated/s:"}
	plans := []*execution.Plan{
		records.program("v", setProgram("50")),
		records.program("v", `return function(current, incoming) if current.n > 10 then current.n = current.n - 45 end; return current end`),
	}
	results := adapter.executePrograms(t.Context(), plans)
	// Alone, the first program is rejected and the second finds n at 1.
	want := []pb.MutationOutcome{pb.MutationOutcome_NOT_APPLIED, pb.MutationOutcome_APPLIED}
	checkOutcomes(t, results, want)
	if n, found := storedN(t, collection, "v"); !found || n != 1 {
		t.Fatal("the second program ran on a document MongoDB rejected", n, found)
	}
}

func TestMongoAFoldedPutReplacedBeforeTheWriteIsNotChecked(t *testing.T) {
	fixture := testmongo.Open(t)
	collection := validatedCollection(t, fixture)
	adapterOpts := adapterTestOptions{fixture: fixture}
	adapter := testAdapter(t, adapterOpts)
	records := recordWrites{t: t, adapter: adapter, prefix: fixture.DB + "/validated/s:"}
	plans := []*execution.Plan{
		records.write("v", "put", 50),
		records.write("v", "put", 1),
	}
	// Only the last put reaches MongoDB, so both report APPLIED, as
	// docs/batching.md states, and the record holds the last.
	results := adapter.ExecuteRecords(t.Context(), plans)
	want := []pb.MutationOutcome{pb.MutationOutcome_APPLIED, pb.MutationOutcome_APPLIED}
	checkOutcomes(t, results, want)
	if n, found := storedN(t, collection, "v"); !found || n != 1 {
		t.Fatal("folded puts", n, found)
	}
}

func TestMongoAMutationWhoseCallerLeftBeforeItsChainRanAgainIsNotApplied(t *testing.T) {
	for _, kind := range []string{"put", "program"} {
		t.Run(kind, func(t *testing.T) {
			fixture := testmongo.Open(t)
			validatedCollection(t, fixture)
			caller, monitor := cancelOnFirstWrite(t)
			adapterOpts := adapterTestOptions{fixture: fixture, monitor: monitor}
			adapter := testAdapter(t, adapterOpts)
			records := recordWrites{t: t, adapter: adapter, prefix: fixture.DB + "/validated/s:"}
			plans := []*execution.Plan{records.write("v", "put", 1), records.write("v", "put", 50)}
			execute := adapter.ExecuteRecords
			if kind == "program" {
				plans = []*execution.Plan{records.program("v", setProgram("1")), records.program("v", setProgram("50"))}
				execute = adapter.executePrograms
			}
			// The chain is rejected and runs again one at a time, after the
			// first caller left.
			plans[0].Context = caller
			results := execute(t.Context(), plans)
			want := []pb.MutationOutcome{pb.MutationOutcome_NOT_APPLIED, pb.MutationOutcome_NOT_APPLIED}
			checkOutcomes(t, results, want)
			if code := results[0].GetMutationResult().GetFailure().GetCode(); code != pb.FailureCode_CANCELLED {
				t.Fatal("the departed caller's cause", code)
			}
		})
	}
}

func TestMongoAChainTooLargeForOneCommandRunsOneAtATime(t *testing.T) {
	fixture := testmongo.Open(t)
	collection := fixture.Admin.Database(fixture.DB).Collection("records")
	commands := &foldCommands{}
	adapterOpts := adapterTestOptions{fixture: fixture, monitor: commands.monitor()}
	adapter := testAdapter(t, adapterOpts)
	settings := adapter.options()
	settings.ExchangeBytes = 4096
	adapter.config.Options = &settings
	padding := strings.Repeat("x", 3000)
	program := func(n string) string {
		source := `return function(current, incoming) return {n = ` + n + `, pad = "` + padding + `"} end`
		return source
	}
	records := recordWrites{t: t, adapter: adapter, prefix: fixture.DB + "/records/s:"}
	plans := []*execution.Plan{
		records.program("big", program("1")),
		records.program("big", program("2")),
	}
	results := adapter.executePrograms(t.Context(), plans)
	want := []pb.MutationOutcome{pb.MutationOutcome_APPLIED, pb.MutationOutcome_APPLIED}
	checkOutcomes(t, results, want)
	if n, found := storedN(t, collection, "big"); !found || n != 2 || commands.commits.Load() != 2 {
		t.Fatal("stored", n, found, "commits", commands.commits.Load())
	}
}
