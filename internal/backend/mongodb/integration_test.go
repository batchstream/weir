//go:build integration

package mongodb

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestNativeRMWConflicts(t *testing.T) {
	for _, kind := range []string{"update", "replace", "delete", "recreate", "missing_insert"} {
		t.Run(kind, func(t *testing.T) {
			backend := testmongo.Open(t)
			native, db := backend.Admin, backend.DB
			c := native.Database(db).Collection("records")
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			filter := bson.D{{Key: "_id", Value: "counter"}}
			original := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int32(1)}}
			if kind != "missing_insert" {
				if _, err := c.InsertOne(ctx, original); err != nil {
					t.Fatal(err)
				}
			}
			var reads atomic.Int32
			monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
				if e.CommandName != "find" || reads.Add(1) != 1 {
					return
				}
				var err error
				replacement := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int64(20)}, {Key: "native", Value: true}}
				switch kind {
				case "update":
					set := bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(10)}}}}
					_, err = c.UpdateOne(ctx, filter, set)
				case "replace":
					_, err = c.ReplaceOne(ctx, filter, replacement)
				case "delete":
					_, err = c.DeleteOne(ctx, filter)
				case "recreate":
					_, err = c.DeleteOne(ctx, filter)
					if err == nil {
						_, err = c.InsertOne(ctx, replacement)
					}
				case "missing_insert":
					_, err = c.InsertOne(ctx, replacement)
				}
				if err != nil {
					t.Error("native writer", err)
				}
			}}
			o := adapterTestOptions{fixture: backend, monitor: monitor}
			a := testAdapter(t, o)
			p := prepareCounter(t, a, "weir://mongo/"+db+"/records/s:counter")
			report := a.IncrementConformance(ctx, p)
			if report.Result.Outcome != pb.MutationOutcome_APPLIED || report.Attempts < 2 || report.Evaluations < 2 {
				t.Fatalf("fresh transaction/recompute required: %+v %+v", report, report.Result)
			}
			raw, err := c.FindOne(ctx, filter).Raw()
			if err != nil {
				t.Fatal(err)
			}
			want := int64(21)
			if kind == "update" {
				want = 12
			} else if kind == "delete" {
				want = 1
			}
			if raw.Lookup("n").AsInt64() != want {
				t.Fatalf("lost native write: %v", raw)
			}
			fields, _ := raw.Elements()
			for _, f := range fields {
				if f.Key() != "_id" && f.Key() != "n" && f.Key() != "native" {
					t.Fatal("injected field", f.Key())
				}
			}
			t.Logf("%s: attempts=%d evaluations=%d commits=%d n=%d fields=%d", kind, report.Attempts, report.Evaluations, report.Commits, want, len(fields))
		})
	}
}
func TestCommitReplyLostAfterRealCommit(t *testing.T) {
	for _, mode := range []string{"once", "all"} {
		t.Run(mode, func(t *testing.T) {
			backend := testmongo.Open(t)
			native, db := backend.Admin, backend.DB
			proxy := testmongo.StartProxy(t, backend)
			proxy.DropCommand = "commitTransaction"
			proxy.DropRemaining.Store(1)
			if mode == "all" {
				proxy.DropRemaining.Store(1 << 60)
			}
			doc := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int64(0)}}
			if _, err := native.Database(db).Collection("records").InsertOne(context.Background(), doc); err != nil {
				t.Fatal(err)
			}
			o := adapterTestOptions{fixture: backend, uri: proxy.URI()}
			a := testAdapter(t, o)
			p := prepareCounter(t, a, "weir://mongo/"+db+"/records/s:counter")
			ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
			defer cancel()
			start := time.Now()
			report := a.IncrementConformance(ctx, p)
			want := pb.MutationOutcome_APPLIED
			if mode == "all" {
				want = pb.MutationOutcome_UNKNOWN
			}
			if report.Result.Outcome != want || report.Attempts != 1 || report.Evaluations != 1 || time.Since(start) > 1200*time.Millisecond {
				t.Fatalf("%+v result=%v elapsed=%s", report, report.Result, time.Since(start))
			}
			var transaction int64
			session := ""
			commits := 0
			dropped := 0
			for _, e := range proxy.Events() {
				if e.Command == "commitTransaction" {
					if !e.Acknowledged {
						t.Fatal("proxy must drop actual successful commit", e)
					}
					commits++
					if e.Dropped {
						dropped++
					}
					if session == "" {
						session = e.Session
						transaction = e.Transaction
					} else if session != e.Session || transaction != e.Transaction {
						t.Fatal("new transaction after ambiguous commit")
					}
				}
			}
			if commits < 2 || commits > 2*MaxCommitAttempts || dropped == 0 {
				t.Fatal("fault not exercised", proxy.Events())
			}
			filter := bson.D{{Key: "_id", Value: "counter"}}
			raw, err := native.Database(db).Collection("records").FindOne(context.Background(), filter).Raw()
			if err != nil || raw.Lookup("n").AsInt64() != 1 {
				t.Fatal("transform replayed", err, raw)
			}
			t.Logf("%s: outcome=%s evaluations=%d attempts=%d wire_commits=%d dropped_successful_replies=%d same_session_txn=true elapsed=%s", mode, want, report.Evaluations, report.Attempts, commits, dropped, time.Since(start))
		})
	}
}
func TestRMWAttemptAndDeadlineBounds(t *testing.T) {
	backend := testmongo.Open(t)
	native, db := backend.Admin, backend.DB
	c := native.Database(db).Collection("records")
	doc := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int32(0)}}
	_, err := c.InsertOne(context.Background(), doc)
	if err != nil {
		t.Fatal(err)
	}
	filter := bson.D{{Key: "_id", Value: "counter"}}
	update := bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int32(1)}}}}
	monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
		if e.CommandName == "find" {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := c.UpdateOne(ctx, filter, update); err != nil {
				t.Error(err)
			}
		}
	}}
	o := adapterTestOptions{fixture: backend, monitor: monitor}
	a := testAdapter(t, o)
	p := prepareCounter(t, a, "weir://mongo/"+db+"/records/s:counter")
	report := a.IncrementConformance(context.Background(), p)
	if report.Result.Outcome != pb.MutationOutcome_NOT_APPLIED || report.Attempts != 5 || report.Commits != 0 {
		t.Fatal(report, report.Result)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report = a.IncrementConformance(ctx, p)
	if report.Result.Outcome != pb.MutationOutcome_NOT_STARTED || report.Attempts != 0 {
		t.Fatal(report)
	}
}
func TestPartialInitializationAndClose(t *testing.T) {
	backend := testmongo.Open(t)
	native := backend.Admin
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	before := connectionCount(t, native)
	cfg := Config{URI: backend.URI, Store: "mongo", Pool: 1}
	cfg = mongoFixtureConfig(t, cfg)
	for i := 0; i < 5; i++ {
		attempt, err := Open(ctx, cfg)
		if err != nil {
			t.Fatal("server qualification must not require a target collection", err)
		}
		if err = attempt.Close(); err != nil {
			t.Fatal(err)
		}
	}
	a, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	if err = a.client.Ping(ctx, nil); err != mongo.ErrClientDisconnected {
		t.Fatal("client not closed", err)
	}
	remaining := connectionCount(t, native)
	for i := 0; i < 100 && remaining > before+1; i++ {
		time.Sleep(5 * time.Millisecond)
		remaining = connectionCount(t, native)
	}
	if remaining > before+1 {
		t.Fatalf("initialization/close leaked sockets: before=%d after=%d", before, remaining)
	}
}

func connectionCount(t *testing.T, client *mongo.Client) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	command := bson.D{{Key: "serverStatus", Value: 1}}
	var result struct {
		Connections struct {
			Current int `bson:"current"`
		} `bson:"connections"`
	}
	if err := client.Database("admin").RunCommand(ctx, command).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result.Connections.Current
}

func TestCommitCancellationRetainsOriginalDeadline(t *testing.T) {
	backend := testmongo.Open(t)
	native, db := backend.Admin, backend.DB
	o := adapterTestOptions{fixture: backend}
	a := testAdapter(t, o)
	p := prepareCounter(t, a, "weir://mongo/"+db+"/records/s:deadline")
	data := bson.D{{Key: "failCommands", Value: bson.A{"commitTransaction"}}, {Key: "appName", Value: "weir:mongo"}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 250}}
	testmongo.FailCommand(t, native, data, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	start := time.Now()
	report := a.IncrementConformance(ctx, p)
	if report.Result.Outcome != pb.MutationOutcome_UNKNOWN || report.Evaluations != 1 || report.Attempts != 1 || time.Since(start) > 350*time.Millisecond {
		t.Fatal("original deadline lost during commit", report, report.Result, time.Since(start))
	}
	t.Log("commit cancellation elapsed", time.Since(start), "outcome", report.Result.Outcome)
}
func TestUnrelatedUniqueConflictDoesNotRetry(t *testing.T) {
	backend := testmongo.Open(t)
	native, db := backend.Admin, backend.DB
	collection := native.Database(db).Collection("records")
	keys := bson.D{{Key: "n", Value: 1}}
	model := mongo.IndexModel{Keys: keys, Options: options.Index().SetUnique(true)}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := collection.Indexes().CreateOne(ctx, model); err != nil {
		t.Fatal(err)
	}
	doc := bson.D{{Key: "_id", Value: "existing"}, {Key: "n", Value: int64(1)}}
	if _, err := collection.InsertOne(ctx, doc); err != nil {
		t.Fatal(err)
	}
	o := adapterTestOptions{fixture: backend}
	a := testAdapter(t, o)
	p := prepareCounter(t, a, "weir://mongo/"+db+"/records/s:new")
	report := a.IncrementConformance(ctx, p)
	if report.Attempts != 1 || report.Commits != 0 || report.Result.Outcome != pb.MutationOutcome_NOT_APPLIED {
		t.Fatal(report, report.Result)
	}
}

func TestAcknowledgedOrdinaryBatchReplyLostIsNotReplayed(t *testing.T) {
	backend := testmongo.Open(t)
	native, db := backend.Admin, backend.DB
	proxy := testmongo.StartProxy(t, backend)
	proxy.DropCommand = "bulkWrite"
	proxy.DropRemaining.Store(1)
	o := adapterTestOptions{fixture: backend, uri: proxy.URI()}
	a := testAdapter(t, o)
	var plans []*execution.Plan
	for _, id := range []string{"one", "two"} {
		doc := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int64(1)}}
		raw, _ := bson.Marshal(doc)
		d := &pb.Document{MediaType: "application/bson", Data: raw}
		action := &pb.MutateRequest_Create{Create: d}
		m := &pb.MutateRequest{Resource: "weir://mongo/" + db + "/records/s:" + id, Action: action}
		v := &pb.Operation_Mutate{Mutate: m}
		op := &pb.Operation{Operation: v}
		p, f := a.prepareRecord(op)
		if f != nil {
			t.Fatal(f)
		}
		plans = append(plans, p)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results, _ := a.executeRecords(ctx, plans)
	for _, r := range results {
		if r.GetMutation().Outcome != pb.MutationOutcome_UNKNOWN {
			t.Fatal(r)
		}
	}
	verifyReconnectRead(t, a, proxy, "weir://mongo/"+db+"/records/s:one")
	inserts := 0
	for _, e := range proxy.Events() {
		if e.Command == "bulkWrite" {
			inserts++
			if !e.Acknowledged || !e.Dropped {
				t.Fatal("not a dropped successful reply", e)
			}
		}
	}
	if inserts != 1 {
		t.Fatal("ordinary mutation replayed", inserts)
	}
	filter := bson.D{}
	n, err := native.Database(db).Collection("records").CountDocuments(ctx, filter)
	if err != nil || n != 2 {
		t.Fatal("real writes must persist", n, err)
	}
}
func TestAmbiguitySurvivesLaterDefiniteError(t *testing.T) {
	backend := testmongo.Open(t)
	native, db := backend.Admin, backend.DB
	proxy := testmongo.StartProxy(t, backend)
	proxy.DropCommand = "commitTransaction"
	proxy.DropRemaining.Store(1)
	gate := make(chan struct{})
	proxy.DropGate = gate
	o := adapterTestOptions{fixture: backend, uri: proxy.URI()}
	a := testAdapter(t, o)
	p := prepareCounter(t, a, "weir://mongo/"+db+"/records/s:sticky")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan RMWReport, 1)
	go func() { done <- a.IncrementConformance(ctx, p) }()
	found := false
	for i := 0; i < 300; i++ {
		for _, e := range proxy.Events() {
			if e.Command == "commitTransaction" && e.Dropped && e.Acknowledged {
				found = true
			}
		}
		if found {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !found {
		close(gate)
		t.Fatal("successful commit never reached proxy")
	}
	data := bson.D{{Key: "failCommands", Value: bson.A{"commitTransaction"}}, {Key: "appName", Value: "weir:mongo"}, {Key: "errorCode", Value: 2}}
	testmongo.FailCommand(t, native, data, 100)
	close(gate)
	report := <-done
	if report.Result.Outcome != pb.MutationOutcome_UNKNOWN || report.Attempts != 1 || report.Evaluations != 1 {
		t.Fatal("later rejection cleared ambiguity", report, report.Result)
	}
	filter := bson.D{{Key: "_id", Value: "sticky"}}
	raw, err := native.Database(db).Collection("records").FindOne(ctx, filter).Raw()
	if err != nil || raw.Lookup("n").AsInt64() != 1 {
		t.Fatal(err, raw)
	}
}
func TestMissingDeleteBatchAcknowledgedWithoutRead(t *testing.T) {
	backend := testmongo.Open(t)
	db := backend.DB
	var reads, deletes atomic.Int32
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "find" {
			reads.Add(1)
		}
		if e.CommandName == "bulkWrite" {
			deletes.Add(1)
		}
	}}
	o := adapterTestOptions{fixture: backend, monitor: monitor}
	a := testAdapter(t, o)
	var plans []*execution.Plan
	for _, id := range []string{"a", "b"} {
		empty := &pb.Empty{}
		action := &pb.MutateRequest_Delete{Delete: empty}
		m := &pb.MutateRequest{Resource: "weir://mongo/" + db + "/records/s:" + id, Action: action}
		v := &pb.Operation_Mutate{Mutate: m}
		op := &pb.Operation{Operation: v}
		p, f := a.prepareRecord(op)
		if f != nil {
			t.Fatal(f)
		}
		plans = append(plans, p)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results, _ := a.executeRecords(ctx, plans)
	for _, r := range results {
		if r.GetMutation().Outcome != pb.MutationOutcome_APPLIED {
			t.Fatal(r)
		}
	}
	if reads.Load() != 0 || deletes.Load() != 1 {
		t.Fatal("unexpected pre-read or no physical batching", reads.Load(), deletes.Load())
	}
}

func TestCloseDuringCommit(t *testing.T) {
	backend := testmongo.Open(t)
	native, db := backend.Admin, backend.DB
	started := make(chan struct{}, 1)
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "commitTransaction" {
			select {
			case started <- struct{}{}:
			default:
			}
		}
	}}
	o := adapterTestOptions{fixture: backend, monitor: monitor}
	a := testAdapter(t, o)
	p := prepareCounter(t, a, "weir://mongo/"+db+"/records/s:closing")
	data := bson.D{{Key: "failCommands", Value: bson.A{"commitTransaction"}}, {Key: "appName", Value: "weir:mongo"}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 250}}
	testmongo.FailCommand(t, native, data, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan RMWReport, 1)
	go func() { done <- a.IncrementConformance(ctx, p) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("commit did not start")
	}
	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	cancel()
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case report := <-done:
		if report.Result.Outcome != pb.MutationOutcome_UNKNOWN || report.Attempts != 1 || report.Evaluations != 1 {
			t.Fatal("close inferred rollback or replayed", report, report.Result)
		}
	case <-time.After(time.Second):
		t.Fatal("transaction worker survived close")
	}
	if time.Since(start) > 300*time.Millisecond {
		t.Fatal("close unbounded", time.Since(start))
	}
	if err := a.client.Ping(context.Background(), nil); err != mongo.ErrClientDisconnected {
		t.Fatal("client not reclaimed", err)
	}
}
