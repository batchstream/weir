//go:build integration

package mongodb

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func mongoExpression(t *testing.T, a *Adapter, database string, doc bson.D) *execution.Plan {
	t.Helper()
	raw := expressionBSON(t, doc)
	op := expressionOperation("weir://mongo/"+database+"/records/s:counter", raw)
	p, f := a.prepareRecord(op)
	if f != nil {
		t.Fatal(f)
	}
	return p
}
func executeMongoExpression(t *testing.T, a *Adapter, p *execution.Plan) *pb.MutationResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	results, _ := a.executeRecords(ctx, []*execution.Plan{p})
	return results[0].GetMutation()
}

func TestMongoExpressionAtomicAndNumeric(t *testing.T) {
	backend := testmongo.Open(t)
	client, db := backend.Admin, backend.DB
	cfg := Config{Store: "mongo", URI: backend.URI, Pool: 4}
	cfg = mongoFixtureConfig(t, cfg)
	a, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	c := client.Database(db).Collection("records")
	inc := bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int64(1)}}}}
	p := mongoExpression(t, a, db, inc)
	r := executeMongoExpression(t, a, p)
	if r.Outcome != pb.MutationOutcome_NOT_APPLIED || r.GetFailure().GetCode() != pb.FailureCode_PRECONDITION_FAILED {
		t.Fatal(r)
	}
	decimal, _ := bson.ParseDecimal128("1.25")
	doc := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int64(9007199254740993)}, {Key: "small", Value: int32(math.MaxInt32)}, {Key: "wide", Value: int64(math.MaxInt64)}, {Key: "decimal", Value: decimal}, {Key: "null", Value: nil}, {Key: "keep", Value: bson.NewObjectID()}, {Key: "remove", Value: 1}}
	if _, err := c.InsertOne(context.Background(), doc); err != nil {
		t.Fatal(err)
	}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "data", Value: bson.D{{Key: "$literal", Value: "ordinary data"}}}}}, {Key: "$unset", Value: bson.D{{Key: "remove", Value: ""}}}, {Key: "$inc", Value: bson.D{{Key: "n", Value: int64(1)}, {Key: "small", Value: int32(1)}, {Key: "decimal", Value: decimal}, {Key: "missing", Value: int32(2)}}}}
	r = executeMongoExpression(t, a, mongoExpression(t, a, db, update))
	if r.Outcome != pb.MutationOutcome_APPLIED {
		t.Fatal(r)
	}
	filter := bson.D{{Key: "_id", Value: "counter"}}
	raw, err := c.FindOne(context.Background(), filter).Raw()
	if err != nil {
		t.Fatal(err)
	}
	if raw.Lookup("n").Int64() != 9007199254740994 || raw.Lookup("small").Type != bson.TypeInt64 || raw.Lookup("small").Int64() != 2147483648 || raw.Lookup("wide").Type != bson.TypeInt64 || raw.Lookup("wide").Int64() != math.MaxInt64 || raw.Lookup("decimal").Decimal128().String() != "2.50" || raw.Lookup("missing").Int32() != 2 || raw.Lookup("remove").Type != 0 || raw.Lookup("keep").ObjectID() != doc[6].Value {
		t.Fatal(raw)
	}
	overflow := bson.D{{Key: "$inc", Value: bson.D{{Key: "wide", Value: int64(1)}}}}
	r = executeMongoExpression(t, a, mongoExpression(t, a, db, overflow))
	if r.Outcome != pb.MutationOutcome_NOT_APPLIED {
		t.Fatal("int64 overflow", r)
	}
	t.Logf("native promotion: int32 overflow -> %v; int64 overflow rejected; decimal=%s", raw.Lookup("small").Type, raw.Lookup("decimal").Decimal128())
	null := bson.D{{Key: "$inc", Value: bson.D{{Key: "null", Value: 1}}}}
	r = executeMongoExpression(t, a, mongoExpression(t, a, db, null))
	if r.Outcome != pb.MutationOutcome_NOT_APPLIED {
		t.Fatal("null inc", r)
	}
	noop := bson.D{{Key: "$set", Value: bson.D{}}}
	r = executeMongoExpression(t, a, mongoExpression(t, a, db, noop))
	if r.Outcome != pb.MutationOutcome_APPLIED {
		t.Fatal("noop", r)
	}
	var group sync.WaitGroup
	for range 4 {
		group.Go(func() {
			for range 10 {
				r := executeMongoExpression(t, a, p)
				if r.Outcome != pb.MutationOutcome_APPLIED {
					t.Error(r)
				}
			}
		})
	}
	group.Wait()
	raw, err = c.FindOne(context.Background(), filter).Raw()
	if err != nil || raw.Lookup("n").Int64() != 9007199254741034 {
		t.Fatal("lost update", raw, err)
	}
}

func TestMongoExpressionNativeCompetition(t *testing.T) {
	for _, kind := range []string{"update", "replace", "delete", "recreate"} {
		t.Run(kind, func(t *testing.T) {
			backend := testmongo.Open(t)
			client, db := backend.Admin, backend.DB
			c := client.Database(db).Collection("records")
			doc := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int64(1)}}
			if _, err := c.InsertOne(context.Background(), doc); err != nil {
				t.Fatal(err)
			}
			filter := bson.D{{Key: "_id", Value: "counter"}}
			var updates, reads atomic.Int32
			monitor := &event.CommandMonitor{Started: func(ctx context.Context, e *event.CommandStartedEvent) {
				if e.CommandName == "find" {
					reads.Add(1)
				}
				if e.CommandName != "bulkWrite" || updates.Add(1) != 1 {
					return
				}
				replacement := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int64(20)}, {Key: "native", Value: true}}
				var err error
				switch kind {
				case "update":
					up := bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: 10}}}}
					_, err = c.UpdateOne(ctx, filter, up)
				case "replace":
					_, err = c.ReplaceOne(ctx, filter, replacement)
				case "delete", "recreate":
					_, err = c.DeleteOne(ctx, filter)
					if err == nil && kind == "recreate" {
						_, err = c.InsertOne(ctx, replacement)
					}
				}
				if err != nil {
					t.Error(err)
				}
			}}
			monitor.Succeeded = func(_ context.Context, e *event.CommandSucceededEvent) {
				if e.CommandName == "bulkWrite" {
					t.Logf("native update reply: %s", e.Reply)
				}
			}
			opts := adapterTestOptions{fixture: backend, monitor: monitor}
			a := testAdapter(t, opts)
			inc := bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: 1}}}}
			r := executeMongoExpression(t, a, mongoExpression(t, a, db, inc))
			if reads.Load() != 0 || updates.Load() != 1 {
				t.Fatal("pre-read or replay", reads.Load(), updates.Load())
			}
			if kind == "delete" {
				if r.Outcome != pb.MutationOutcome_NOT_APPLIED {
					t.Fatal(r)
				}
				return
			}
			raw, err := c.FindOne(context.Background(), filter).Raw()
			want := int64(21)
			if kind == "update" {
				want = 12
			}
			if err != nil || r.Outcome != pb.MutationOutcome_APPLIED || raw.Lookup("n").AsInt64() != want {
				t.Fatal(raw, r, err)
			}
		})
	}
}

func TestMongoExpressionReplyLossAndConcern(t *testing.T) {
	for _, mode := range []string{"drop", "truncate", "missing_n", "concern", "conflict"} {
		t.Run(mode, func(t *testing.T) {
			backend := testmongo.Open(t)
			client, db := backend.Admin, backend.DB
			c := client.Database(db).Collection("records")
			doc := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int64(0)}}
			if _, err := c.InsertOne(context.Background(), doc); err != nil {
				t.Fatal(err)
			}
			proxy := testmongo.StartProxy(t, backend)
			if mode == "drop" {
				proxy.DropCommand = "bulkWrite"
				proxy.DropRemaining.Store(1)
			}
			if mode == "truncate" || mode == "missing_n" {
				proxy.AlterCommand = "bulkWrite"
				proxy.AlterMode = mode
				proxy.AlterRemaining.Store(1)
			}
			if mode == "concern" || mode == "conflict" {
				data := bson.D{{Key: "failCommands", Value: bson.A{"bulkWrite"}}, {Key: "appName", Value: "weir:mongo"}}
				if mode == "concern" {
					concern := bson.E{Key: "writeConcernError", Value: bson.D{{Key: "code", Value: 64}, {Key: "errmsg", Value: "fixture concern"}}}
					data = append(data, concern)
				} else {
					conflict := bson.E{Key: "errorCode", Value: 112}
					data = append(data, conflict)
				}
				testmongo.FailCommand(t, client, data, 1)
			}
			opts := adapterTestOptions{fixture: backend, uri: proxy.URI()}
			a := testAdapter(t, opts)
			inc := bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int64(1)}}}}
			r := executeMongoExpression(t, a, mongoExpression(t, a, db, inc))
			want := pb.MutationOutcome_UNKNOWN
			if mode == "conflict" {
				want = pb.MutationOutcome_NOT_APPLIED
			}
			if r.Outcome != want {
				t.Fatal(mode, r)
			}
			if mode == "drop" {
				verifyReconnectRead(t, a, proxy, "weir://mongo/"+db+"/records/s:"+"counter")
			}
			writes := 0
			for _, e := range proxy.Events() {
				if e.Command == "bulkWrite" {
					writes++
				}
			}
			if writes != 1 {
				t.Fatal("replay", writes)
			}
			filter := bson.D{{Key: "_id", Value: "counter"}}
			raw, err := c.FindOne(context.Background(), filter).Raw()
			n := int64(1)
			if mode == "conflict" {
				n = 0
			}
			if err != nil || raw.Lookup("n").Int64() != n {
				t.Fatal(raw, err)
			}
		})
	}
}

func TestMongoExpressionCancellationLedgerAndDrain(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline", "drain"} {
		t.Run(mode, func(t *testing.T) {
			backend := testmongo.Open(t)
			client, db := backend.Admin, backend.DB
			seed := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int64(0)}}
			c := client.Database(db).Collection("records")
			if _, err := c.InsertOne(context.Background(), seed); err != nil {
				t.Fatal(err)
			}
			proxy := testmongo.StartProxy(t, backend)
			proxy.DropCommand = "bulkWrite"
			proxy.DropRemaining.Store(1)
			gate := make(chan struct{})
			proxy.DropGate = gate
			defer close(gate)
			opts := adapterTestOptions{fixture: backend, uri: proxy.URI()}
			a := testAdapter(t, opts)
			limits := store.DefaultLimits()
			limits.Concurrency = 1
			runtime, err := store.New(a, limits)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := runtime.Close(ctx); err != nil {
					t.Error(err)
				}
			}()
			inc := bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int64(1)}}}}
			p := mongoExpression(t, a, db, inc)
			runtime.SetOverloaded(true)
			if _, failure, _ := runtime.Submit(context.Background(), p, nil); failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
				t.Fatal(failure)
			}
			runtime.SetOverloaded(false)
			duration := 2 * time.Second
			if mode == "deadline" {
				duration = 150 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()
			first, failure, _ := runtime.Submit(ctx, p, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			until := time.Now().Add(time.Second)
			for {
				ack := false
				for _, e := range proxy.Events() {
					ack = ack || e.Command == "bulkWrite" && e.Dropped && e.Acknowledged
				}
				if ack {
					break
				}
				if time.Now().After(until) {
					t.Fatal("no real acknowledgement")
				}
				time.Sleep(time.Millisecond)
			}
			queuedCtx, stop := context.WithCancel(context.Background())
			queued, failure, _ := runtime.Submit(queuedCtx, p, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			stop()
			wait, done := context.WithTimeout(context.Background(), time.Second)
			defer done()
			result, err := queued.Wait(wait)
			if err != nil || result.GetMutation().GetOutcome() != pb.MutationOutcome_NOT_STARTED {
				t.Fatal(result, err)
			}
			queued.Ack()
			if mode == "cancel" {
				cancel()
			}
			if mode == "drain" {
				drain, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
				err := runtime.Close(drain)
				stop()
				if err != nil {
					t.Fatal(err)
				}
			}
			result, err = first.Wait(wait)
			if err != nil || result.GetMutation().GetOutcome() != pb.MutationOutcome_UNKNOWN {
				t.Fatal(result, err)
			}
			first.Ack()
			if mode == "drop" {
				verifyReconnectRead(t, a, proxy, "weir://mongo/"+db+"/records/s:"+"counter")
			}
			writes := 0
			for _, e := range proxy.Events() {
				if e.Command == "bulkWrite" {
					writes++
				}
			}
			if writes != 1 {
				t.Fatal("queued execution/replay", writes)
			}
			s := runtime.Snapshot()
			if s.Pending != 0 || s.Active != 0 || s.Retained != 0 || s.ResultBytes != 0 {
				t.Fatal("ledger leak", s)
			}
		})
	}
}
