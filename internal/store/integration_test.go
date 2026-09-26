//go:build integration

package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/mongostore"
	"github.com/batchstream/weir/internal/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type fixture struct {
	runtime *Runtime
	native  *mongo.Client
	db      string
}

func setup(t *testing.T) fixture {
	native, db := testmongo.Open(t)
	cfg := mongostore.Config{URI: testmongo.URI, Store: "mongo", Database: db, Collection: "records"}
	l := DefaultLimits()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cfg.Pool = uint64(l.Concurrency)
	a, err := mongostore.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	r, err := New(a, l)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	f := fixture{runtime: r, native: native, db: db}
	return f
}
func readPlan(t *testing.T, f fixture, key string) *execution.Plan {
	t.Helper()
	req := &pb.ReadRequest{Resource: "weir://mongo/" + f.db + "/records/s:" + key}
	v := &pb.BulkOperation_Read{Read: req}
	op := &pb.BulkOperation{Operation: v}
	p, err := f.runtime.Prepare(op)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func createPlan(t *testing.T, f fixture, key string) *execution.Plan {
	t.Helper()
	doc := bson.D{{Key: "_id", Value: key}, {Key: "n", Value: int32(1)}}
	raw, _ := bson.Marshal(doc)
	d := &pb.Document{MediaType: "application/bson", Data: raw}
	v := &pb.MutateRequest_Create{Create: d}
	m := &pb.MutateRequest{Resource: "weir://mongo/" + f.db + "/records/s:" + key, Action: v}
	mv := &pb.BulkOperation_Mutate{Mutate: m}
	op := &pb.BulkOperation{Operation: mv}
	p, err := f.runtime.Prepare(op)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func warm(t *testing.T, f fixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var tickets []*Ticket
	for i := 0; i < 24; i++ {
		p := readPlan(t, f, fmt.Sprint(i))
		ticket, e, _ := f.runtime.Submit(ctx, p, nil)
		if e != nil {
			t.Fatal(e)
		}
		tickets = append(tickets, ticket)
	}
	for _, ticket := range tickets {
		if _, err := ticket.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		ticket.Ack()
	}
	if f.runtime.Snapshot().Window < 2 {
		t.Fatal("AIMD did not grow on saturated demand")
	}
}
func TestNativeIndependentReadsOverlap(t *testing.T) {
	f := setup(t)
	warm(t, f)
	data := bson.D{{Key: "failCommands", Value: bson.A{"find"}}, {Key: "appName", Value: "weir:" + f.db}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 200}}
	testmongo.FailCommand(t, f.native, data, 2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p := readPlan(t, f, "same")
	start := time.Now()
	a, _, _ := f.runtime.Submit(ctx, p, nil)
	b, _, _ := f.runtime.Submit(ctx, p, nil)
	if _, err := a.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	a.Ack()
	b.Ack()
	if time.Since(start) > 360*time.Millisecond {
		t.Fatal("independent reads serialized", time.Since(start))
	}
	t.Log("same-key independent reads elapsed", time.Since(start))
}
func TestNativeBatchMixedDeadlines(t *testing.T) {
	f := setup(t)
	data := bson.D{{Key: "failCommands", Value: bson.A{"insert"}}, {Key: "appName", Value: "weir:" + f.db}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 150}}
	testmongo.FailCommand(t, f.native, data, 1)
	short, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	long, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p := createPlan(t, f, "short")
	a, e, _ := f.runtime.Submit(short, p, nil)
	if e != nil {
		t.Fatal(e)
	}
	p = createPlan(t, f, "long")
	b, e, _ := f.runtime.Submit(long, p, nil)
	if e != nil {
		t.Fatal(e)
	}
	if _, err := a.Wait(short); err == nil {
		t.Fatal("short caller should time out")
	}
	result, err := b.Wait(long)
	if err != nil || result.GetMutation().Outcome != pb.MutationOutcome_APPLIED {
		t.Fatal("unrelated caller cancelled", err, result)
	}
	b.Ack()
	filter := bson.D{}
	count, err := f.native.Database(f.db).Collection("records").CountDocuments(long, filter)
	if err != nil || count != 2 {
		t.Fatal("expired dispatched mutation may still apply", count, err)
	}
	t.Log("shared insert: short caller timed out; both persisted; surviving caller APPLIED")
}
func TestNativeBatchItemAndUncertainErrors(t *testing.T) {
	f := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	existing := bson.D{{Key: "_id", Value: "exists"}, {Key: "n", Value: 1}}
	if _, err := f.native.Database(f.db).Collection("records").InsertOne(ctx, existing); err != nil {
		t.Fatal(err)
	}
	var tickets []*Ticket
	for _, key := range []string{"exists", "new"} {
		p := createPlan(t, f, key)
		ticket, e, _ := f.runtime.Submit(ctx, p, nil)
		if e != nil {
			t.Fatal(e)
		}
		tickets = append(tickets, ticket)
	}
	for i, ticket := range tickets {
		r, err := ticket.Wait(ctx)
		if err != nil {
			t.Fatal(err)
		}
		want := pb.MutationOutcome_APPLIED
		if i == 0 {
			want = pb.MutationOutcome_NOT_APPLIED
		}
		if r.GetMutation().Outcome != want {
			t.Fatal(r)
		}
		ticket.Ack()
	}
	data := bson.D{{Key: "failCommands", Value: bson.A{"insert"}}, {Key: "appName", Value: "weir:" + f.db}, {Key: "closeConnection", Value: true}}
	testmongo.FailCommand(t, f.native, data, 1)
	tickets = nil
	for _, key := range []string{"unknown_a", "unknown_b"} {
		p := createPlan(t, f, key)
		ticket, e, _ := f.runtime.Submit(ctx, p, nil)
		if e != nil {
			t.Fatal(e)
		}
		tickets = append(tickets, ticket)
	}
	for _, ticket := range tickets {
		r, err := ticket.Wait(ctx)
		if err != nil || r.GetMutation().Outcome != pb.MutationOutcome_UNKNOWN {
			t.Fatal(err, r)
		}
		ticket.Ack()
	}
}
func TestNativeShutdownQueueAndExecution(t *testing.T) {
	f := setup(t)
	data := bson.D{{Key: "failCommands", Value: bson.A{"insert"}}, {Key: "appName", Value: "weir:" + f.db}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 500}}
	testmongo.FailCommand(t, f.native, data, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	p := createPlan(t, f, "executing")
	a, _, _ := f.runtime.Submit(ctx, p, nil)
	for f.runtime.Snapshot().Active == 0 {
		time.Sleep(time.Millisecond)
	}
	q := createPlan(t, f, "queued")
	b, _, _ := f.runtime.Submit(ctx, q, nil)
	drain, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	start := time.Now()
	if err := f.runtime.Close(drain); err != nil {
		t.Fatal(err)
	}
	ar, _ := a.Wait(ctx)
	br, _ := b.Wait(ctx)
	if ar.GetMutation().Outcome != pb.MutationOutcome_UNKNOWN || br.GetMutation().Outcome != pb.MutationOutcome_NOT_STARTED {
		t.Fatal(ar, br)
	}
	a.Ack()
	b.Ack()
	if time.Since(start) > time.Second || f.runtime.Snapshot().Active != 0 {
		t.Fatal("unbounded shutdown")
	}
}

func TestNativeWriteConcernAmbiguity(t *testing.T) {
	f := setup(t)
	wc := bson.D{{Key: "code", Value: 64}, {Key: "errmsg", Value: "isolated test concern failure"}}
	data := bson.D{{Key: "failCommands", Value: bson.A{"insert"}}, {Key: "appName", Value: "weir:" + f.db}, {Key: "writeConcernError", Value: wc}}
	testmongo.FailCommand(t, f.native, data, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var tickets []*Ticket
	for _, key := range []string{"wc_a", "wc_b"} {
		p := createPlan(t, f, key)
		ticket, e, _ := f.runtime.Submit(ctx, p, nil)
		if e != nil {
			t.Fatal(e)
		}
		tickets = append(tickets, ticket)
	}
	for _, ticket := range tickets {
		r, err := ticket.Wait(ctx)
		if err != nil || r.GetMutation().Outcome != pb.MutationOutcome_UNKNOWN {
			t.Fatal(err, r)
		}
		ticket.Ack()
	}
	filter := bson.D{}
	n, err := f.native.Database(f.db).Collection("records").CountDocuments(ctx, filter)
	if err != nil || n != 2 {
		t.Fatal("fault must follow real writes", n, err)
	}
}
func TestNativeCancellationDispatchRace(t *testing.T) {
	f := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("race_%d", i)
		p := createPlan(t, f, key)
		child, stop := context.WithCancel(ctx)
		ticket, e, _ := f.runtime.Submit(child, p, nil)
		if e != nil {
			stop()
			t.Fatal(e)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			if i%2 == 0 {
				time.Sleep(time.Millisecond)
			}
			stop()
		}()
		<-done
		// Observe the server terminal evidence, not the cancelled caller's transport.
		select {
		case <-ticket.ready:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		result := ticket.Result()
		if result.GetMutation().Outcome == pb.MutationOutcome_NOT_STARTED {
			filter := bson.D{{Key: "_id", Value: key}}
			err := f.native.Database(f.db).Collection("records").FindOne(ctx, filter).Err()
			if err != mongo.ErrNoDocuments {
				t.Fatal("false NOT_STARTED", err)
			}
		}
		ticket.Ack()
	}
}
func TestNativeGracefulDrainCompletesAccepted(t *testing.T) {
	f := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s := f.runtime.NewSession()
	defer s.Close()
	var tickets []*Ticket
	for i := 0; i < 6; i++ {
		p := createPlan(t, f, fmt.Sprintf("drain_%d", i))
		ticket, e, _ := f.runtime.Submit(ctx, p, s)
		if e != nil {
			t.Fatal(e)
		}
		tickets = append(tickets, ticket)
	}
	if err := f.runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for _, ticket := range tickets {
		if ticket.Result().GetMutation().Outcome != pb.MutationOutcome_APPLIED {
			t.Fatal(ticket.Result())
		}
		ticket.Ack()
	}
	if s.Outstanding() != 0 {
		t.Fatal("credits retained after drain")
	}
}
