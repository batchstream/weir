//go:build integration

package mongodb

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

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
		d := &pb.Document{ContentType: "application/bson", Data: raw}
		action := &pb.MutateRequest_Create{Create: d}
		m := &pb.MutateRequest{Resource: db + "/records/s:" + id, Action: action}
		opOperation := &pb.Command_Mutate{Mutate: m}
		opCommand := &pb.Command{Operation: opOperation}
		op := &pb.ExecuteRequest{Index: 1, Command: opCommand}
		p, f := prepareTestRecord(a, op)
		if f != nil {
			t.Fatal(f)
		}
		plans = append(plans, p)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results := a.executeRecords(ctx, plans)
	for _, r := range results {
		if r.GetMutationResult().Outcome != pb.MutationOutcome_UNKNOWN {
			t.Fatal(r)
		}
	}
	verifyReconnectRead(t, a, proxy, db+"/records/s:one")
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
		m := &pb.MutateRequest{Resource: db + "/records/s:" + id, Action: action}
		opOperation := &pb.Command_Mutate{Mutate: m}
		opCommand := &pb.Command{Operation: opOperation}
		op := &pb.ExecuteRequest{Index: 1, Command: opCommand}
		p, f := prepareTestRecord(a, op)
		if f != nil {
			t.Fatal(f)
		}
		plans = append(plans, p)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results := a.executeRecords(ctx, plans)
	for _, r := range results {
		if r.GetMutationResult().Outcome != pb.MutationOutcome_APPLIED {
			t.Fatal(r)
		}
	}
	if reads.Load() != 0 || deletes.Load() != 1 {
		t.Fatal("unexpected pre-read or no physical batching", reads.Load(), deletes.Load())
	}
}
