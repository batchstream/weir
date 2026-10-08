//go:build integration

package store

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type fixture struct {
	runtime *Runtime
	native  *mongo.Client
	db      string
}

func setup(t *testing.T, manual bool) fixture {
	backend := testmongo.Open(t)
	native, db := backend.Admin, backend.DB
	cfg := mongodb.Config{URI: backend.URI, Store: "mongo"}
	parsed, err := url.Parse(cfg.URI)
	if err != nil {
		t.Fatal("invalid owned MongoDB fixture URI")
	}
	if parsed.User != nil {
		cfg.Username = parsed.User.Username()
		cfg.Password, _ = parsed.User.Password()
		parsed.User = nil
	}
	cfg.URI = parsed.String()

	l := DefaultLimits()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	a, err := mongodb.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := newRuntime(a, l)
	if !manual {
		go r.loop()
	}
	t.Cleanup(func() {
		if manual {
			go r.loop()
		}
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
	read := &pb.ReadRequest{Resource: f.db + "/records/s:" + key}
	operation := &pb.Command_Read{Read: read}
	command := &pb.Command{Operation: operation}
	record, err := execution.NewRecord("mongo", 1, command)
	if err != nil {
		t.Fatal(err)
	}
	work, failure := f.runtime.PrepareRecord(record)
	if failure != nil {
		t.Fatal(failure)
	}
	return work
}

func createMutation(t *testing.T, f fixture, key string) *pb.MutateRequest {
	t.Helper()
	doc := bson.D{{Key: "_id", Value: key}, {Key: "n", Value: int32(1)}}
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{ContentType: "application/bson", Data: raw}
	action := &pb.MutateRequest_Create{Create: document}
	mutation := &pb.MutateRequest{Resource: f.db + "/records/s:" + key, Action: action}
	return mutation
}

func createPlan(t *testing.T, f fixture, key string) *execution.Plan {
	t.Helper()
	mutation := createMutation(t, f, key)
	operation := &pb.Command_Mutate{Mutate: mutation}
	command := &pb.Command{Operation: operation}
	record, err := execution.NewRecord("mongo", 1, command)
	if err != nil {
		t.Fatal(err)
	}
	work, failure := f.runtime.PrepareRecord(record)
	if failure != nil {
		t.Fatal(failure)
	}
	return work
}

func submitNativeMutations(t *testing.T, ctx context.Context, f fixture, requests []*pb.MutateRequest) []*Ticket {
	t.Helper()
	var tickets []*Ticket
	for index, request := range requests {
		operation := &pb.Command_Mutate{Mutate: request}
		command := &pb.Command{Operation: operation}
		record, err := execution.NewRecord("mongo", uint64(index+1), command)
		if err != nil {
			t.Fatal(err)
		}
		work, failure := f.runtime.PrepareRecord(record)
		if failure != nil {
			t.Fatal(failure)
		}
		ticket, failure, _ := f.runtime.Submit(ctx, work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	return tickets
}

func executeNativeBatch(t *testing.T, f fixture, count int) {
	t.Helper()
	selected := selectCrossBatch(f.runtime)
	if selected == nil || len(selected.items) != count {
		t.Fatal("admitted independent records did not share physical execution", selected)
	}
	f.runtime.execute(selected)
}

func TestNativeIndependentReadsOverlap(t *testing.T) {
	f := setup(t, false)
	data := bson.D{{Key: "failCommands", Value: bson.A{"find"}}, {Key: "appName", Value: "weir:mongo"}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 200}}
	testmongo.FailCommand(t, f.native, data, 2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p := readPlan(t, f, "same")
	start := time.Now()
	a, failure, _ := f.runtime.Submit(ctx, p, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	// Dispatch the first ticket before admitting the second so this exercises
	// two physical finds rather than one coalesced find containing both reads.
	for f.runtime.Snapshot().Active == 0 {
		select {
		case <-ctx.Done():
			t.Fatal("first physical read did not dispatch", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	second := readPlan(t, f, "same")
	b, failure, _ := f.runtime.Submit(ctx, second, nil)
	if failure != nil {
		t.Fatal(failure)
	}
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
func TestNativeIndependentMutationDeadlines(t *testing.T) {
	f := setup(t, false)
	data := bson.D{{Key: "failCommands", Value: bson.A{"bulkWrite"}}, {Key: "appName", Value: "weir:mongo"}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 150}}
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
	<-short.Done()
	if short.Err() != context.DeadlineExceeded {
		t.Fatal("short caller did not reach its independent deadline", short.Err())
	}
	result, err := b.Wait(long)
	if err != nil || result.GetMutationResult().Outcome != pb.MutationOutcome_APPLIED {
		t.Fatal("unrelated caller cancelled", err, result)
	}
	b.Ack()
	// Keep the admitted ticket until its terminal evidence can be compared with
	// the database. Waiting on the expired transport would abandon that result.
	shortResult, err := a.Wait(long)
	if err != nil {
		t.Fatal("short caller's backend execution did not finish", err)
	}
	shortFilter := bson.D{{Key: "_id", Value: "short"}}
	count, err := f.native.Database(f.db).Collection("records").CountDocuments(long, shortFilter)
	if err != nil {
		t.Fatal(err)
	}
	switch shortResult.GetMutationResult().GetOutcome() {
	case pb.MutationOutcome_NOT_STARTED, pb.MutationOutcome_NOT_APPLIED:
		if count != 0 {
			t.Fatal("definite no-effect outcome contradicted persisted write", shortResult, count)
		}
	case pb.MutationOutcome_APPLIED:
		if count != 1 {
			t.Fatal("acknowledged short write was not persisted", shortResult, count)
		}
	case pb.MutationOutcome_UNKNOWN:
		if shortResult.GetMutationResult().GetFailure() == nil {
			t.Fatal("uncertain canceled write lost its failure", shortResult)
		}
	default:
		t.Fatal("short caller lost terminal outcome", shortResult)
	}
	a.Ack()
	longFilter := bson.D{{Key: "_id", Value: "long"}}
	if err := f.native.Database(f.db).Collection("records").FindOne(long, longFilter).Err(); err != nil {
		t.Fatal("independent surviving caller did not persist", err)
	}
}
func TestNativeBatchItemAndUncertainErrors(t *testing.T) {
	f := setup(t, true)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	existing := bson.D{{Key: "_id", Value: "exists"}, {Key: "n", Value: 1}}
	if _, err := f.native.Database(f.db).Collection("records").InsertOne(ctx, existing); err != nil {
		t.Fatal(err)
	}
	requests := []*pb.MutateRequest{createMutation(t, f, "exists"), createMutation(t, f, "new")}
	tickets := submitNativeMutations(t, ctx, f, requests)
	executeNativeBatch(t, f, 2)
	for index, ticket := range tickets {
		result, err := ticket.Wait(ctx)
		want := pb.MutationOutcome_APPLIED
		if index == 0 {
			want = pb.MutationOutcome_NOT_APPLIED
		}
		if err != nil || ticket.plan.ID != uint64(index+1) || result.GetMutationResult().GetOutcome() != want {
			t.Fatal("item-error batch lost independent result association", err, result)
		}
		ticket.Ack()
	}
	data := bson.D{{Key: "failCommands", Value: bson.A{"bulkWrite"}}, {Key: "appName", Value: "weir:mongo"}, {Key: "closeConnection", Value: true}}
	testmongo.FailCommand(t, f.native, data, 1)
	requests = []*pb.MutateRequest{createMutation(t, f, "unknown_a"), createMutation(t, f, "unknown_b")}
	tickets = submitNativeMutations(t, ctx, f, requests)
	executeNativeBatch(t, f, 2)
	for _, ticket := range tickets {
		result, err := ticket.Wait(ctx)
		if err != nil || result.GetMutationResult().GetOutcome() != pb.MutationOutcome_UNKNOWN {
			t.Fatal("lost reply justified a definite write outcome", err, result)
		}
		ticket.Ack()
	}
	waitReleased(t, f.runtime)
}
func TestNativeShutdownPreservesDispatchedEvidence(t *testing.T) {
	f := setup(t, false)
	data := bson.D{{Key: "failCommands", Value: bson.A{"bulkWrite"}}, {Key: "appName", Value: "weir:mongo"}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 500}}
	testmongo.FailCommand(t, f.native, data, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	p := createPlan(t, f, "executing")
	a, failure, _ := f.runtime.Submit(ctx, p, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	for f.runtime.Snapshot().Active == 0 {
		select {
		case <-ctx.Done():
			t.Fatal("executing ticket never dispatched", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	q := createPlan(t, f, "queued")
	b, failure, _ := f.runtime.Submit(ctx, q, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	drain, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	start := time.Now()
	if err := f.runtime.Close(drain); err != nil {
		t.Fatal(err)
	}
	ar, err := a.Wait(ctx)
	if err != nil || ar.GetMutationResult() == nil {
		t.Fatal("forced shutdown discarded the executing ticket's evidence", ar, err)
	}
	br, err := b.Wait(ctx)
	if err != nil || br.GetMutationResult() == nil {
		t.Fatal("forced shutdown discarded the queued ticket's evidence", br, err)
	}
	if ar.GetMutationResult().GetOutcome() != pb.MutationOutcome_UNKNOWN ||
		(br.GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_STARTED && br.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED && br.GetMutationResult().GetOutcome() != pb.MutationOutcome_UNKNOWN) {
		t.Fatal(ar, br)
	}
	if snapshot := f.runtime.Snapshot(); snapshot.Retained != 2 || snapshot.ResultBytes != 2*execution.ResultOverheadBytes {
		t.Fatal("forced shutdown released synchronous evidence before acknowledgement", snapshot)
	}
	a.Ack()
	b.Ack()
	if snapshot := f.runtime.Snapshot(); time.Since(start) > time.Second || snapshot.Active != 0 || snapshot.Retained != 0 || snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 || snapshot.WorkingBytes != 0 {
		t.Fatal("shutdown or acknowledgement retained execution state", snapshot)
	}
}

func TestNativeWriteConcernAmbiguity(t *testing.T) {
	f := setup(t, true)
	wc := bson.D{{Key: "code", Value: 64}, {Key: "errmsg", Value: "isolated test concern failure"}}
	data := bson.D{{Key: "failCommands", Value: bson.A{"bulkWrite"}}, {Key: "appName", Value: "weir:mongo"}, {Key: "writeConcernError", Value: wc}}
	testmongo.FailCommand(t, f.native, data, 1)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	requests := []*pb.MutateRequest{createMutation(t, f, "wc_a"), createMutation(t, f, "wc_b")}
	tickets := submitNativeMutations(t, ctx, f, requests)
	executeNativeBatch(t, f, 2)
	for index, ticket := range tickets {
		result, err := ticket.Wait(ctx)
		if err != nil || ticket.plan.ID != uint64(index+1) || result.GetMutationResult().GetOutcome() != pb.MutationOutcome_UNKNOWN {
			t.Fatal("write concern batch lost association or uncertainty", err, result)
		}
		ticket.Ack()
	}
	filter := bson.D{}
	n, err := f.native.Database(f.db).Collection("records").CountDocuments(ctx, filter)
	if err != nil || n != 2 {
		t.Fatal("fault must follow real writes", n, err)
	}
	waitReleased(t, f.runtime)
}
func TestNativeCancellationDispatchRace(t *testing.T) {
	f := setup(t, false)
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
		result, err := ticket.Wait(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if result.GetMutationResult().Outcome == pb.MutationOutcome_NOT_STARTED {
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
	f := setup(t, false)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	requests := make([]*pb.MutateRequest, 6)
	for index := range requests {
		requests[index] = createMutation(t, f, fmt.Sprintf("drain_%d", index))
	}
	tickets := submitNativeMutations(t, ctx, f, requests)
	f.runtime.BeginDrain()
	for _, ticket := range tickets {
		result, err := ticket.Wait(ctx)
		if err != nil || result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
			t.Fatal("accepted write failed during graceful drain", err, result)
		}
		ticket.Ack()
	}
	if err := f.runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := f.runtime.Snapshot()
	if snapshot.Pending != 0 || snapshot.Active != 0 || snapshot.Retained != 0 || snapshot.Publishers != 0 || snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 || snapshot.WorkingBytes != 0 {
		t.Fatal("drain retained tasks or buffers", snapshot)
	}
	filter := bson.D{}
	count, err := f.native.Database(f.db).Collection("records").CountDocuments(ctx, filter)
	if err != nil || count != 6 {
		t.Fatal("accepted writes missing after drain", count, err)
	}
}

func TestNativeCallerCancellationReleasesExecution(t *testing.T) {
	for _, commandName := range []string{"count", "listCollections"} {
		t.Run(commandName, func(t *testing.T) {
			f := setup(t, false)
			data := bson.D{{Key: "failCommands", Value: bson.A{commandName}}, {Key: "appName", Value: "weir:mongo"}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 500}}
			testmongo.FailCommand(t, f.native, data, 1)
			command := bson.D{{Key: "count", Value: "records"}}
			raw, err := bson.Marshal(command)
			if err != nil {
				t.Fatal(err)
			}
			body := &pb.Document{ContentType: "application/bson", Data: raw}
			native := &pb.NativeRequest{Resource: f.db + "/records", Request: body}
			variant := &pb.Command_Native{Native: native}
			call := &pb.Command{Operation: variant}
			work, failure := f.runtime.PrepareCommand(1, call)
			if failure != nil {
				t.Fatal(failure)
			}
			caller, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			session := f.runtime.NewSession()
			defer session.Close()
			ticket, failure, _ := f.runtime.Submit(caller, work, session)
			if failure != nil {
				t.Fatal(failure)
			}
			for caller.Err() == nil {
				select {
				case emission := <-session.Events:
					emission.Release()
				case <-caller.Done():
				}
			}
			ticket.Abandon()
			session.Close()
			waitReleased(t, f.runtime)
			if caller.Err() != context.DeadlineExceeded {
				t.Fatal("caller deadline cause lost", caller.Err())
			}
		})
	}
}

func TestLuaCallerDeadlineKeepsCommitUncertainty(t *testing.T) {
	for _, command := range []string{"find", "commitTransaction"} {
		t.Run(command, func(t *testing.T) {
			f := setup(t, false)
			timeout := 100 * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			document := bson.D{{Key: "_id", Value: "lua-timeout"}, {Key: "n", Value: int32(1)}}
			collection := f.native.Database(f.db).Collection("records")
			if _, err := collection.InsertOne(ctx, document); err != nil {
				t.Fatal(err)
			}
			data := bson.D{{Key: "failCommands", Value: bson.A{command}}, {Key: "appName", Value: "weir:mongo"}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 500}}
			testmongo.FailCommand(t, f.native, data, 1)
			program := &pb.LuaTransform{Source: []byte(`return function(current, incoming) current.n = current.n + 1; return current end`)}
			form := &pb.Transform_Lua{Lua: program}
			transform := &pb.Transform{Form: form}
			action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
			mutation := &pb.MutateRequest{Resource: f.db + "/records/s:lua-timeout", Action: action}
			operation := &pb.Command_Mutate{Mutate: mutation}
			call := &pb.Command{Operation: operation}
			record, err := execution.NewRecord("mongo", 1, call)
			if err != nil {
				t.Fatal(err)
			}
			work, failure := f.runtime.PrepareRecord(record)
			if failure != nil {
				t.Fatal(failure)
			}
			caller, stopCaller := context.WithTimeout(ctx, timeout)
			defer stopCaller()
			started := time.Now()
			ticket, failure, _ := f.runtime.Submit(caller, work, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			result, err := ticket.Wait(ctx)
			elapsed := time.Since(started)
			expected := pb.MutationOutcome_NOT_APPLIED
			if command == "commitTransaction" {
				expected = pb.MutationOutcome_UNKNOWN
			}
			if err != nil || result.GetMutationResult().GetOutcome() != expected || result.GetMutationResult().GetFailure().GetCode() != pb.FailureCode_DEADLINE_EXCEEDED || elapsed > timeout+500*time.Millisecond {
				t.Fatal("Lua database I/O escaped deadline or lost uncertainty", err, result, elapsed)
			}
			ticket.Ack()
			waitReleased(t, f.runtime)
			filter := bson.D{{Key: "_id", Value: "lua-timeout"}}
			deadline := time.Now().Add(time.Second)
			for {
				raw, err := collection.FindOne(ctx, filter).Raw()
				if err != nil {
					t.Fatal(err)
				}
				value := raw.Lookup("n").Int32()
				if command == "find" {
					if value != 1 {
						t.Fatal("aborted transaction changed the record", value)
					}
					break
				}
				if value == 2 {
					break
				}
				if value != 1 || time.Now().After(deadline) {
					t.Fatal("uncertain commit was missing or Lua replayed", value)
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Logf("blocked %s: caller deadline=%s elapsed=%s outcome=%s", command, timeout, elapsed, expected)
		})
	}
}
