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

func setup(t *testing.T) fixture {
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
	cfg.Pool = uint64(l.Concurrency)
	a, err := mongodb.Open(ctx, cfg)
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
	req := &pb.ReadRequest{Resource: f.db + "/records/s:" + key}
	v := &pb.Operation_Read{Read: req}
	operation := &pb.Operation{Index: 1, Operation: v}
	p, err := f.runtime.adapter.PrepareOperation(operation)
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
	m := &pb.MutateRequest{Resource: f.db + "/records/s:" + key, Action: v}
	mv := &pb.Operation_Mutate{Mutate: m}
	operation := &pb.Operation{Index: 1, Operation: mv}
	p, err := f.runtime.adapter.PrepareOperation(operation)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func warm(t *testing.T, f fixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Same-key requests stay in distinct physical batches but may overlap across
	// independent callers. Their backlog exercises configured concurrent dispatch.
	p := readPlan(t, f, "warm")
	for round := 0; round < 3; round++ {
		var tickets []*Ticket
		for i := 0; i < 8; i++ {
			ticket, failure, _ := f.runtime.Submit(ctx, p, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			tickets = append(tickets, ticket)
		}
		for _, ticket := range tickets {
			result, err := ticket.Wait(ctx)
			if err != nil || result.GetRead().GetFailure() != nil {
				t.Fatal("warm point read failed", err, result)
			}
			ticket.Ack()
		}
	}
	if f.runtime.Snapshot().ConcurrencyLimit != 2 {
		t.Fatal("configured concurrency changed", f.runtime.Snapshot())
	}
}
func TestNativeIndependentReadsOverlap(t *testing.T) {
	f := setup(t)
	warm(t, f)
	data := bson.D{{Key: "failCommands", Value: bson.A{"find"}}, {Key: "appName", Value: "weir:mongo"}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 200}}
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
	t.Log("shared bulkWrite: short caller timed out; both persisted; surviving caller APPLIED")
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
	data := bson.D{{Key: "failCommands", Value: bson.A{"bulkWrite"}}, {Key: "appName", Value: "weir:mongo"}, {Key: "closeConnection", Value: true}}
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
	f.runtime.mu.Lock()
	f.runtime.limits.Concurrency = 1
	f.runtime.mu.Unlock()
	data := bson.D{{Key: "failCommands", Value: bson.A{"bulkWrite"}}, {Key: "appName", Value: "weir:mongo"}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 500}}
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
	data := bson.D{{Key: "failCommands", Value: bson.A{"bulkWrite"}}, {Key: "appName", Value: "weir:mongo"}, {Key: "writeConcernError", Value: wc}}
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
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	operations := make([]*pb.Operation, 6)
	for i := range operations {
		plan := createPlan(t, f, fmt.Sprintf("drain_%d", i))
		operations[i] = plan.Operation
		operations[i].Index = uint64(i + 1)
	}
	prepared, failure := f.runtime.PrepareBatch(operations)
	if failure != nil {
		t.Fatal(failure)
	}
	ticket, failure, _ := f.runtime.SubmitBatch(ctx, prepared)
	if failure != nil {
		t.Fatal(failure)
	}
	results, err := ticket.WaitBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		if result.GetMutation().Outcome != pb.MutationOutcome_APPLIED {
			t.Fatal(result)
		}
	}
	ticket.Ack()
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

func TestRouteNativeBackendIODeadline(t *testing.T) {
	for _, test := range []struct {
		name, command string
		timeout       time.Duration
		block         int32
		completion    pb.NativeCompletion
	}{
		{name: "configured_command", command: "count", timeout: 100 * time.Millisecond, block: 500, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE},
		{name: "configured_qualification", command: "listCollections", timeout: 100 * time.Millisecond, block: 500, completion: pb.NativeCompletion_NATIVE_NOT_STARTED},
		{name: "default_command", command: "count", timeout: 2 * time.Second, block: 3000, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := setup(t)
			f.runtime.mu.Lock()
			f.runtime.limits.BackendTimeout = test.timeout
			f.runtime.mu.Unlock()
			data := bson.D{{Key: "failCommands", Value: bson.A{test.command}}, {Key: "appName", Value: "weir:mongo"}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: test.block}}
			testmongo.FailCommand(t, f.native, data, 1)
			command := bson.D{{Key: "count", Value: "records"}}
			raw, err := bson.Marshal(command)
			if err != nil {
				t.Fatal(err)
			}
			descriptor := &pb.Document{MediaType: mongodb.NativeDescriptor}
			open := &pb.NativeOpen{Resource: f.db + "/records", Descriptor_: descriptor, BodyMediaType: "application/bson"}
			native := &pb.NativeRequest{Open: open, Body: raw}
			variant := &pb.Command_Native{Native: native}
			call := &pb.Command{Version: 1, Operation: variant}
			work, failure := f.runtime.PrepareCommand(1, call)
			if failure != nil {
				t.Fatal(failure)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			session := f.runtime.NewSession()
			defer session.Close()
			started := time.Now()
			ticket, failure, _ := f.runtime.Submit(ctx, work, session)
			if failure != nil {
				t.Fatal(failure)
			}
			var end *pb.NativeEnd
			for {
				select {
				case emission := <-session.Events:
					if emission.Event.GetNativeEnd() != nil {
						if end != nil {
							t.Fatal("multiple native completion events")
						}
						end = emission.Event.GetNativeEnd()
					}
					emission.Release()
					if emission.End {
						ticket.Ack()
						goto completed
					}
				case <-ctx.Done():
					t.Fatal("backend deadline failed to release request", ctx.Err())
				}
			}
		completed:
			elapsed := time.Since(started)
			if end.GetCompletion() != test.completion || end.GetFailure().GetCode() != pb.FailureCode_DEADLINE_EXCEEDED || elapsed > test.timeout+500*time.Millisecond {
				t.Fatal("native backend I/O escaped configured cap", end, elapsed, test.timeout)
			}
			waitReleased(t, f.runtime)
			filter := bson.D{}
			count, err := f.native.Database(f.db).Collection("records").CountDocuments(ctx, filter)
			if err != nil || count != 0 {
				t.Fatal("deadline affected independent backend connection", count, err)
			}
			t.Logf("blocked %s: configured=%s elapsed=%s completion=%s", test.command, test.timeout, elapsed, end.Completion)
		})
	}
}

func TestRouteLuaDatabaseIODeadlineAndCommitUncertainty(t *testing.T) {
	for _, command := range []string{"find", "commitTransaction"} {
		t.Run(command, func(t *testing.T) {
			f := setup(t)
			timeout := 100 * time.Millisecond
			f.runtime.mu.Lock()
			f.runtime.limits.BackendTimeout = timeout
			f.runtime.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			document := bson.D{{Key: "_id", Value: "lua-timeout"}, {Key: "n", Value: int32(1)}}
			collection := f.native.Database(f.db).Collection("records")
			if _, err := collection.InsertOne(ctx, document); err != nil {
				t.Fatal(err)
			}
			data := bson.D{{Key: "failCommands", Value: bson.A{command}}, {Key: "appName", Value: "weir:mongo"}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 500}}
			testmongo.FailCommand(t, f.native, data, 1)
			program := &pb.ProgramTransform{Runtime: "lua.v1", Source: []byte(`return weir.replace(weir.set(current, "n", weir.add(weir.get(current, "n"), weir.i32("1"))))`)}
			form := &pb.Transform_Program{Program: program}
			transform := &pb.Transform{Form: form}
			action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
			mutation := &pb.MutateRequest{Resource: f.db + "/records/s:lua-timeout", Action: action}
			variant := &pb.Operation_Mutate{Mutate: mutation}
			operation := &pb.Operation{Index: 1, Operation: variant}
			work, failure := f.runtime.adapter.PrepareOperation(operation)
			if failure != nil {
				t.Fatal(failure)
			}
			started := time.Now()
			ticket, failure, _ := f.runtime.Submit(ctx, work, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			result, err := ticket.Wait(ctx)
			elapsed := time.Since(started)
			expected := pb.MutationOutcome_NOT_APPLIED
			if command == "commitTransaction" {
				expected = pb.MutationOutcome_UNKNOWN
			}
			if err != nil || result.GetMutation().GetOutcome() != expected || result.GetMutation().GetFailure().GetCode() != pb.FailureCode_DEADLINE_EXCEEDED || elapsed > timeout+500*time.Millisecond {
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
			t.Logf("blocked %s: cap=%s elapsed=%s outcome=%s", command, timeout, elapsed, expected)
		})
	}
}
