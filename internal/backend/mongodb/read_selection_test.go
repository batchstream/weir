package mongodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

// Only the selected attempt fails. Detached record cleanup still has a working
// mock server, so an unnecessary cleanup is observable without racing timers.
type readSelectionDeployment struct {
	*drivertest.MockDeployment
	selections     atomic.Int32
	failAt         int32
	entered        chan struct{}
	selectionError func(context.Context) error
}

func (d *readSelectionDeployment) SelectServer(ctx context.Context, selector description.ServerSelector) (driver.Server, error) {
	if d.selections.Add(1) == d.failAt {
		if d.entered != nil {
			close(d.entered)
			<-ctx.Done()
		}
		return nil, d.selectionError(ctx)
	}
	return d.MockDeployment.SelectServer(ctx, selector)
}

type readSelectionCommands struct {
	mu    sync.Mutex
	names []string
	raw   []bson.Raw
}

func (c *readSelectionCommands) started(_ context.Context, e *event.CommandStartedEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.names = append(c.names, e.CommandName)
	c.raw = append(c.raw, append(bson.Raw(nil), e.Command...))
}

func (c *readSelectionCommands) snapshot() ([]string, []bson.Raw) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.names...), append([]bson.Raw(nil), c.raw...)
}

type readSelectionOptions struct {
	responses      []bson.D
	failAt         int32
	entered        chan struct{}
	selectionError func(context.Context) error
	lostReply      bool
}

func readSelectionAdapter(t *testing.T, opts readSelectionOptions) (*Adapter, *readSelectionDeployment, *readSelectionCommands) {
	t.Helper()
	deployment := &readSelectionDeployment{
		MockDeployment: drivertest.NewMockDeployment(opts.responses...),
		failAt:         opts.failAt, entered: opts.entered, selectionError: opts.selectionError,
	}
	commands := &readSelectionCommands{}
	monitor := &event.CommandMonitor{Started: commands.started}
	if opts.lostReply {
		monitor.Failed = func(_ context.Context, e *event.CommandFailedEvent) {
			if e.CommandName == "find" {
				cleanup := bson.D{{Key: "ok", Value: 1}}
				deployment.AddResponses(cleanup)
			}
		}
	}
	clientOptions := options.Client().SetRetryReads(false).SetRetryWrites(false).SetMaxAdaptiveRetries(0).SetMonitor(monitor)
	clientOptions.Deployment = deployment
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{Store: "mongo"}
	adapter := &Adapter{client: client, deployment: deployment, commandMonitor: monitor, config: config}
	initializeTestLifetime(adapter)
	t.Cleanup(func() {
		if err := adapter.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return adapter, deployment, commands
}

func readSelectionPlans(t *testing.T, adapter *Adapter, ids []string) []*execution.Plan {
	t.Helper()
	plans := make([]*execution.Plan, 0, len(ids))
	for i, id := range ids {
		opts := batchOperationOptions{resource: "db/records/s:" + id, action: "read", index: uint64(i + 1)}
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	return plans
}

func TestMongoInitialUnsentReadSelectionEndsOnlyLocalSession(t *testing.T) {
	entered := make(chan struct{})
	cleanup := bson.D{{Key: "ok", Value: 1}}
	opts := readSelectionOptions{
		responses: []bson.D{cleanup}, failAt: 1, entered: entered,
		selectionError: func(ctx context.Context) error {
			selection := topology.ServerSelectionError{Wrapped: ctx.Err()}
			return selection
		},
	}
	adapter, deployment, commands := readSelectionAdapter(t, opts)
	plans := readSelectionPlans(t, adapter, []string{"a"})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan []*pb.Event, 1)
	go func() { done <- adapter.executeReads(ctx, plans) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("initial read never entered server selection")
	}
	if sessions := adapter.client.NumberSessionsInProgress(); sessions != 1 {
		t.Error("fresh explicit session not owned while selection blocked", sessions)
	}
	cancel()
	var replies []*pb.Event
	select {
	case replies = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("read did not finish after selection cancellation")
	}
	names, _ := commands.snapshot()
	t.Logf("before Disconnect: selections=%d commands=%v sessions=%d failure=%v", deployment.selections.Load(), names, adapter.client.NumberSessionsInProgress(), replies[0].GetReadResult().GetFailure())
	if replies[0].GetReadResult().GetFailure().GetCode() != pb.FailureCode_CANCELLED {
		t.Error("selection cancellation changed the public read failure", replies)
	}
	if sessions := adapter.client.NumberSessionsInProgress(); sessions != 0 {
		t.Error("local explicit session retained before Disconnect", sessions)
	}
	if deployment.selections.Load() != 1 || len(names) != 0 {
		t.Fatal("unsent initial read issued remote record cleanup", deployment.selections.Load(), names)
	}
}

func TestMongoInitialReadUncertainErrorsRetainSessionCleanup(t *testing.T) {
	selection := topology.ServerSelectionError{Wrapped: context.Canceled}
	commandError := mongo.CommandError{Code: 91, Message: "uncertain", Wrapped: selection}
	cases := []struct {
		name string
		err  error
	}{
		{name: "context", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "wrapped selection", err: fmt.Errorf("uncertain: %w", selection)},
		{name: "joined selection", err: errors.Join(errors.New("uncertain"), selection)},
		{name: "pointer selection", err: &selection},
		{name: "command containing selection", err: commandError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var sessionID bson.Raw
			cleanup := bson.D{{Key: "ok", Value: 1}}
			opts := readSelectionOptions{responses: []bson.D{cleanup}, failAt: 1, selectionError: func(ctx context.Context) error {
				sessionID = append(bson.Raw(nil), mongo.SessionFromContext(ctx).ID()...)
				return tc.err
			}}
			adapter, deployment, commands := readSelectionAdapter(t, opts)
			plans := readSelectionPlans(t, adapter, []string{"a"})
			replies := adapter.executeReads(t.Context(), plans)
			names, raw := commands.snapshot()
			t.Logf("before Disconnect: selections=%d commands=%v failure=%v", deployment.selections.Load(), names, replies[0].GetReadResult().GetFailure())
			if !reflect.DeepEqual(names, []string{"killSessions"}) || deployment.selections.Load() != 2 {
				t.Fatal("uncertain error skipped cleanup or replayed read", names, deployment.selections.Load())
			}
			ids, err := raw[0].Lookup("killSessions").Array().Values()
			if err != nil || len(ids) != 1 || !bytes.Equal(ids[0].Document(), sessionID) || len(sessionID) == 0 {
				t.Fatal("cleanup did not target the read's explicit session", raw)
			}
			if adapter.client.NumberSessionsInProgress() != 0 || replies[0].GetReadResult().GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE {
				t.Fatal("uncertain read changed session retirement or public failure", replies)
			}
		})
	}
}

func TestMongoSentInitialReadRetainsUnknownCursorCleanup(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost_reply=%v", lost), func(t *testing.T) {
			malformed := bson.D{{Key: "ok", Value: 1}}
			cleanup := bson.D{{Key: "ok", Value: 1}}
			opts := readSelectionOptions{responses: []bson.D{malformed, cleanup}, lostReply: lost}
			if lost {
				opts.responses = nil
			}
			adapter, deployment, commands := readSelectionAdapter(t, opts)
			plans := readSelectionPlans(t, adapter, []string{"a"})
			replies := adapter.executeReads(t.Context(), plans)
			names, raw := commands.snapshot()
			if !reflect.DeepEqual(names, []string{"find", "killSessions"}) || deployment.selections.Load() != 2 {
				t.Fatal("sent initial read skipped cleanup or replayed find", names, deployment.selections.Load())
			}
			ids, err := raw[1].Lookup("killSessions").Array().Values()
			if err != nil || len(ids) != 1 || !bytes.Equal(ids[0].Document(), raw[0].Lookup("lsid").Document()) {
				t.Fatal("unknown cursor cleanup changed session identity", raw)
			}
			if adapter.client.NumberSessionsInProgress() != 0 || replies[0].GetReadResult().GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE {
				t.Fatal("uncertain sent read changed session retirement or public failure", replies)
			}
			t.Logf("before Disconnect: selections=%d commands=%v failure=%v", deployment.selections.Load(), names, replies[0].GetReadResult().GetFailure())
		})
	}
}

func TestMongoLaterReadSelectionRetainsKnownCursorCleanup(t *testing.T) {
	document := bson.D{{Key: "_id", Value: "a"}}
	cursor := bson.D{{Key: "id", Value: int64(55)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document}}}
	cleanup := bson.D{{Key: "ok", Value: 1}, {Key: "cursorsKilled", Value: bson.A{int64(55)}}, {Key: "cursorsAlive", Value: bson.A{}}, {Key: "cursorsNotFound", Value: bson.A{}}, {Key: "cursorsUnknown", Value: bson.A{}}}
	opts := readSelectionOptions{responses: []bson.D{readCursorResponse(cursor), cleanup}, failAt: 2, selectionError: func(context.Context) error {
		selection := topology.ServerSelectionError{Wrapped: context.DeadlineExceeded}
		return selection
	}}
	adapter, deployment, commands := readSelectionAdapter(t, opts)
	plans := readSelectionPlans(t, adapter, []string{"a", "b"})
	replies := adapter.executeReads(t.Context(), plans)
	names, raw := commands.snapshot()
	if !reflect.DeepEqual(names, []string{"find", "killCursors"}) || deployment.selections.Load() != 3 {
		t.Fatal("later selection failure skipped known cursor cleanup or replayed find/getMore", names, deployment.selections.Load())
	}
	if raw[1].Lookup("killCursors").StringValue() != "records" || raw[1].Lookup("$db").StringValue() != "db" || !bytes.Equal(raw[0].Lookup("lsid").Document(), raw[1].Lookup("lsid").Document()) {
		t.Fatal("known cursor cleanup changed namespace or explicit session", raw)
	}
	ids, err := raw[1].Lookup("cursors").Array().Values()
	if err != nil || len(ids) != 1 || ids[0].Int64() != 55 {
		t.Fatal("known cursor cleanup changed cursor identity", raw)
	}
	if replies[0].GetReadResult().GetDocument() == nil || replies[1].GetReadResult().GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE || adapter.client.NumberSessionsInProgress() != 0 {
		t.Fatal("later failure erased completed read or retained session", replies)
	}
	t.Logf("before Disconnect: selections=%d commands=%v later_failure=%v", deployment.selections.Load(), names, replies[1].GetReadResult().GetFailure())
}

func TestMongoRuntimeDrainCancelsUnsentReadWithoutRemoteCleanup(t *testing.T) {
	entered := make(chan struct{})
	cleanup := bson.D{{Key: "ok", Value: 1}}
	opts := readSelectionOptions{
		responses: []bson.D{collectionQualificationResponse("db", "records"), cleanup}, failAt: 2, entered: entered,
		selectionError: func(ctx context.Context) error {
			selection := topology.ServerSelectionError{Wrapped: ctx.Err()}
			return selection
		},
	}
	adapter, deployment, commands := readSelectionAdapter(t, opts)
	var sessionsAtDisconnect atomic.Int32
	// Observe local ownership before client shutdown; endSessions during
	// Disconnect is legitimate and is not record killSessions/killCursors.
	monitor := adapter.commandMonitor
	started := monitor.Started
	monitor.Started = func(ctx context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "endSessions" {
			sessionsAtDisconnect.Store(int32(adapter.client.NumberSessionsInProgress()))
		}
		started(ctx, e)
	}
	sessionsAtDisconnect.Store(-1)
	limits := store.DefaultLimits()
	runtime, err := store.New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	caller, cancelCaller := context.WithCancel(t.Context())
	defer cancelCaller()
	read := &pb.ReadRequest{Resource: "db/records/s:a"}
	operation := &pb.Command_Read{Read: read}
	command := &pb.Command{Operation: operation}
	record, err := execution.NewRecord("mongo", 1, command)
	if err != nil {
		t.Fatal(err)
	}
	work, failure := runtime.PrepareRecord(caller, record)
	if failure != nil {
		t.Fatal(failure)
	}
	ticket, failure := runtime.Submit(caller, work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("normal Runtime qualification/Execute never reached read selection")
	}
	if adapter.client.NumberSessionsInProgress() != 1 {
		t.Error("blocked runtime read lacks actual session ownership", adapter.client.NumberSessionsInProgress())
	}
	drain, expireDrain := context.WithCancel(t.Context())
	runtime.BeginDrain()
	expireDrain()
	if err := runtime.Close(drain); err != nil {
		t.Fatal("unsent read did not join runtime Close", err)
	}
	if caller.Err() != nil {
		t.Fatal("runtime shutdown depended on caller cancellation", caller.Err())
	}
	if sessionsAtDisconnect.Load() != 0 || adapter.client.NumberSessionsInProgress() != 0 {
		t.Fatal("session was not retired before Disconnect", sessionsAtDisconnect.Load(), adapter.client.NumberSessionsInProgress())
	}
	names, _ := commands.snapshot()
	t.Logf("after Close: selections=%d commands=%v", deployment.selections.Load(), names)
	if !reflect.DeepEqual(names, []string{"listCollections", "endSessions"}) {
		t.Fatal("runtime issued forbidden record cleanup or replay", names)
	}
	result, err := ticket.Wait(t.Context())
	if err != nil || result.GetReadResult().GetFailure() == nil {
		t.Fatal("retired read lost its failure evidence", result, err)
	}
}
