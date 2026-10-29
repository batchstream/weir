package mongodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
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
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/mnet"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

// This seam preserves the actual driver's command/session paths. find is sent
// before its reply is lost; killSessions blocks until its cleanup context ends.
type retirementDeployment struct {
	*drivertest.MockDeployment
	command        atomic.Value
	findSent       chan struct{}
	cleanupSent    chan struct{}
	disconnected   chan struct{}
	endReadContext chan context.Context
	slowEnd        bool
	disconnectErr  error
}

func (d *retirementDeployment) SelectServer(context.Context, description.ServerSelector) (driver.Server, error) {
	return d, nil
}
func (d *retirementDeployment) Connection(ctx context.Context) (*mnet.Connection, error) {
	base, err := d.MockDeployment.Connection(ctx)
	if err != nil {
		return nil, err
	}
	wrapper := &retirementConnection{Connection: base, deployment: d}
	connection := &mnet.Connection{ReadWriteCloser: wrapper, Describer: base.Describer}
	return connection, nil
}
func (d *retirementDeployment) Disconnect(ctx context.Context) error {
	defer close(d.disconnected)
	if err := d.MockDeployment.Disconnect(ctx); err != nil {
		return err
	}
	return d.disconnectErr
}

type retirementConnection struct {
	*mnet.Connection
	deployment *retirementDeployment
	command    string
}

func (c *retirementConnection) Write(ctx context.Context, message []byte) error {
	c.command = c.deployment.command.Load().(string)
	err := c.Connection.Write(ctx, message)
	if err == nil {
		switch c.command {
		case "find":
			close(c.deployment.findSent)
		case "killSessions":
			close(c.deployment.cleanupSent)
		}
	}
	return err
}
func (c *retirementConnection) Read(ctx context.Context) ([]byte, error) {
	switch c.command {
	case "find", "killSessions":
		<-ctx.Done()
		return nil, ctx.Err()
	case "endSessions":
		if c.deployment.slowEnd {
			if c.deployment.endReadContext != nil {
				c.deployment.endReadContext <- ctx
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	return c.Connection.Read(ctx)
}

type retirementFixture struct {
	adapter    *Adapter
	deployment *retirementDeployment
	commands   *readSelectionCommands
}

func newRetirementFixture(t *testing.T, slow bool) retirementFixture {
	t.Helper()
	cleanup := bson.D{{Key: "ok", Value: 1}}
	responses := []bson.D{collectionQualificationResponse("db", "records"), cleanup}
	if slow {
		responses = []bson.D{cleanup, cleanup, cleanup, collectionQualificationResponse("db", "records"), cleanup}
	}
	deployment := &retirementDeployment{MockDeployment: drivertest.NewMockDeployment(responses...), findSent: make(chan struct{}), cleanupSent: make(chan struct{}), disconnected: make(chan struct{}), slowEnd: slow}
	commands := &readSelectionCommands{}
	monitor := &event.CommandMonitor{Started: func(ctx context.Context, event *event.CommandStartedEvent) {
		deployment.command.Store(event.CommandName)
		commands.started(ctx, event)
	}}
	opts := options.Client().SetRetryReads(false).SetRetryWrites(false).SetMaxAdaptiveRetries(0).SetMonitor(monitor)
	opts.Deployment = deployment
	client, err := mongo.Connect(opts)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Store: "mongo"}
	adapter := &Adapter{client: client, deployment: deployment, commandMonitor: monitor, config: cfg}
	initializeTestLifetime(adapter)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := adapter.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	if slow {
		sessions := make([]*mongo.Session, 0, 3)
		for range 3 {
			session, err := client.StartSession()
			if err != nil {
				t.Fatal(err)
			}
			sessions = append(sessions, session)
			ctx := mongo.NewSessionContext(t.Context(), session)
			ping := bson.D{{Key: "ping", Value: 1}}
			if err := client.Database("admin").RunCommand(ctx, ping).Err(); err != nil {
				t.Fatal(err)
			}
		}
		for _, session := range sessions {
			session.EndSession(t.Context())
		}
	}
	fixture := retirementFixture{adapter: adapter, deployment: deployment, commands: commands}
	return fixture
}

func TestMongoDefaultRetirementSentUnknownCursor(t *testing.T) {
	for _, slow := range []bool{false, true} {
		t.Run(fmt.Sprintf("blocked_endSessions=%t", slow), func(t *testing.T) {
			fixture := newRetirementFixture(t, slow)
			adapter, deployment := fixture.adapter, fixture.deployment
			limits := store.DefaultLimits()
			runtime, err := store.New(adapter, limits)
			if err != nil {
				t.Fatal(err)
			}
			caller, stop := context.WithCancel(t.Context())
			defer stop()
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
			case <-deployment.findSent:
			case <-time.After(time.Second):
				t.Fatal("find not actually sent")
			}
			if adapter.client.NumberSessionsInProgress() != 1 {
				t.Fatal("missing actual session ownership")
			}
			started := time.Now()
			drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = runtime.Close(drain)
			elapsed := time.Since(started)
			t.Logf("elapsed=%s error=%v sessions=%d", elapsed, err, adapter.client.NumberSessionsInProgress())
			if err != nil || elapsed < 6500*time.Millisecond || elapsed > 7500*time.Millisecond {
				t.Fatal("default retirement failed or drain was shortened", err, elapsed)
			}
			if caller.Err() != nil {
				t.Fatal("shutdown canceled the caller instead of its owned contexts")
			}
			select {
			case <-deployment.cleanupSent:
			default:
				t.Fatal("required unknown-cursor killSessions not invoked")
			}
			select {
			case <-deployment.disconnected:
			default:
				t.Fatal("adapter disconnect worker not joined")
			}
			if adapter.client.NumberSessionsInProgress() != 0 {
				t.Fatal("successful Close left sessions")
			}
			result, err := ticket.Wait(t.Context())
			if err != nil || result.GetReadResult().GetFailure() == nil {
				t.Fatal("retired read lost its failure evidence", result, err)
			}
			names, raw := fixture.commands.snapshot()
			offset := 0
			if slow {
				offset = 3
			}
			tail := names[offset:]
			if len(tail) < 3 || len(tail) > 4 || tail[0] != "listCollections" || tail[1] != "find" || tail[2] != "killSessions" || len(tail) == 4 && tail[3] != "endSessions" {
				t.Fatal("commands replayed or cleanup omitted", names)
			}
			if slow && len(tail) != 4 {
				t.Fatal("idle session endSessions path not exercised", names)
			}
			ids, err := raw[offset+2].Lookup("killSessions").Array().Values()
			if err != nil || len(ids) != 1 || !bytes.Equal(ids[0].Document(), raw[offset+1].Lookup("lsid").Document()) {
				t.Fatal("cleanup used the wrong explicit session identity", err)
			}
		})
	}
}

func TestMongoLifetimePreservesOrdinaryCleanupAndClosesParallelLateCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		adapter := &Adapter{}
		initializeTestLifetime(adapter)
		type contextKey struct{}
		key := contextKey{}
		parent := context.WithValue(t.Context(), key, "owned-value")
		caller, stop := context.WithCancel(parent)
		stop()
		cleanup, release := adapter.cleanupContext(caller)
		defer release()
		deadline, ok := cleanup.Deadline()
		if !ok || deadline.Sub(time.Now()) != 2*time.Second || cleanup.Err() != nil || cleanup.Value(key) != "owned-value" {
			t.Fatal("ordinary cancellation changed detached cleanup", cleanup.Err())
		}
		time.Sleep(time.Second)
		if cleanup.Err() != nil {
			t.Fatal("ordinary cleanup shortened")
		}
		if err := adapter.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if !errors.Is(cleanup.Err(), context.Canceled) {
			t.Fatal("preexisting cleanup missed closed rail", cleanup.Err())
		}
		var workers sync.WaitGroup
		for range 32 {
			workers.Go(func() {
				late, cancel := adapter.cleanupContext(parent)
				defer cancel()
				if !errors.Is(late.Err(), context.Canceled) {
					t.Error("late cleanup restarted a window", late.Err())
				}
			})
		}
		workers.Wait()
		if _, failure := adapter.PrepareNative(parent, nil); failure == nil {
			t.Fatal("closed PrepareNative accepted work")
		}
		if _, failure := adapter.PrepareRecord(parent, nil); failure == nil {
			t.Fatal("closed PrepareRecord accepted work")
		}
	})
}

func TestMongoLifetimeCancelsPreexistingSelectionAndPendingMetadata(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		opts := readSelectionOptions{failAt: 1, entered: entered, selectionError: func(ctx context.Context) error { return ctx.Err() }}
		adapter, deployment, commands := readSelectionAdapter(t, opts)
		target := namespace{database: "db", collection: "records"}
		results := make(chan *pb.Failure, 17)
		go func() { results <- adapter.qualifyTarget(t.Context(), target) }()
		<-entered
		for range 16 {
			go func() { results <- adapter.qualifyTarget(t.Context(), target) }()
		}
		synctest.Wait()
		if err := adapter.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		for range 17 {
			if failure := <-results; failure == nil {
				t.Fatal("closed pending metadata became cached success")
			}
		}
		if failure := adapter.qualifyTarget(t.Context(), target); failure == nil {
			t.Fatal("closed target qualification restarted")
		}
		if deployment.selections.Load() != 1 {
			t.Fatal("shared metadata waiter restarted qualification", deployment.selections.Load())
		}
		names, _ := commands.snapshot()
		if len(names) != 0 {
			t.Fatal("selection waiter sent metadata I/O after close", names)
		}
		adapter.targets.mu.Lock()
		retained := len(adapter.targets.entries)
		adapter.targets.mu.Unlock()
		if retained != 0 {
			t.Fatal("failed metadata cached success", retained)
		}
	})
}

func TestMongoRetirementPreservesDisconnectError(t *testing.T) {
	expected := errors.New("injected genuine retirement failure")
	deployment := &retirementDeployment{MockDeployment: drivertest.NewMockDeployment(), disconnected: make(chan struct{}), disconnectErr: expected}
	opts := options.Client()
	opts.Deployment = deployment
	client, err := mongo.Connect(opts)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &Adapter{client: client}
	initializeTestLifetime(adapter)
	if err := adapter.Close(t.Context()); !errors.Is(err, expected) {
		t.Fatal("genuine close error suppressed", err)
	}
	if err := adapter.Close(t.Context()); !errors.Is(err, expected) {
		t.Fatal("repeat close lost original result", err)
	}
}

func TestMongoLifetimeJoinsActualTopologySelectionWithoutNetwork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// An explicitly empty seed list starts no monitor, dial or DNS worker. It
		// exercises the pinned topology's real subscription-wait selection loop.
		cfg := topology.Config{ReplicaSetName: "offline"}
		deployment, err := topology.New(&cfg)
		if err != nil {
			t.Fatal(err)
		}
		opts := options.Client()
		opts.Deployment = deployment
		client, err := mongo.Connect(opts)
		if err != nil {
			t.Fatal(err)
		}
		adapter := &Adapter{client: client, deployment: deployment}
		initializeTestLifetime(adapter)
		command := bson.D{{Key: "count", Value: "records"}}
		raw, err := bson.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		document := &pb.Document{ContentType: "application/bson", Data: raw}
		request := &pb.NativeRequest{Resource: "db", Request: document}
		operation := &pb.Command_Native{Native: request}
		input := &pb.Command{Operation: operation}
		caller := t.Context()
		work, failure := adapter.PrepareNative(caller, input)
		if failure != nil {
			t.Fatal(failure)
		}
		events := make(chan *pb.Event, 1)
		ends := make(chan *pb.NativeEnd, 1)
		joined := make(chan struct{})
		go func() {
			defer close(joined)
			ends <- adapter.ExecuteNative(caller, work, func(event *pb.Event) error { events <- event; return nil })
		}()
		synctest.Wait()
		select {
		case <-joined:
			t.Fatal("actual selector was not waiting")
		default:
		}
		if err := adapter.Close(caller); err != nil {
			t.Fatal(err)
		}
		<-joined
		terminal := <-ends
		if terminal.GetFailure() == nil || caller.Err() != nil {
			t.Fatal("selection cancellation lost evidence or canceled its caller", terminal)
		}
		if len(events) != 0 {
			t.Fatal("selection waiter emitted output before failing")
		}
	})
}

func TestMongoOrdinaryDetachedCleanupKeepsFullTwoSeconds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		adapter := &Adapter{}
		initializeTestLifetime(adapter)
		caller, cancel := context.WithCancel(t.Context())
		cancel()
		cleanup, release := adapter.cleanupContext(caller)
		defer release()
		time.Sleep(2*time.Second - time.Nanosecond)
		if cleanup.Err() != nil {
			t.Fatal("ordinary detached cleanup expired early", cleanup.Err())
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if !errors.Is(cleanup.Err(), context.DeadlineExceeded) || adapter.lifetime.Err() != nil {
			t.Fatal("ordinary cleanup deadline changed the adapter lifetime", cleanup.Err())
		}
		if err := adapter.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMongoClosedLifetimeNeverConfirmsLateAbort(t *testing.T) {
	cleanup := bson.D{{Key: "ok", Value: 1}}
	responses := []bson.D{cleanup}
	adapter := batchMockAdapter(t, responses, nil)
	session, err := adapter.client.StartSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.EndSession(t.Context())
	if err := session.StartTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if adapter.abortProgramTransaction(session, true) {
		t.Fatal("closed lifetime authorized retry as confirmed abort")
	}
}

func TestMongoLifetimeDoesNotCacheMetadataSuccessAfterClose(t *testing.T) {
	var adapter *Adapter
	monitor := &event.CommandMonitor{Succeeded: func(ctx context.Context, event *event.CommandSucceededEvent) {
		if event.CommandName == "listCollections" {
			if err := adapter.Close(ctx); err != nil {
				t.Error(err)
			}
		}
	}}
	responses := []bson.D{collectionQualificationResponse("db", "records")}
	adapter = batchMockAdapter(t, responses, monitor)
	target := namespace{database: "db", collection: "records"}
	if failure := adapter.qualifyTarget(t.Context(), target); failure == nil {
		t.Fatal("metadata reply racing teardown became successful qualification")
	}
	adapter.targets.mu.Lock()
	retained := len(adapter.targets.entries)
	adapter.targets.mu.Unlock()
	if retained != 0 {
		t.Fatal("successful metadata reply was cached after its rail closed", retained)
	}
}

func TestMongoDirectCloseCapsUndeadlinedCallerAtDefaultCleanup(t *testing.T) {
	assertMongoDirectCloseBound(t, 0)
}

func TestMongoDirectClosePreservesEarlierCallerDeadline(t *testing.T) {
	assertMongoDirectCloseBound(t, 250*time.Millisecond)
}

func assertMongoDirectCloseBound(t *testing.T, parentTimeout time.Duration) {
	t.Helper()
	fixture := newRetirementFixture(t, true)
	adapter, deployment := fixture.adapter, fixture.deployment
	deployment.endReadContext = make(chan context.Context, 1)
	if adapter.options().CleanupTimeout != 2*time.Second || adapter.client.NumberSessionsInProgress() != 0 {
		t.Fatal("direct-close fixture changed default cleanup or left prewarm sessions active")
	}
	type callerKey struct{}
	key := callerKey{}
	parent := context.WithValue(context.Background(), key, "direct-close-caller")
	caller, cancel := context.WithCancel(parent)
	if parentTimeout > 0 {
		cancel()
		caller, cancel = context.WithTimeout(parent, parentTimeout)
	}
	defer cancel()
	started := time.Now()
	result := make(chan error, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		result <- adapter.Close(caller)
	}()
	// On failure only, cancel the parent and reap the fixture before reporting
	// the regression. A passing Close must not need this external intervention.
	defer func() { cancel(); <-joined }()
	guard := 2500 * time.Millisecond
	if parentTimeout > 0 {
		guard = parentTimeout + 500*time.Millisecond
	}
	var closeErr error
	select {
	case closeErr = <-result:
	case <-time.After(guard):
		cancel()
		<-joined
		t.Fatal("direct Close exceeded the supplied/default bound", time.Since(started))
	}
	<-joined
	elapsed := time.Since(started)
	if closeErr != nil {
		t.Fatal("best-effort endSessions deadline manufactured a Disconnect error", closeErr)
	}
	var readContext context.Context
	select {
	case readContext = <-deployment.endReadContext:
	default:
		t.Fatal("direct Close never blocked in actual endSessions Read")
	}
	deadline, ok := readContext.Deadline()
	if !ok || readContext.Value(key) != "direct-close-caller" || !errors.Is(readContext.Err(), context.DeadlineExceeded) {
		t.Fatal("Disconnect did not preserve caller values and bounded deadline", readContext.Err())
	}
	if parentTimeout == 0 {
		if _, ok := caller.Deadline(); ok {
			t.Fatal("fixture parent unexpectedly had a deadline")
		}
		if caller.Err() != nil {
			t.Fatal("Close canceled its caller to stop endSessions", caller.Err())
		}
		if elapsed < 1900*time.Millisecond || elapsed > guard || deadline.Sub(started) < 1900*time.Millisecond || deadline.Sub(started) > 2100*time.Millisecond {
			t.Fatal("direct Close failed to use unchanged default 2s cap", elapsed, deadline.Sub(started))
		}
	} else {
		parentDeadline, ok := caller.Deadline()
		if !ok || !deadline.Equal(parentDeadline) || !errors.Is(caller.Err(), context.DeadlineExceeded) || elapsed < 200*time.Millisecond || elapsed > guard {
			t.Fatal("Close extended or detached the earlier parent deadline", elapsed, deadline, parentDeadline, caller.Err())
		}
	}
	select {
	case <-deployment.disconnected:
	default:
		t.Fatal("actual Disconnect owner did not join")
	}
	if adapter.client.NumberSessionsInProgress() != 0 {
		t.Fatal("direct Close retained a real session owner")
	}
	names, raw := fixture.commands.snapshot()
	if len(names) != 4 || names[0] != "ping" || names[1] != "ping" || names[2] != "ping" || names[3] != "endSessions" {
		t.Fatal("direct Close did not retire genuine idle session identities exactly once", names)
	}
	ids, err := raw[3].Lookup("endSessions").Array().Values()
	if err != nil || len(ids) != 3 {
		t.Fatal("endSessions did not use the three genuinely used idle session IDs", err)
	}
	t.Logf("actual direct Close elapsed=%s error=%v parent_timeout=%s caller_error=%v sessions=%d commands=%v", elapsed, closeErr, parentTimeout, caller.Err(), adapter.client.NumberSessionsInProgress(), names)
}
