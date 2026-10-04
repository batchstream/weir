//go:build integration

package mongodb

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func TestMongoOwnerRemoteTail(t *testing.T) {
	f := testmongo.OpenSecure(t)
	proxy := testmongo.StartProxy(t, &f.Fixture)
	entered, gate := make(chan struct{}), make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(gate) })
	var held atomic.Bool
	proxy.Monitor = &event.CommandMonitor{Started: func(ctx context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "bulkWrite" && held.CompareAndSwap(false, true) {
			close(entered)
			select {
			case <-gate:
			case <-ctx.Done():
			}
		}
	}}
	cfg := Config{URI: proxy.URI(), Store: "mongo", Pool: 1}
	cfg = mongoFixtureConfig(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	old := ownerMutation(t, a, f.DB, "remote-tail")
	next := ownerMutation(t, a, f.DB, "independent")
	callCtx, stop := context.WithCancel(ctx)
	result := make(chan []*execution.Result, 1)
	go func() { r, _ := a.executeRecords(callCtx, []*execution.Plan{old}); result <- r }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("request not received")
	}
	t.Logf("%s command=bulkWrite ID=remote-tail observer received; real DB write still held", time.Now().UTC().Format(time.RFC3339Nano))
	stop()
	r := <-result
	if r[0].Mutation.GetOutcome() != pb.MutationOutcome_UNKNOWN {
		t.Fatal("cancellation lost uncertainty", r)
	}
	// A lost bulkWrite reply also leaves its cursor unknown. Session cleanup
	// opens a replacement connection after cancellation closes the original raw
	// socket; the replacement must stay within the same ownership budget.
	t.Logf("cancelled operation and cursor cleanup: %+v", a.dialer.snapshot())
	ownerWait(t, func() bool {
		snapshot := a.dialer.snapshot()
		return snapshot.Owned == 2 && snapshot.Acquired == 3 && snapshot.Released == 1
	})
	current, peak := proxy.Sockets()
	if current != 3 || peak != 3 {
		t.Fatal("observer did not retain cancelled request", current)
	}
	cleanups := 0
	for _, observed := range proxy.Events() {
		if observed.Command == "killSessions" && observed.Acknowledged {
			cleanups++
		}
	}
	if cleanups != 1 {
		t.Fatal("lost reply session was not cleaned exactly once", cleanups)
	}
	t.Logf("%s driver operation returned UNKNOWN, local raw closed: owner=%+v proxy=%d/%d", time.Now().UTC().Format(time.RFC3339Nano), a.dialer.snapshot(), current, peak)
	r, _ = a.executeRecords(ctx, []*execution.Plan{next})
	if r[0].Mutation.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("new independent mutation failed", r)
	}
	current, peak = proxy.Sockets()
	if current != 3 || peak != 3 || a.dialer.snapshot().Owned != 2 || a.dialer.snapshot().Peak != 2 {
		t.Fatal("local and observer layers not separated", current, peak, a.dialer.snapshot())
	}
	t.Logf("%s independent mutation ACK: local owned/peak=2/2, observer upstream=3/3; cancelled raw not charged as alive", time.Now().UTC().Format(time.RFC3339Nano))
	tailStart := time.Now()
	release.Do(func() { close(gate) })
	ownerWait(t, func() bool { n, _ := proxy.Sockets(); return n == 2 })
	updates := 0
	for _, e := range proxy.Events() {
		if e.Command == "bulkWrite" && e.Acknowledged {
			updates++
		}
	}
	if updates != 2 {
		t.Fatal("mutation replay or missing remote tail ACK", updates)
	}
	for _, id := range []string{"remote-tail", "independent"} {
		filter := bson.D{{Key: "_id", Value: id}}
		count, err := f.Client.Database(f.DB).Collection("records").CountDocuments(ctx, filter)
		if err != nil || count != 1 {
			t.Fatal("real database readback", id, count, err)
		}
	}
	t.Logf("%s real DB ACK/readback after cancellation; observer tail retired in %s after gate release; bulkWrite commands=2, each distinct ID once", time.Now().UTC().Format(time.RFC3339Nano), time.Since(tailStart))
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	ownerZero(t, a.dialer)
	ownerWait(t, func() bool { n, _ := proxy.Sockets(); return n == 0 })
}

func ownerMutation(t *testing.T, a *Adapter, database, id string) *execution.Plan {
	t.Helper()
	doc := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: 1}}
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	body := &pb.Document{MediaType: "application/bson", Data: raw}
	put := &pb.MutateRequest_Put{Put: body}
	req := &pb.MutateRequest{Resource: database + "/records/s:" + id, Action: put}
	op := &execution.Operation{Mutate: req}
	plan, failure := prepareTestRecord(a, op)
	if failure != nil {
		t.Fatal(failure)
	}
	return plan
}

type retirementEvent struct {
	at time.Time
	id int64
}
type retirementConn struct {
	net.Conn
	entered chan<- time.Time
	release <-chan struct{}
}

func (c *retirementConn) Close() error {
	c.entered <- time.Now()
	<-c.release
	return c.Conn.Close()
}

// A fixed-driver probe, separate from the production Open qualification below:
// expiring an idle pool member forces removeConnection -> async raw Close -> dial.
func TestMongoDriverRetirementOwnership(t *testing.T) {
	f := testmongo.OpenSecure(t)
	d := newBoundedDialer(2, 3)
	cfg := Config{URI: f.URI, Store: "mongo", Pool: 1}
	cfg = mongoFixtureConfig(t, cfg)
	if err := ValidateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	opts, err := connectionOptions(cfg, d)
	if err != nil {
		t.Fatal(err)
	}
	retired := make(chan retirementEvent, 8)
	var overflow atomic.Bool
	monitor := &event.PoolMonitor{Event: func(e *event.PoolEvent) {
		if e.Type == event.ConnectionClosed {
			observed := retirementEvent{at: time.Now(), id: e.ConnectionID}
			select {
			case retired <- observed:
			default:
				overflow.Store(true)
			}
		}
	}}
	opts.SetDirect(true).SetMaxPoolSize(1).SetMaxConnecting(2).SetMinPoolSize(0).SetMaxConnIdleTime(20 * time.Millisecond).SetServerMonitoringMode(options.ServerMonitoringModePoll).SetConnectTimeout(mongoConnectTimeout).SetServerSelectionTimeout(mongoConnectTimeout).SetPoolMonitor(monitor).SetRetryReads(false).SetRetryWrites(false)
	client, err := mongo.Connect(opts)
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	var release sync.Once
	defer func() {
		release.Do(func() { close(gate) })
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = client.Disconnect(ctx)
		d.close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		t.Fatal(err)
	}
	entered := make(chan time.Time, 2)
	d.mu.Lock()
	for c := range d.conns {
		wrapped := &retirementConn{Conn: c.raw, entered: entered, release: gate}
		c.raw = wrapped
	}
	d.mu.Unlock()
	time.Sleep(30 * time.Millisecond)
	result := make(chan error, 1)
	go func() { result <- client.Ping(ctx, readpref.Primary()) }()
	var removed retirementEvent
	select {
	case removed = <-retired:
	case <-ctx.Done():
		t.Fatal("driver did not retire idle pool member")
	}
	var closing time.Time
	select {
	case closing = <-entered:
	case <-ctx.Done():
		t.Fatal("raw Close not entered")
	}
	ownerWait(t, func() bool { return d.snapshot().Dialing == 1 })
	s := d.snapshot()
	if s.Owned != 2 || s.Closing != 1 || s.Acquired != 2 || s.Peak != 2 || closing.Before(removed.at) {
		t.Fatal("driver retirement order/bound", s)
	}
	t.Logf("pool removed ID=%d at=%s; raw Close entered=%s; replacement dial waiting, owner=%+v", removed.id, removed.at.UTC().Format(time.RFC3339Nano), closing.UTC().Format(time.RFC3339Nano), s)
	release.Do(func() { close(gate) })
	if err := <-result; err != nil {
		t.Fatal("replacement failed after raw Close", err)
	}
	if d.snapshot().Peak != 2 || d.snapshot().Acquired != 3 {
		t.Fatal("replacement ownership", d.snapshot())
	}
	d.stop()
	if err := client.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}
	d.close()
	ownerZero(t, d)
	if overflow.Load() {
		t.Fatal("driver event observation overflow")
	}
	t.Logf("%s replacement completed, final %+v", time.Now().UTC().Format(time.RFC3339Nano), d.snapshot())
}
