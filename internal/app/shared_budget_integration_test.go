//go:build integration

package app

import (
	"context"
	"fmt"
	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir/internal/testutil"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

// This observation is inside the owned wire proxy, not a second scheduler.
// All command transitions for these independent processes share this one lock.
// Held replies have already executed in Mongo; held requests have not.
type budgetObservation struct {
	mu                               sync.Mutex
	active, peak, started, completed int
	gate                             <-chan struct{}
	delay                            time.Duration
}

func (o *budgetObservation) start(ctx context.Context, e *event.CommandStartedEvent) {
	if !budgetCommand(e.CommandName) {
		return
	}
	o.begin(ctx)
}

func (o *budgetObservation) begin(ctx context.Context) {
	o.mu.Lock()
	o.active++
	o.peak = max(o.peak, o.active)
	o.started++
	gate, delay := o.gate, o.delay
	o.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
		}
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
		}
	}
}

func (o *budgetObservation) finish(_ context.Context, e *event.CommandSucceededEvent) {
	if !budgetCommand(e.CommandName) {
		return
	}
	o.end()
}

func (o *budgetObservation) end() {
	o.mu.Lock()
	o.active--
	o.completed++
	o.mu.Unlock()
}

func budgetCommand(name string) bool {
	switch name {
	case "find", "count", "findAndModify", "insert", "update", "delete", "bulkWrite", "getMore", "killCursors", "endSessions", "killSessions":
		return true
	}
	return false
}

func (o *budgetObservation) hold(gate <-chan struct{}, delay time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.gate, o.delay = gate, delay
}

func (o *budgetObservation) snapshot() (active, peak, started, completed int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.active, o.peak, o.started, o.completed
}

type mongoBudgetExecutor struct {
	store       string
	process     *process
	client      pb.StoreServiceClient
	proxy       *testmongo.Proxy
	concurrency int
	root        string
	drop        chan struct{}
	locals      int
}

type mongoBudgetStart struct {
	binary      string
	fixture     *testmongo.SecureFixture
	observation *budgetObservation
	concurrency int
	extra       bool
}

func startMongoBudgetExecutor(t *testing.T, opts mongoBudgetStart) *mongoBudgetExecutor {
	t.Helper()
	binary, fixture, observation, concurrency := opts.binary, opts.fixture, opts.observation, opts.concurrency
	proxy := testmongo.StartProxy(t, &fixture.Fixture)
	proxy.Monitor = &event.CommandMonitor{Started: observation.start, Succeeded: observation.finish}
	drop := make(chan struct{})
	proxy.DropCommand, proxy.DropGate = "bulkWrite", drop
	backend := mongoFixtureConfig(t, proxy.URI())
	local := &Local{MongoDB: backend, MaxConcurrency: concurrency, MaxBatchOperations: 1}
	service := StoreConfig{Name: "records", Local: local}

	cfg := DefaultConfig()
	// The largest backend concurrency is four. Admit queued work as well as
	// active calls so the overload phase exercises both ledgers independently.
	cfg.Basic.Transport.MaxSessions = 8
	cfg.Basic.Listeners.Application, cfg.Basic.Listeners.Peer, cfg.Basic.Diagnostics.Address = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
	cfg.Basic.Discovery.Group = "records"
	cfg.Routing.Stores = []StoreConfig{service}
	if opts.extra {
		second := StoreConfig{Name: "extra", Local: local}

		cfg.Routing.Stores = append(cfg.Routing.Stores, second)

	}
	p := startProcess(t, binary, cfg)
	e := &mongoBudgetExecutor{
		store:       "records",
		process:     p,
		client:      endpointProcessClient(t, p.address),
		proxy:       proxy,
		concurrency: concurrency,
		root:        fixture.DB + "/records",
		drop:        drop,
		locals:      len(cfg.Routing.Stores),
	}
	t.Logf("start time=%s PID=%d C=%d application=%s diagnostics=%s", time.Now().UTC().Format(time.RFC3339Nano), p.command.Process.Pid, concurrency, p.address, p.diagnostic)
	return e
}

func budgetPut(root, id string) *pb.MutateRequest {
	doc := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int64(1)}}
	raw, _ := bson.Marshal(doc)
	document := &pb.Document{ContentType: "application/bson", Data: raw}
	action := &pb.MutateRequest_Put{Put: document}
	request := &pb.MutateRequest{Resource: root + "/s:" + id, Action: action}
	return request
}

func budgetWait(t *testing.T, label string, predicate func() bool) {
	t.Helper()
	until := time.Now().Add(1500 * time.Millisecond)
	for !predicate() {
		if time.Now().After(until) {
			t.Fatal("barrier not reached:", label)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func budgetConcurrency(t *testing.T, p *process) int {
	t.Helper()
	return int(testmetrics.Sum(testmetrics.Scrape(t, p.diagnostic), "weir_store_concurrency_limit"))
}

func budgetFreshRead(t *testing.T, e *mongoBudgetExecutor, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request := &pb.ReadRequest{Resource: e.root + "/s:" + id}
	for attempts := 1; ; attempts++ {
		recordResult, err := testutil.ExecuteRecord(ctx, e.client, testutil.RecordRequest(e.store, request))
		result := recordResult.GetReadResult()
		if err == nil && result.GetFailure() == nil {
			if attempts > 1 {
				t.Logf("fresh independent read probes recovered PID=%d calls=%d", e.process.command.Process.Pid, attempts)
			}
			return
		}
		if ctx.Err() != nil {
			t.Fatal("new Mongo calls did not recover within original 3s budget", result, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestMongoSharedProcessBudget(t *testing.T) {
	if os.Getenv("WEIR_SHARED_BUDGET_INTEGRATION") != "mongo" {
		t.Skip("requires WEIR_SHARED_BUDGET_INTEGRATION=mongo")
	}
	fixture := testmongo.OpenSecure(t)
	binary := buildEndpointProcess(t)
	observation := &budgetObservation{}
	start := mongoBudgetStart{binary: binary, fixture: fixture, observation: observation, concurrency: 1}
	first := startMongoBudgetExecutor(t, start)
	peers := []*mongoBudgetExecutor{first}
	budgetFreshRead(t, first, "initial")
	for _, c := range []int{2, 4} {
		start.concurrency = c
		peers = append(peers, startMongoBudgetExecutor(t, start))
	}
	t.Log("executor sequence 1 -> 3; all share one TLS/SCRAM database; capacities 1+2+4=7")
	// Warm independent calls across all configured execution slots. Write,
	// batch and stream coexistence is exercised below.
	observation.hold(nil, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	var workers sync.WaitGroup
	var reads atomic.Int64
	for _, e := range peers {
		for range 8 {
			workers.Go(func() {
				for ctx.Err() == nil {
					request := &pb.ReadRequest{Resource: e.root + "/s:warm"}
					recordResult2, err := testutil.ExecuteRecord(ctx, e.client, testutil.RecordRequest(e.store, request))
					result := recordResult2.GetReadResult()
					if err == nil && result.GetFailure() == nil {
						reads.Add(1)
					}
				}
			})
		}
	}
	workers.Wait()
	cancel()
	observation.hold(nil, 0)
	budgetWait(t, "warm calls settled", func() bool {
		a, _, _, _ := observation.snapshot()
		return a == 0
	})
	for _, e := range peers {
		if capacity := budgetConcurrency(t, e.process); capacity != e.concurrency {
			t.Fatal("configured concurrency changed", capacity, e.concurrency)
		}
	}
	// A simultaneous wire barrier makes the 7 in-flight exchanges inspectable;
	// this is not the sum of unrelated instantaneous metric samples.
	gate := make(chan struct{})
	observation.hold(gate, 0)
	var held sync.WaitGroup
	for _, e := range peers {
		for i := 0; i < e.concurrency; i++ {
			held.Go(func() { budgetFreshRead(t, e, fmt.Sprintf("held%d", i)) })
		}
	}
	budgetWait(t, "seven real wire requests", func() bool {
		a, _, _, _ := observation.snapshot()
		return a == 7
	})
	total := 0
	for _, e := range peers {
		current, peak := e.proxy.Sockets()
		owned := budgetOwner(t, e.process, e.locals, e.concurrency+1)
		total += owned
		if owned != e.concurrency+1 {
			t.Fatal("Mongo local owner did not cover pool and polling monitor", owned)
		}
		families := testmetrics.Scrape(t, e.process.diagnostic)
		if testmetrics.Sum(families, "weir_store_active_executions") != float64(e.concurrency) || testmetrics.Sum(families, "weir_store_concurrency_limit") != float64(e.concurrency) {
			t.Fatal("runtime assembly cap")
		}
		t.Logf(
			"barrier PID=%d active=C=%d upstream TCP current/peak=%d/%d (observer, independently of local owner)",
			e.process.command.Process.Pid,
			e.concurrency,
			current,
			peak,
		)
	}
	if total != 10 {
		t.Fatal("three process socket budget", total)
	}
	close(gate)
	observation.hold(nil, 0)
	held.Wait()
	t.Logf("stable load: 24 producers, 1.2s, proxy delay=10ms, completed reads=%d; simultaneous in-flight=7, local owned=10", reads.Load())
	data := bson.D{{Key: "failCommands", Value: bson.A{"find"}}, {Key: "errorCode", Value: 16500}}
	testmongo.FailCommand(t, fixture.Admin, data, 1)
	callCtx, stop := context.WithTimeout(context.Background(), time.Second)
	faultRead := &pb.ReadRequest{Resource: peers[2].root + "/s:congestion"}
	faultResult, faultError := testutil.ExecuteRecord(callCtx, peers[2].client, testutil.RecordRequest("records", faultRead))
	stop()
	if faultError != nil || faultResult.GetReadResult().GetFailure() == nil {
		t.Fatal("Mongo congestion lost its read failure", faultError, faultResult)
	}
	if budgetConcurrency(t, peers[2].process) != 4 || budgetConcurrency(t, peers[1].process) != 2 {
		t.Fatal("backend failure changed configured execution limits")
	}
	failureLabels := map[string]string{"operation": "read", "outcome": "failure"}
	failures := testmetrics.Sample(testmetrics.Scrape(t, peers[2].process.diagnostic), "weir_store_records_total", failureLabels)
	if failures.GetCounter().GetValue() < 1 {
		t.Fatal("backend read failure was not counted")
	}
	t.Log("real failCommand 16500: read failure counted; configured C4 and C2 unchanged")
	mongoBudgetOverload(t, peers, observation)
	mongoBudgetMixed(t, peers, fixture, observation)
	discovery := budgetDiscoveryOptions{
		binary:  binary,
		peers:   []*process{peers[0].process, peers[1].process, peers[2].process},
		request: budgetPut(first.root, "discovered"),
	}
	budgetDirectDiscovery(t, discovery)
	start.concurrency = 1
	start.extra = true
	replacement := mongoBudgetReplacement(t, peers, start)
	peers = append(peers, replacement)
	for _, e := range peers {
		e.process.stop(t)
	}
	budgetWait(t, "all four observers upstream sockets closed", func() bool {
		for _, e := range peers {
			n, _ := e.proxy.Sockets()
			if n != 0 {
				return false
			}
		}
		return true
	})
	for _, e := range peers {
		current, peak := e.proxy.Sockets()
		t.Logf("final PID=%d C=%d upstream current=%d event high-water=%d", e.process.command.Process.Pid, e.concurrency, current, peak)
		budgetClosedOwner(t, e.process, e.locals, e.concurrency+1)
	}
	_, peak, started, completed := observation.snapshot()
	if started != completed {
		t.Fatal("unfinished proxy commands", started, completed)
	}
	t.Logf("final wire observations peak=%d started=%d replied=%d; executor sockets=0; all owned Weir processes Wait completed", peak, started, completed)
}

func mongoBudgetOverload(t *testing.T, peers []*mongoBudgetExecutor, o *budgetObservation) {
	t.Helper()
	expected := 0
	for _, e := range peers {
		expected += budgetConcurrency(t, e.process)
	}
	gate := make(chan struct{})
	o.hold(gate, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 450*time.Millisecond)
	var workers sync.WaitGroup
	var rejected, completed atomic.Int64
	for _, e := range peers {
		for worker := 0; worker < 24; worker++ {
			workers.Go(func() {
				for n := 0; ctx.Err() == nil; n++ {
					request := budgetPut(e.root, fmt.Sprintf("overload%d-%d", worker, n))
					success := budgetLoadCall(ctx, e.client, testutil.RecordRequest(e.store, request), worker%3)
					if !success {
						rejected.Add(1)
					} else {
						completed.Add(1)
					}
				}
			})
		}
	}
	budgetWait(t, "overload active", func() bool {
		a, _, _, _ := o.snapshot()
		return a == expected
	})
	for _, e := range peers {
		f := testmetrics.Scrape(t, e.process.diagnostic)
		if testmetrics.Sum(f, "weir_store_pending_entries") < 1 || testmetrics.Sum(f, "weir_store_active_executions") > float64(e.concurrency) {
			t.Fatal("no queue or exceeded cap")
		}
	}
	// Release before the 2s physical timeout; caller cancellation is separately
	// exercised by the remaining producer calls and replacement below.
	time.Sleep(100 * time.Millisecond)
	close(gate)
	o.hold(nil, 0)
	workers.Wait()
	cancel()
	if rejected.Load() == 0 || completed.Load() == 0 {
		t.Fatal("overload did not reject/recover", rejected.Load(), completed.Load())
	}
	for _, e := range peers {
		budgetFreshRead(t, e, "recovered")
	}
	t.Logf(
		"bounded overload: 72 continuous Read/Mutate/Bulk producers/450ms, blocked wire for 100ms, rejected/non-OK=%d successful=%d; fresh calls recovered",
		rejected.Load(),
		completed.Load(),
	)
}

func mongoBudgetMixed(t *testing.T, peers []*mongoBudgetExecutor, fixture *testmongo.SecureFixture, observation *budgetObservation) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var workers sync.WaitGroup
	// Three independent runtimes and a direct native writer compete on one key.
	initial := budgetPut(peers[0].root, "counter")
	recordResult3, err := testutil.ExecuteRecord(ctx, peers[0].client, testutil.RecordRequest("records", initial))
	result := recordResult3.GetMutationResult()
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal(result, err)
	}
	for _, e := range peers {
		workers.Go(func() {
			for range 12 {
				increment := bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int64(1)}}}}
				raw, _ := bson.Marshal(increment)
				document := &pb.Document{ContentType: mongodb.ExpressionContentType, Data: raw}
				form := &pb.Transform_BackendExpression{BackendExpression: document}
				transform := &pb.Transform{Form: form}
				action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
				request := &pb.MutateRequest{Resource: e.root + "/s:counter", Action: action}
				recordResult4, err := testutil.ExecuteRecord(ctx, e.client, testutil.RecordRequest(e.store, request))
				result := recordResult4.GetMutationResult()
				if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
					t.Error("atomic increment", result, err)
					return
				}
			}
		})
	}
	workers.Go(func() {
		collection := fixture.Client.Database(fixture.DB).Collection("records")
		filter := bson.D{{Key: "_id", Value: "counter"}}
		update := bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int64(1)}}}}
		for range 12 {
			if _, err := collection.UpdateOne(ctx, filter, update); err != nil {
				t.Error("native writer", err)
				return
			}
		}
	})
	workers.Wait()
	filter := bson.D{{Key: "_id", Value: "counter"}}
	var observed struct {
		N int64 `bson:"n"`
	}
	if err := fixture.Client.Database(fixture.DB).Collection("records").FindOne(ctx, filter).Decode(&observed); err != nil || observed.N != 49 {
		t.Fatal("lost cross-process/native increment", observed.N, err)
	}
	// Each mutation batch shares one streaming RPC; input-order results are all checked.
	for _, e := range peers {
		requests := make([]*pb.MutateRequest, 0, 4)
		for i := range 4 {
			request := budgetPut(e.root, fmt.Sprintf("bulk%d-%d", e.concurrency, i))
			requests = append(requests, request)
		}
		reply, err := testutil.MutateRecords(ctx, e.client, "records", requests)
		if err != nil || len(reply) != len(requests) {
			t.Fatal("batch mutation", reply, err)
		}
		for i, result := range reply {
			if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
				t.Fatal("batch mutation", i, result)
			}
		}
	}
	// Inspect real admission while the owned wire proxy holds Native and Scan.
	// Then overlap both streams with ordinary operations across three processes.
	gate := make(chan struct{})
	observation.hold(gate, 0)
	var release sync.Once
	defer release.Do(func() { close(gate) })
	workers.Go(func() { mongoBudgetNative(t, peers[0]) })
	workers.Go(func() {
		request := &pb.ScanRequest{Resource: peers[1].root}
		scanVariant := &pb.Command_Scan{Scan: request}
		scanCall := &pb.Command{Operation: scanVariant}
		scan, err := testutil.ExecuteEvents(ctx, peers[1].client, peers[1].store, scanCall)
		if err != nil {
			t.Error(err)
			return
		}
		for {
			frame, err := scan.Recv()
			if err != nil {
				t.Error(err)
				return
			}
			if end := frame.GetScanEnd(); end != nil {
				if end.Failure != nil {
					t.Error(end)
				}
				break
			}
		}
		if _, err := scan.Recv(); err != io.EOF {
			t.Error("Scan EOF", err)
		}
	})
	budgetWait(t, "Native and Scan occupy runtime reservations", func() bool {
		return budgetRuntimeOccupied(t, peers[0].process) && budgetRuntimeOccupied(t, peers[1].process)
	})
	release.Do(func() { close(gate) })
	observation.hold(nil, 0)
	for _, e := range peers {
		workers.Go(func() {
			for range 8 {
				budgetFreshRead(t, e, "counter")
			}
		})
	}
	workers.Wait()
	if t.Failed() {
		t.Fatal("mixed operations failed")
	}
	budgetWait(t, "completed Native and Scan reservations released", func() bool {
		return budgetRuntimeReleased(t, peers[0].process) && budgetRuntimeReleased(t, peers[1].process)
	})
	// Mongo's Scan uses singleBatch reads and owns no persistent database cursor.
	// Hold the first read to prove ownership, then cancel while further reads
	// are delayed and verify the actual pending/execution/result ledgers clear.
	cancelGate := make(chan struct{})
	observation.hold(cancelGate, 0)
	var releaseCancel sync.Once
	defer releaseCancel.Do(func() { close(cancelGate) })
	scanCtx, stopScan := context.WithCancel(ctx)
	defer stopScan()
	request := &pb.ScanRequest{Resource: peers[1].root}
	scanVariant := &pb.Command_Scan{Scan: request}
	scanCall := &pb.Command{Operation: scanVariant}
	scan, err := testutil.ExecuteEvents(scanCtx, peers[1].client, peers[1].store, scanCall)
	if err != nil {
		t.Fatal(err)
	}
	budgetWait(t, "cancelled Scan first read holds reservations", func() bool {
		return budgetRuntimeOccupied(t, peers[1].process)
	})
	observation.hold(nil, 50*time.Millisecond)
	releaseCancel.Do(func() { close(cancelGate) })
	if frame, err := scan.Recv(); err != nil || frame.GetDocument() == nil {
		t.Fatal("cancel Scan first page", err)
	}
	if !budgetRuntimeOccupied(t, peers[1].process) {
		t.Fatal("Scan completed before cancellation evidence")
	}
	stopScan()
	observation.hold(nil, 0)
	if _, err := scan.Recv(); err == nil || err == io.EOF {
		t.Fatal("cancelled Scan unexpectedly completed", err)
	}
	budgetWait(t, "cancelled Scan reservations released", func() bool {
		return budgetRuntimeReleased(t, peers[1].process)
	})
	t.Log("Read/Mutate/Bulk/Native/Scan completed; three executor increments 36 + independent native writer 12 + seed 1 = 49; database atomicity, no global Weir ordering claim")
}

func mongoBudgetNative(t *testing.T, e *mongoBudgetExecutor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	filter := bson.D{{Key: "_id", Value: "counter"}}
	command := bson.D{{Key: "count", Value: "records"}, {Key: "query", Value: filter}, {Key: "limit", Value: 1}}
	raw, _ := bson.Marshal(command)
	request := &pb.Document{ContentType: "application/bson", Data: raw}
	nativeCall := &pb.NativeRequest{Resource: e.root, Request: request}
	nativeVariant := &pb.Command_Native{Native: nativeCall}
	call := &pb.Command{Operation: nativeVariant}
	stream, err := testutil.ExecuteEvents(ctx, e.client, e.store, call)
	if err != nil {
		t.Fatal(err)
	}
	for {
		reply, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if end := reply.GetNativeEnd(); end != nil {
			if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE {
				t.Fatal(end)
			}
			break
		}
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatal(err)
	}
}

func mongoBudgetReplacement(t *testing.T, peers []*mongoBudgetExecutor, opts mongoBudgetStart) *mongoBudgetExecutor {
	t.Helper()
	old := peers[0]
	fixture := opts.fixture
	replacement := startMongoBudgetExecutor(t, opts)
	t.Logf(
		"executor sequence 3 -> 4 time=%s; old PID=%d new PID=%d both live",
		time.Now().UTC().Format(time.RFC3339Nano),
		old.process.command.Process.Pid,
		replacement.process.command.Process.Pid,
	)
	if testmetrics.Sum(testmetrics.Scrape(t, replacement.process.diagnostic), "weir_store_concurrency_limit") != 2 {
		t.Fatal("two Local budgets merged")
	}
	budgetWait(t, "two independent Mongo pools", func() bool {
		n, _ := replacement.proxy.Sockets()
		return n == 4
	})
	oldUpdates := 0
	for _, e := range old.proxy.Events() {
		if e.Command == "bulkWrite" {
			oldUpdates++
		}
	}
	targets := make([]budgetReadTarget, 0, 5)
	for _, e := range append(peers, replacement) {
		target := budgetReadTarget{
			process: e.process,
			client:  e.client,
			root:    e.root,
			store:   e.store,
			locals:  e.locals,
			limit:   e.concurrency + 1,
		}
		targets = append(targets, target)
	}
	extra := budgetReadTarget{
		process: replacement.process,
		client:  replacement.client,
		root:    replacement.root,
		store:   "extra",
		locals:  2,
		limit:   2,
	}
	targets = append(targets, extra)
	budgetOverlap(t, targets, opts.observation)
	budgetReplacementReads(t, targets[3:])
	mongoBudgetNative(t, replacement)
	extraExecutor := *replacement
	extraExecutor.store = extra.store
	mongoBudgetNative(t, &extraExecutor)
	drop := old.drop
	old.proxy.DropRemaining.Store(1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resultChannel := make(chan *pb.MutationResult, 1)
	oldRequest := budgetPut(old.root, "lost-reply")
	go func() {
		recordResult5, err := testutil.ExecuteRecord(ctx, old.client, testutil.RecordRequest(old.store, oldRequest))
		result := recordResult5.GetMutationResult()
		if err != nil {
			t.Error("expected conservative terminal response", err)
		}
		resultChannel <- result
	}()
	budgetWait(t, "backend success before reply loss", func() bool {
		for _, event := range old.proxy.Events() {
			if event.Dropped && event.Acknowledged {
				return true
			}
		}
		return false
	})
	queuedCtx, queuedCancel := context.WithCancel(ctx)
	queuedDone := make(chan struct{})
	go func() {
		defer close(queuedDone)
		request := budgetPut(old.root, "queued-cancel")
		_, _ = testutil.ExecuteRecord(queuedCtx, old.client, testutil.RecordRequest(old.store, request))
	}()
	budgetWait(t, "old queued plus executed write", func() bool {
		return testmetrics.Sum(testmetrics.Scrape(t, old.process.diagnostic), "weir_store_pending_entries") == 1
	})
	queuedCancel()
	<-queuedDone
	budgetWait(t, "queued cancellation settled", func() bool {
		return testmetrics.Sum(testmetrics.Scrape(t, old.process.diagnostic), "weir_store_pending_entries") == 0
	})
	if err := old.process.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	close(drop)
	result := <-resultChannel
	if result.GetOutcome() != pb.MutationOutcome_UNKNOWN {
		t.Fatal("confirmed write with lost response must be UNKNOWN", result)
	}
	old.process.stop(t)
	budgetWait(t, "old observer upstream tail closed", func() bool {
		n, _ := old.proxy.Sockets()
		return n == 0
	})
	request := budgetPut(replacement.root, "new-independent")
	recordResult6, err := testutil.ExecuteRecord(ctx, replacement.client, testutil.RecordRequest(replacement.store, request))
	result = recordResult6.GetMutationResult()
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("replacement new mutation", result, err)
	}
	request = budgetPut(extra.root, "new-independent-extra")
	recordResult7, err := testutil.ExecuteRecord(ctx, replacement.client, testutil.RecordRequest(extra.store, request))
	result = recordResult7.GetMutationResult()
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("extra Local new mutation", result, err)
	}
	for _, id := range []string{"lost-reply", "queued-cancel", "new-independent", "new-independent-extra"} {
		filter := bson.D{{Key: "_id", Value: id}}
		n, err := fixture.Client.Database(fixture.DB).Collection("records").CountDocuments(ctx, filter)
		want := int64(1)
		if id == "queued-cancel" {
			want = 0
		}
		if err != nil || n != want {
			t.Fatal("replacement readback", id, n, err)
		}
	}
	dropped, updates := 0, 0
	for _, event := range old.proxy.Events() {
		if event.Command == "bulkWrite" {
			updates++
		}
		if event.Dropped {
			dropped++
		}
	}
	newUpdates := 0
	for _, e := range replacement.proxy.Events() {
		if e.Command == "bulkWrite" {
			newUpdates++
		}
	}
	if dropped != 1 || updates != oldUpdates+1 || newUpdates != 2 {
		t.Fatal("unexpected dropped execution count", dropped)
	}
	t.Logf(
		"executor sequence 4 -> 3 time=%s; old PID=%d Wait exited, upstream=0; acknowledged/drop=1, client UNKNOWN; queued cancel had no effect; new PID=%d handles two new independent mutations across both Local stores",
		time.Now().UTC().Format(time.RFC3339Nano),
		old.process.command.Process.Pid,
		replacement.process.command.Process.Pid,
	)
	return replacement
}

type budgetDiscoveryOptions struct {
	binary  string
	peers   []*process
	request *pb.MutateRequest
}

func budgetDirectDiscovery(t *testing.T, opts budgetDiscoveryOptions) {
	t.Helper()
	before := make([]float64, len(opts.peers))
	cfg := emptyConfig(t)
	cfg.Basic.Listeners.Peer, cfg.Basic.Diagnostics.Address = "127.0.0.1:0", "127.0.0.1:0"
	for i, p := range opts.peers {
		cfg.Basic.Discovery.Seeds = append(cfg.Basic.Discovery.Seeds, p.addresses[1])
		before[i] = testmetrics.Sum(testmetrics.Scrape(t, p.diagnostic), "weir_store_records_total")
	}
	seed := startProcess(t, opts.binary, cfg)
	defer seed.stop(t)
	client := openDiscoveredClient(t, seed.address, []string{"records"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	requests := make([]*weirclient.MutateRequest, 0, 4)
	for range 4 {
		request := &weirclient.MutateRequest{
			Resource: opts.request.Resource,
			Action:   weirclient.MutationPut,
			Document: opts.request.GetPut(),
		}
		requests = append(requests, request)
	}
	options := weirclient.MutateOptions{StoreName: "records", Requests: requests}
	results, err := client.Mutate(ctx, options)
	if err != nil || len(results) != len(requests) {
		t.Fatal("direct pinned batch", results, err)
	}
	for i, result := range results {
		if result == nil || result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
			t.Fatal("direct ordered batch", i, result)
		}
	}
	metrics := testmetrics.Scrape(t, seed.diagnostic)
	if metrics["weir_backend_connections_limit"] != nil || metrics["weir_store_concurrency_limit"] != nil || metrics["weir_store_executions_total"] != nil {
		t.Fatal("initialization node owns execution or forwarding")
	}
	targets := 0
	for i, p := range opts.peers {
		delta := testmetrics.Sum(testmetrics.Scrape(t, p.diagnostic), "weir_store_records_total") - before[i]
		if delta == 4 {
			targets++
		} else if delta != 0 {
			t.Fatal("business batch migrated or replayed", delta)
		}
	}
	if targets != 1 {
		t.Fatal("business batch did not remain on one executor", targets)
	}
	t.Logf("initialization-only PID=%d, direct ordered batch/4 writes; no relay or local pool", seed.command.Process.Pid)
}

// The same finite overload shape drives both real adapter profiles. Every batch
// result is checked; errors are counted and mutations are never replayed.
func budgetLoadCall(ctx context.Context, client pb.StoreServiceClient, fixture testutil.RecordFixture, mode int) bool {
	request := fixture.Command.GetMutate()
	switch mode {
	case 0:
		response, err := testutil.ExecuteRecord(ctx, client, fixture)
		if err != nil || response == nil {
			return false
		}
		result := response.GetMutationResult()
		return result.GetFailure() == nil && result.GetOutcome() == pb.MutationOutcome_APPLIED
	case 1:
		read := &pb.ReadRequest{Resource: request.Resource}
		response, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest(fixture.StoreName, read))
		if err != nil || response == nil {
			return false
		}
		result := response.GetReadResult()
		return result.GetFailure() == nil
	default:
		batch := []*pb.MutateRequest{fixture.Command.GetMutate(), fixture.Command.GetMutate()}
		response, err := testutil.MutateRecords(ctx, client, fixture.StoreName, batch)
		if err != nil || len(response) != len(batch) {
			return false
		}
		for _, result := range response {
			if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
				return false
			}
		}
		return true
	}
}

func budgetRuntimeGauges(t *testing.T, p *process) map[string]float64 {
	t.Helper()
	families := testmetrics.Scrape(t, p.diagnostic)
	values := make(map[string]float64)
	for _, name := range []string{"pending_entries", "result_reserved_entries", "result_reserved_bytes", "active_executions", "working_reserved_bytes", "publishers"} {
		metric := "weir_store_" + name
		if families[metric] == nil {
			t.Fatal("required runtime resource observation missing", metric)
		}
		values[name] = testmetrics.Sum(families, metric)
	}
	return values
}

func budgetRuntimeOccupied(t *testing.T, p *process) bool {
	t.Helper()
	values := budgetRuntimeGauges(t, p)
	return values["result_reserved_entries"] > 0 && values["result_reserved_bytes"] > 0 && values["active_executions"] > 0 && values["working_reserved_bytes"] > 0
}

func budgetRuntimeReleased(t *testing.T, p *process) bool {
	t.Helper()
	for _, value := range budgetRuntimeGauges(t, p) {
		if value != 0 {
			return false
		}
	}
	return true
}
