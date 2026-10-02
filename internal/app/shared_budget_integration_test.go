//go:build integration

package app

import (
	"context"
	"fmt"
	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir/internal/testutil"
	"io"
	"os"
	"strings"
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
	cfg.Basic.Listeners.Application, cfg.Basic.Listeners.Peer, cfg.Basic.Diagnostics.Address = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
	cfg.Basic.Discovery.Group = "records"
	cfg.Routing.Stores = []StoreConfig{service}
	if opts.extra {
		second := StoreConfig{Name: "extra", Local: local}

		cfg.Routing.Stores = append(cfg.Routing.Stores, second)

	}
	p := startProcess(t, binary, cfg)
	e := &mongoBudgetExecutor{
		process:     p,
		client:      endpointProcessClient(t, p.address),
		proxy:       proxy,
		concurrency: concurrency,
		root:        "weir://records/" + fixture.DB + "/records",
		drop:        drop,
		locals:      len(cfg.Routing.Stores),
	}
	t.Logf("start time=%s PID=%d C=%d application=%s diagnostics=%s", time.Now().UTC().Format(time.RFC3339Nano), p.command.Process.Pid, concurrency, p.address, p.diagnostic)
	return e
}

func budgetPut(root, id string) *pb.MutateRequest {
	doc := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int64(1)}}
	raw, _ := bson.Marshal(doc)
	document := &pb.Document{MediaType: "application/bson", Data: raw}
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

func budgetWindow(t *testing.T, p *process) int {
	t.Helper()
	return int(testmetrics.Sum(testmetrics.Scrape(t, p.diagnostic), "weir_store_window"))
}

func budgetFreshRead(t *testing.T, e *mongoBudgetExecutor, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request := &pb.ReadRequest{Resource: e.root + "/s:" + id}
	for attempts := 1; ; attempts++ {
		routedResult180, err := weirclient.Record(ctx, e.client, testutil.RecordCall(request))
		result := routedResult180.GetRead()
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
	if os.Getenv("WEIR_M12_INTEGRATION") != "mongo" {
		t.Skip("requires WEIR_M12_INTEGRATION=mongo")
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
	// Keep actual demand eligible across AIMD's 250ms growth interval. Each read
	// remains independent; write/Bulk/stream coexistence is exercised below.
	observation.hold(nil, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	var workers sync.WaitGroup
	var reads atomic.Int64
	for _, e := range peers {
		for range 8 {
			workers.Go(func() {
				for ctx.Err() == nil {
					request := &pb.ReadRequest{Resource: e.root + "/s:warm"}
					routedResult221, err := weirclient.Record(ctx, e.client, testutil.RecordCall(request))
					result := routedResult221.GetRead()
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
		if window := budgetWindow(t, e.process); window != e.concurrency {
			t.Fatal("AIMD failed to reach configured C", window, e.concurrency)
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
		if testmetrics.Sum(families, "weir_store_active_executions") != float64(e.concurrency) || testmetrics.Sum(families, "weir_store_window_limit") != float64(e.concurrency) {
			t.Fatal("runtime assembly cap")
		}
		t.Logf(
			"barrier PID=%d active=window=C=%d upstream TCP current/peak=%d/%d (observer, independently of local owner)",
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
	_, _ = weirclient.Record(callCtx, peers[2].client, testutil.RecordCall(faultRead))
	stop()
	if budgetWindow(t, peers[2].process) != 2 || budgetWindow(t, peers[1].process) != 2 {
		t.Fatal("Mongo AIMD instances not independent")
	}
	t.Log("real failCommand 16500: C4 window 4->2, C2 stays 2; independent feedback")
	mongoBudgetOverload(t, peers, observation)
	mongoBudgetMixed(t, peers, fixture)
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
		expected += budgetWindow(t, e.process)
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
					success := budgetLoadCall(ctx, e.client, request, worker%3)
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

func mongoBudgetMixed(t *testing.T, peers []*mongoBudgetExecutor, fixture *testmongo.SecureFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var workers sync.WaitGroup
	// Three independent runtimes and a direct native writer compete on one key.
	initial := budgetPut(peers[0].root, "counter")
	routedResult391, err := weirclient.Record(ctx, peers[0].client, testutil.RecordCall(initial))
	result := routedResult391.GetMutation()
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal(result, err)
	}
	for _, e := range peers {
		workers.Go(func() {
			for range 12 {
				increment := bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int64(1)}}}}
				raw, _ := bson.Marshal(increment)
				document := &pb.Document{MediaType: mongodb.ExpressionMedia, Data: raw}
				form := &pb.Transform_BackendExpression{BackendExpression: document}
				transform := &pb.Transform{Form: form}
				action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
				request := &pb.MutateRequest{Resource: e.root + "/s:counter", Action: action}
				routedResult405, err := weirclient.Record(ctx, e.client, testutil.RecordCall(request))
				result := routedResult405.GetMutation()
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
	// A live Bulk uses the production framing/ledger; every result is consumed.
	for _, e := range peers {
		stream := testutil.OpenEvents(ctx, e.client, "records")
		for i := 0; i < 4; i++ {
			request := budgetPut(e.root, fmt.Sprintf("bulk%d-%d", e.concurrency, i))
			mutation := &pb.Operation_Mutate{Mutate: request}
			op := &pb.Operation{Index: uint64(i), Operation: mutation}
			_, item := testutil.OperationCall(op)
			frame := item
			if err := stream.Send(frame); err != nil {
				t.Fatal(err)
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatal(err)
		}
		for range 4 {
			reply, err := stream.Recv()
			if err != nil || reply.GetResult().GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal("Bulk", reply, err)
			}
		}
		if _, err := stream.Recv(); err != io.EOF {
			t.Fatal("Bulk EOF", err)
		}
	}
	// Native and Scan share one live-session slot within each Store, so overlap
	// them across executors while ordinary operations continue on all three.
	workers.Go(func() { mongoBudgetNative(t, peers[0]) })
	workers.Go(func() {
		request := &pb.ScanRequest{Resource: peers[1].root}
		scanVariant := &pb.Call_Scan{Scan: request}
		scanCall := &pb.Call{Version: 1, Operation: scanVariant}
		scan, err := testutil.OneEvents(ctx, peers[1].client, scanCall)
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
	budgetWait(t, "Scan session released", func() bool {
		return testmetrics.Sum(testmetrics.Scrape(t, peers[1].process.diagnostic), "weir_store_live_sessions") == 0
	})
	beforeCleanup := 0
	for _, event := range peers[1].proxy.Events() {
		if event.Command == "killCursors" {
			beforeCleanup++
		}
	}
	scanCtx, stopScan := context.WithCancel(ctx)
	request := &pb.ScanRequest{Resource: peers[1].root}
	scanVariant := &pb.Call_Scan{Scan: request}
	scanCall := &pb.Call{Version: 1, Operation: scanVariant}
	scan, err := testutil.OneEvents(scanCtx, peers[1].client, scanCall)
	if err != nil {
		t.Fatal(err)
	}
	if frame, err := scan.Recv(); err != nil || frame.GetDocument() == nil {
		t.Fatal("cancel Scan first page", err)
	}
	stopScan()
	budgetWait(t, "real Mongo cursor cleanup", func() bool {
		n := 0
		for _, event := range peers[1].proxy.Events() {
			if event.Command == "killCursors" && event.Acknowledged {
				n++
			}
		}
		return n > beforeCleanup
	})
	budgetWait(t, "cancelled Scan ledger empty", func() bool {
		return testmetrics.Sum(testmetrics.Scrape(t, peers[1].process.diagnostic), "weir_store_live_sessions") == 0
	})
	t.Log("Read/Mutate/Bulk/Native/Scan completed; three executor increments 36 + independent native writer 12 + seed 1 = 49; database atomicity, no global Weir ordering claim")
}

func mongoBudgetNative(t *testing.T, e *mongoBudgetExecutor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	descriptor := &pb.Document{MediaType: mongodb.NativeDescriptor}
	open := &pb.NativeOpen{Resource: e.root, Descriptor_: descriptor, BodyMediaType: "application/bson"}
	filter := bson.D{{Key: "_id", Value: "counter"}}
	command := bson.D{{Key: "count", Value: "records"}, {Key: "query", Value: filter}, {Key: "limit", Value: 1}}
	raw, _ := bson.Marshal(command)
	nativeCall := &pb.NativeCall{Open: open, Body: raw}
	nativeVariant := &pb.Call_Native{Native: nativeCall}
	call := &pb.Call{Version: 1, Operation: nativeVariant}
	stream, err := testutil.OneEvents(ctx, e.client, call)
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
	if testmetrics.Sum(testmetrics.Scrape(t, replacement.process.diagnostic), "weir_store_window_limit") != 2 {
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
			locals:  e.locals,
			limit:   e.concurrency + 1,
		}
		targets = append(targets, target)
	}
	extra := budgetReadTarget{
		process: replacement.process,
		client:  replacement.client,
		root:    strings.Replace(replacement.root, "weir://records/", "weir://extra/", 1),
		locals:  2,
		limit:   2,
	}
	targets = append(targets, extra)
	budgetOverlap(t, targets, opts.observation)
	budgetReplacementReads(t, targets[3:])
	mongoBudgetNative(t, replacement)
	extraExecutor := *replacement
	extraExecutor.root = extra.root
	mongoBudgetNative(t, &extraExecutor)
	drop := old.drop
	old.proxy.DropRemaining.Store(1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resultChannel := make(chan *pb.MutationResult, 1)
	oldRequest := budgetPut(old.root, "lost-reply")
	go func() {
		routedResult642, err := weirclient.Record(ctx, old.client, testutil.RecordCall(oldRequest))
		result := routedResult642.GetMutation()
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
		_, _ = weirclient.Record(queuedCtx, old.client, testutil.RecordCall(request))
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
	routedResult685, err := weirclient.Record(ctx, replacement.client, testutil.RecordCall(request))
	result = routedResult685.GetMutation()
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("replacement new mutation", result, err)
	}
	request = budgetPut(extra.root, "new-independent-extra")
	var routedResult690 *pb.Result
	routedResult690, err = weirclient.Record(ctx, replacement.client, testutil.RecordCall(request))
	result = routedResult690.GetMutation()
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
	produced, completed := 0, 0
	options := weirclient.Options{
		StoreName: "records",
		Produce: func(context.Context) (*pb.Call, error) {
			if produced == 4 {
				return nil, io.EOF
			}
			produced++
			mutation := &pb.Call_Mutate{Mutate: opts.request}
			call := &pb.Call{Version: 1, Operation: mutation}
			return call, nil
		},
		Consume: func(_ context.Context, id uint64, event *pb.Event) error {
			if event.GetResult() != nil {
				if id != uint64(completed+1) || event.GetResult().GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
					return fmt.Errorf("direct ordered batch invalid result %d", id)
				}
				completed++
			}
			return nil
		},
	}
	if err := client.Execute(ctx, options); err != nil || completed != 4 {
		t.Fatal("direct pinned batch", completed, err)
	}
	metrics := testmetrics.Scrape(t, seed.diagnostic)
	if metrics["weir_backend_connections_limit"] != nil || metrics["weir_store_window_limit"] != nil || metrics["weir_store_executions_total"] != nil || metrics["weir_relay_terminations_total"] != nil {
		t.Fatal("initialization node owns execution or forwarding")
	}
	targets := 0
	for i, p := range opts.peers {
		delta := testmetrics.Sum(testmetrics.Scrape(t, p.diagnostic), "weir_store_records_total") - before[i]
		if delta == 4 {
			targets++
		} else if delta != 0 {
			t.Fatal("business stream migrated or replayed", delta)
		}
	}
	if targets != 1 {
		t.Fatal("business stream did not remain on one executor", targets)
	}
	t.Logf("initialization-only PID=%d, direct ordered batch/4 writes; no relay or local pool", seed.command.Process.Pid)
}

// The same finite overload shape drives both real adapter profiles. Each Bulk
// call checks result association and End/EOF; errors are counted, never replayed.
func budgetLoadCall(ctx context.Context, client pb.StoreServiceClient, request *pb.MutateRequest, mode int) bool {
	switch mode {
	case 0:
		routedResult820, err := weirclient.Record(ctx, client, testutil.RecordCall(request))
		result := routedResult820.GetMutation()
		return err == nil && result.GetFailure() == nil && result.GetOutcome() == pb.MutationOutcome_APPLIED
	case 1:
		read := &pb.ReadRequest{Resource: request.Resource}
		routedResult824, err := weirclient.Record(ctx, client, testutil.RecordCall(read))
		result := routedResult824.GetRead()
		return err == nil && result.GetFailure() == nil
	default:
		stream := testutil.OpenEvents(ctx, client, "records")
		var frame *pb.Call
		variant := &pb.Operation_Mutate{Mutate: request}
		operation := &pb.Operation{Operation: variant}
		_, item := testutil.OperationCall(operation)
		frame = item
		if stream.Send(frame) != nil || stream.CloseSend() != nil {
			return false
		}
		reply, err := stream.Recv()
		if err != nil || reply.GetResult().GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
			return false
		}
		_, err = stream.Recv()
		return err == io.EOF
	}
}
