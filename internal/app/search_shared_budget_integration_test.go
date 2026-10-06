//go:build integration

package app

import (
	"context"
	"encoding/json"
	"fmt"
	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir/internal/testutil"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testsearch"
)

type searchBudgetExecutor struct {
	store       string
	process     *process
	client      pb.StoreServiceClient
	proxy       *searchBudgetProxy
	root        string
	concurrency int
	locals      int
}

type searchBudgetStart struct {
	binary      string
	fixture     *testsearch.SecureFixture
	observation *budgetObservation
	concurrency int
	extra       bool
}

func startSearchBudgetExecutor(t *testing.T, opts searchBudgetStart) *searchBudgetExecutor {
	t.Helper()
	b := opts.fixture.Backend
	proxy := startSearchBudgetProxy(t, opts.fixture, opts.observation)
	connection := &SearchConnection{Username: b.Username, Password: b.Password, CAFile: b.CAFile}
	backend := &Search{
		URL:        "https://" + proxy.listener.Addr().String(),
		Connection: connection,
	}
	local := &Local{Search: backend, MaxConcurrency: opts.concurrency, MaxBatchOperations: 1}
	service := StoreConfig{Name: "records", Local: local}

	cfg := DefaultConfig()
	// Keep transport admission above the largest backend concurrency so the
	// overload phase observes queued work as well as transport rejections.
	cfg.Basic.Transport.MaxSessions = 8
	cfg.Basic.Listeners.Application, cfg.Basic.Listeners.Peer, cfg.Basic.Diagnostics.Address = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
	cfg.Basic.Discovery.Group = "records"
	cfg.Routing.Stores = []StoreConfig{service}
	if opts.extra {
		second := StoreConfig{Name: "extra", Local: local}

		cfg.Routing.Stores = append(cfg.Routing.Stores, second)

	}
	p := startProcess(t, opts.binary, cfg)
	e := &searchBudgetExecutor{
		store:       "records",
		process:     p,
		client:      endpointProcessClient(t, p.address),
		proxy:       proxy,
		root:        b.Index,
		concurrency: opts.concurrency,
		locals:      len(cfg.Routing.Stores),
	}
	t.Logf(
		"start time=%s PID=%d C=%d extra-local=%t application=%s diagnostics=%s",
		time.Now().UTC().Format(time.RFC3339Nano),
		p.command.Process.Pid,
		opts.concurrency,
		opts.extra,
		p.address,
		p.diagnostic,
	)
	return e
}

func searchBudgetPut(root, id string) *pb.MutateRequest {
	document := &pb.Document{ContentType: "application/json", Data: []byte(`{"n":1}`)}
	action := &pb.MutateRequest_Put{Put: document}
	request := &pb.MutateRequest{Resource: root + "/s:" + id, Action: action}
	return request
}

func searchBudgetRead(t *testing.T, e *searchBudgetExecutor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request := &pb.ReadRequest{Resource: e.root + "/s:seed"}
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
			t.Fatal("new Search calls did not recover within original 3s budget", result, err)
		}
		// A cancelled HTTP/2 connection may still be reconnecting. These are
		// distinct read-only probes, never a retry of an old mutation.
		time.Sleep(20 * time.Millisecond)
	}
}

func TestSearchSharedProcessBudget(t *testing.T) {
	if os.Getenv("WEIR_SHARED_BUDGET_INTEGRATION") != "search" {
		t.Skip("requires WEIR_SHARED_BUDGET_INTEGRATION=search")
	}
	f := testsearch.OpenSecure(t)
	binary := buildEndpointProcess(t)
	observation := &budgetObservation{}
	opts := searchBudgetStart{binary: binary, fixture: f, observation: observation, concurrency: 1}
	first := startSearchBudgetExecutor(t, opts)
	peers := []*searchBudgetExecutor{first}
	searchBudgetRead(t, first)
	for _, c := range []int{2, 4} {
		opts.concurrency = c
		peers = append(peers, startSearchBudgetExecutor(t, opts))
	}
	t.Log("executor sequence 1 -> 3; capacities 1+2+4=7; one native HTTPS/Basic index")
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
	budgetWait(t, "warm settled", func() bool {
		a, _, _, _ := observation.snapshot()
		return a == 0
	})
	for _, e := range peers {
		if capacity := budgetConcurrency(t, e.process); capacity != e.concurrency {
			t.Fatal("Search configured concurrency changed", capacity, e.concurrency)
		}
	}
	gate := make(chan struct{})
	observation.hold(gate, 0)
	for _, e := range peers {
		for range e.concurrency {
			workers.Go(func() { searchBudgetRead(t, e) })
		}
	}
	budgetWait(t, "seven Search HTTP exchanges", func() bool {
		a, _, _, _ := observation.snapshot()
		return a == 7
	})
	for _, e := range peers {
		current, peak := e.proxy.sockets()
		budgetOwner(t, e.process, e.locals, e.concurrency+1)
		metrics := testmetrics.Scrape(t, e.process.diagnostic)
		if testmetrics.Sum(metrics, "weir_store_active_executions") != float64(e.concurrency) || testmetrics.Sum(metrics, "weir_store_concurrency_limit") != float64(e.concurrency) {
			t.Fatal("Search runtime assembly")
		}
		t.Logf("barrier PID=%d active=C=%d upstream TCP=%d peak=%d", e.process.command.Process.Pid, e.concurrency, current, peak)
	}
	close(gate)
	observation.hold(nil, 0)
	workers.Wait()
	t.Logf("stable segment: 24 readers/1.2s, 10ms proxy delay, completed=%d; simultaneous HTTP exchanges=7", reads.Load())
	// Hold Native after all P ordinary connections are already idle. Its separate
	// connection must coexist with those idles while using one execution permit.
	nativeGate := make(chan struct{})
	peers[2].proxy.observation.hold(nativeGate, 0)
	workers.Go(func() { searchBudgetNative(t, peers[2]) })
	budgetWait(t, "Native plus idle pool", func() bool {
		n, _ := peers[2].proxy.sockets()
		return n == peers[2].concurrency+1
	})
	t.Logf("Native + ordinary idle connections: PID=%d TCP=%d=P+1 while Native holds one Store permit", peers[2].process.command.Process.Pid, peers[2].concurrency+1)
	close(nativeGate)
	peers[2].proxy.observation.hold(nil, 0)
	workers.Wait()
	// A synthetic 429 preserves backend read failure evidence without changing
	// configured execution capacity. The overload segment uses real admission.
	peers[2].proxy.reject.Store(true)
	callCtx, stop := context.WithTimeout(context.Background(), time.Second)
	request := &pb.ReadRequest{Resource: peers[2].root + "/s:seed"}
	faultResult, faultError := testutil.ExecuteRecord(callCtx, peers[2].client, testutil.RecordRequest(peers[2].store, request))
	stop()
	peers[2].proxy.reject.Store(false)
	if faultError != nil || faultResult.GetReadResult().GetFailure() == nil {
		t.Fatal("Search congestion lost its read failure", faultError, faultResult)
	}
	if budgetConcurrency(t, peers[2].process) != 4 || budgetConcurrency(t, peers[1].process) != 2 {
		t.Fatal("Search backend failure changed configured execution limits")
	}
	failureLabels := map[string]string{"operation": "read", "outcome": "failure"}
	failures := testmetrics.Sample(testmetrics.Scrape(t, peers[2].process.diagnostic), "weir_store_records_total", failureLabels)
	if failures.GetCounter().GetValue() < 1 {
		t.Fatal("backend read failure was not counted")
	}
	for _, e := range peers {
		searchBudgetRead(t, e)
	}
	searchBudgetOverload(t, peers, observation)
	searchBudgetMixed(t, peers, f)
	discovery := budgetDiscoveryOptions{
		binary:  binary,
		peers:   []*process{peers[0].process, peers[1].process, peers[2].process},
		request: searchBudgetPut(first.root, "discovered"),
	}
	budgetDirectDiscovery(t, discovery)
	opts.concurrency = 1
	opts.extra = true
	replacement := searchBudgetReplacement(t, peers, opts)
	peers = append(peers, replacement)
	for _, e := range peers {
		e.process.stop(t)
	}
	budgetWait(t, "Search sockets closed", func() bool {
		for _, e := range peers {
			n, _ := e.proxy.sockets()
			if n != 0 {
				return false
			}
		}
		return true
	})
	for _, e := range peers {
		current, peak := e.proxy.sockets()
		t.Logf("final PID=%d C=%d upstream current=%d event high-water=%d", e.process.command.Process.Pid, e.concurrency, current, peak)
		budgetClosedOwner(t, e.process, e.locals, e.concurrency+1)
	}
	_, peak, started, completed := observation.snapshot()
	if started != completed {
		t.Fatal("unfinished Search exchanges", started, completed)
	}
	t.Logf("final proxy HTTP peak=%d total=%d completed=%d; all executor upstream sockets=0; all Weir processes Wait completed", peak, started, completed)
}

func searchBudgetOverload(t *testing.T, peers []*searchBudgetExecutor, o *budgetObservation) {
	t.Helper()
	expected := 0
	for _, e := range peers {
		expected += budgetConcurrency(t, e.process)
	}
	gate := make(chan struct{})
	o.hold(gate, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 450*time.Millisecond)
	var workers sync.WaitGroup
	var rejected, applied atomic.Int64
	for _, e := range peers {
		for worker := 0; worker < 24; worker++ {
			workers.Go(func() {
				for n := 0; ctx.Err() == nil; n++ {
					request := searchBudgetPut(e.root, fmt.Sprintf("overload%d-%d", worker, n))
					success := budgetLoadCall(ctx, e.client, testutil.RecordRequest(e.store, request), worker%3)
					if !success {
						rejected.Add(1)
					} else {
						applied.Add(1)
					}
				}
			})
		}
	}
	budgetWait(t, "Search overload active", func() bool {
		a, _, _, _ := o.snapshot()
		return a == expected
	})
	for _, e := range peers {
		f := testmetrics.Scrape(t, e.process.diagnostic)
		if testmetrics.Sum(f, "weir_store_pending_entries") < 1 || testmetrics.Sum(f, "weir_store_active_executions") > float64(e.concurrency) {
			t.Fatal("Search queue/cap")
		}
	}
	time.Sleep(100 * time.Millisecond)
	close(gate)
	o.hold(nil, 0)
	workers.Wait()
	cancel()
	if rejected.Load() == 0 || applied.Load() == 0 {
		t.Fatal("no Search overload/recovery", rejected.Load(), applied.Load())
	}
	for _, e := range peers {
		searchBudgetRead(t, e)
	}
	t.Logf("72 mixed Read/Mutate/Bulk producers/450ms, 100ms wire hold: rejected/non-OK=%d successful=%d, fresh calls recovered", rejected.Load(), applied.Load())
}

func searchBudgetMixed(t *testing.T, peers []*searchBudgetExecutor, f *testsearch.SecureFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var workers sync.WaitGroup
	var wins atomic.Int32
	// Create's backend atomic precondition must have exactly one winner across
	// all processes and a direct native client. No scheduler key domain is shared.
	gate := make(chan struct{})
	for _, e := range peers {
		workers.Go(func() {
			<-gate
			request := searchBudgetPut(e.root, "race-create")
			doc := request.GetPut()
			request.Action = &pb.MutateRequest_Create{Create: doc}
			recordResult3, err := testutil.ExecuteRecord(ctx, e.client, testutil.RecordRequest(e.store, request))
			result := recordResult3.GetMutationResult()
			if err != nil {
				t.Error(err)
				return
			}
			if result.GetOutcome() == pb.MutationOutcome_APPLIED {
				wins.Add(1)
			} else if result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED {
				t.Error("Create ambiguity", result)
			}
		})
	}
	workers.Go(func() {
		<-gate
		status, _ := f.Backend.Do(t, "PUT", "/"+f.Backend.Index+"/_create/race-create", `{"n":1}`)
		if status == 201 {
			wins.Add(1)
		} else if status == 429 {
			// The owned backend deliberately has one write worker and one
			// queue entry. A capacity rejection cannot count as a Create win.
			t.Log("native Create rejected at the configured backend capacity")
		} else if status != 409 {
			t.Error("native create", status)
		}
	})
	close(gate)
	workers.Wait()
	if wins.Load() != 1 {
		t.Fatal("atomic create winner", wins.Load())
	}
	for _, e := range peers {
		requests := make([]*pb.MutateRequest, 0, 4)
		for i := range 4 {
			request := searchBudgetPut(e.root, fmt.Sprintf("bulk%d-%d", e.concurrency, i))
			requests = append(requests, request)
		}
		reply, err := testutil.MutateRecords(ctx, e.client, "records", requests)
		if err != nil || len(reply) != len(requests) {
			t.Fatal("Search batch association", reply, err)
		}
		for i, result := range reply {
			status, _ := f.Admin.Do(t, "GET", fmt.Sprintf("/%s/_doc/bulk%d-%d", f.Backend.Index, e.concurrency, i), "")
			switch result.GetOutcome() {
			case pb.MutationOutcome_APPLIED:
				if status != 200 {
					t.Fatal("false applied batch", i, status)
				}
			case pb.MutationOutcome_NOT_APPLIED:
				if status != 404 || result.GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE {
					t.Fatal("false not-applied batch", i, status, result)
				}
			default:
				t.Fatal("unexpected batch evidence", i, result)
			}
		}
	}
	f.Admin.Do(t, "POST", "/"+f.Backend.Index+"/_refresh", "")
	workers.Go(func() { searchBudgetNative(t, peers[0]) })
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
			t.Error(err)
		}
	})
	for _, e := range peers {
		workers.Go(func() {
			for range 8 {
				searchBudgetRead(t, e)
			}
		})
	}
	workers.Wait()
	if t.Failed() {
		t.Fatal("Search mixed operations failed")
	}
	t.Log("Read/Mutate/Bulk/Native/Scan coexistence; three processes plus direct native Create: exactly one winner, DB atomicity")
}

func searchBudgetNative(t *testing.T, e *searchBudgetExecutor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	httpRequest, err := http.NewRequest(http.MethodGet, "http://ignored.invalid/_doc/native-probe", nil)
	if err != nil {
		t.Fatal(err)
	}
	nativeCall, err := weirclient.NewHTTPNativeRequest(e.root, httpRequest)
	if err != nil {
		t.Fatal(err)
	}
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

func searchBudgetReplacement(t *testing.T, peers []*searchBudgetExecutor, opts searchBudgetStart) *searchBudgetExecutor {
	t.Helper()
	old := peers[0]
	replacement := startSearchBudgetExecutor(t, opts)
	t.Logf(
		"3 -> 4 executors time=%s; replacement PID=%d has TWO Local adapters for same index",
		time.Now().UTC().Format(time.RFC3339Nano),
		replacement.process.command.Process.Pid,
	)
	if testmetrics.Sum(testmetrics.Scrape(t, replacement.process.diagnostic), "weir_store_concurrency_limit") != 2 {
		t.Fatal("two Search Local budgets merged")
	}
	n, _ := replacement.proxy.sockets()
	if n != 2 {
		t.Fatal("two Search pools", n)
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
	extraExecutor := *replacement
	extraExecutor.store = extra.store
	nativeGate := make(chan struct{})
	opts.observation.hold(nativeGate, 0)
	var native sync.WaitGroup
	native.Go(func() { searchBudgetNative(t, replacement) })
	native.Go(func() { searchBudgetNative(t, &extraExecutor) })
	budgetWait(t, "two Native plus ordinary idle owners", func() bool { return budgetOwner(t, replacement.process, 2, 2) == 4 })
	close(nativeGate)
	opts.observation.hold(nil, 0)
	native.Wait()
	oldBefore := old.proxy.mutations.Load()
	oldApplied := old.proxy.applied.Load()
	old.proxy.dropNext.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	response := make(chan *pb.MutationResult, 1)
	go func() {
		request := searchBudgetPut(old.root, "lost-reply")
		recordResult4, err := testutil.ExecuteRecord(ctx, old.client, testutil.RecordRequest(old.store, request))
		result := recordResult4.GetMutationResult()
		if err != nil {
			t.Error(err)
		}
		response <- result
	}()
	budgetWait(t, "Search actual backend commit", func() bool { return old.proxy.dropped.Load() == 1 && old.proxy.applied.Load() == oldApplied+1 })
	queuedCtx, queuedCancel := context.WithCancel(ctx)
	queuedDone := make(chan struct{})
	go func() {
		defer close(queuedDone)
		request := searchBudgetPut(old.root, "queued-cancel")
		_, _ = testutil.ExecuteRecord(queuedCtx, old.client, testutil.RecordRequest(old.store, request))
	}()
	budgetWait(t, "Search old queued write", func() bool {
		return testmetrics.Sum(testmetrics.Scrape(t, old.process.diagnostic), "weir_store_pending_entries") == 1
	})
	queuedCancel()
	<-queuedDone
	budgetWait(t, "Search queued cancel", func() bool {
		return testmetrics.Sum(testmetrics.Scrape(t, old.process.diagnostic), "weir_store_pending_entries") == 0
	})
	if err := old.process.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	close(old.proxy.drop)
	result := <-response
	if result.GetOutcome() != pb.MutationOutcome_UNKNOWN {
		t.Fatal("Search commit reply loss outcome", result)
	}
	old.process.stop(t)
	budgetWait(t, "Search old sockets closed", func() bool {
		n, _ := old.proxy.sockets()
		return n == 0
	})
	request := searchBudgetPut(replacement.root, "new-independent")
	recordResult5, err := testutil.ExecuteRecord(ctx, replacement.client, testutil.RecordRequest(replacement.store, request))
	result = recordResult5.GetMutationResult()
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal(result, err)
	}
	request = searchBudgetPut(extra.root, "new-independent-extra")
	recordResult6, err := testutil.ExecuteRecord(ctx, replacement.client, testutil.RecordRequest(extra.store, request))
	result = recordResult6.GetMutationResult()
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("extra Local mutation", result, err)
	}
	if old.proxy.mutations.Load() != oldBefore+1 || replacement.proxy.mutations.Load() != 2 {
		t.Fatal("old mutation replay or queued write sent")
	}
	for _, id := range []string{"lost-reply", "queued-cancel", "new-independent", "new-independent-extra"} {
		status, raw := opts.fixture.Admin.Do(t, "GET", "/"+opts.fixture.Backend.Index+"/_doc/"+id, "")
		if id == "queued-cancel" {
			if status != 404 {
				t.Fatal("queued write reached backend")
			}
			continue
		}
		var doc struct {
			Version int `json:"_version"`
		}
		if status != 200 || json.Unmarshal(raw, &doc) != nil || doc.Version != 1 {
			t.Fatal("version/readback", id, status, doc.Version)
		}
	}
	t.Logf(
		"4 -> 3 time=%s old PID=%d Wait exited, sockets=0; acknowledged drop=1 -> UNKNOWN; queued write absent; new mutation count=2 across both Local stores, versions=1; no replay",
		time.Now().UTC().Format(time.RFC3339Nano),
		old.process.command.Process.Pid,
	)
	return replacement
}
