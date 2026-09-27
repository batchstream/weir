//go:build integration

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	spb "github.com/batchstream/weir/api/weir/search/v1"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"google.golang.org/protobuf/proto"
)

type searchBudgetExecutor struct {
	process     *process
	client      pb.WeirClient
	proxy       *searchBudgetProxy
	root        string
	concurrency int
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
	connection := &search.Connection{Username: b.Username, Password: b.Password, CAFile: b.CAFile}
	backend := &Search{URL: "https://" + proxy.listener.Addr().String(), Index: b.Index, Profile: b.Profile, Connection: connection}
	local := &Local{Search: backend, Concurrency: opts.concurrency, BatchOperations: 1}
	service := Service{Name: "database", Local: local}
	route := Route{Store: "records", Service: "database"}
	cfg := DefaultConfig()
	cfg.Application, cfg.Peer, cfg.Diagnostics = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
	cfg.Services, cfg.Routes = []Service{service}, []Route{route}
	if opts.extra {
		second := Service{Name: "second", Local: local}
		secondRoute := Route{Store: "extra", Service: "second"}
		cfg.Services = append(cfg.Services, second)
		cfg.Routes = append(cfg.Routes, secondRoute)
	}
	p := startProcess(t, opts.binary, cfg)
	e := &searchBudgetExecutor{process: p, client: endpointProcessClient(t, p.address), proxy: proxy, root: "weir://records/" + b.Index, concurrency: opts.concurrency}
	t.Logf("start time=%s PID=%d C=%d extra-local=%t application=%s diagnostics=%s", time.Now().UTC().Format(time.RFC3339Nano), p.command.Process.Pid, opts.concurrency, opts.extra, p.address, p.diagnostic)
	return e
}
func searchBudgetPut(root, id string) *pb.MutateRequest {
	document := &pb.Document{MediaType: "application/json", Data: []byte(`{"n":1}`)}
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
		result, err := e.client.Read(ctx, request)
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
	if os.Getenv("WEIR_M12_INTEGRATION") != "search" {
		t.Skip("requires WEIR_M12_INTEGRATION=search")
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
					result, err := e.client.Read(ctx, request)
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
	budgetWait(t, "warm settled", func() bool { a, _, _, _ := observation.snapshot(); return a == 0 })
	for _, e := range peers {
		if window := budgetWindow(t, e.process); window != e.concurrency {
			t.Fatal("Search AIMD did not reach C", window, e.concurrency)
		}
	}
	gate := make(chan struct{})
	observation.hold(gate, 0)
	for _, e := range peers {
		for range e.concurrency {
			workers.Go(func() { searchBudgetRead(t, e) })
		}
	}
	budgetWait(t, "seven Search HTTP exchanges", func() bool { a, _, _, _ := observation.snapshot(); return a == 7 })
	for _, e := range peers {
		current, peak := e.proxy.sockets()
		if current != e.concurrency || peak > e.concurrency+1 {
			t.Fatal("Search ordinary TCP budget", current, peak, e.concurrency)
		}
		metrics := testmetrics.Scrape(t, e.process.diagnostic)
		if testmetrics.Sum(metrics, "weir_store_active_executions") != float64(e.concurrency) || testmetrics.Sum(metrics, "weir_store_window_limit") != float64(e.concurrency) {
			t.Fatal("Search runtime assembly")
		}
		t.Logf("barrier PID=%d active=window=C=%d upstream TCP=%d peak=%d", e.process.command.Process.Pid, e.concurrency, current, peak)
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
	budgetWait(t, "Native plus idle pool", func() bool { n, _ := peers[2].proxy.sockets(); return n == peers[2].concurrency+1 })
	t.Logf("Native + ordinary idle connections: PID=%d TCP=%d=P+1 while Native holds one Store permit", peers[2].process.command.Process.Pid, peers[2].concurrency+1)
	close(nativeGate)
	peers[2].proxy.observation.hold(nil, 0)
	workers.Wait()
	// A synthetic 429 on only one executor's proxy proves independent AIMD;
	// the overload segment below is real admission pressure, not this fault.
	peers[2].proxy.reject.Store(true)
	callCtx, stop := context.WithTimeout(context.Background(), time.Second)
	request := &pb.ReadRequest{Resource: peers[2].root + "/s:seed"}
	_, _ = peers[2].client.Read(callCtx, request)
	stop()
	peers[2].proxy.reject.Store(false)
	if budgetWindow(t, peers[2].process) != 2 || budgetWindow(t, peers[1].process) != 2 {
		t.Fatal("AIMD instances not independent")
	}
	for _, e := range peers {
		searchBudgetRead(t, e)
	}
	searchBudgetOverload(t, peers, observation)
	searchBudgetMixed(t, peers, f)
	forward := budgetForwardOptions{binary: binary, peers: []*process{peers[0].process, peers[1].process, peers[2].process}, request: searchBudgetPut(first.root, "forwarded")}
	budgetForwarding(t, forward)
	opts.concurrency = 1
	opts.extra = true
	searchBudgetReplacement(t, peers, opts)
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
		if peak > e.concurrency+1 {
			t.Fatal("Search socket high-water exceeded P+1", peak)
		}
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
		expected += budgetWindow(t, e.process)
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
					success := budgetLoadCall(ctx, e.client, request, worker%3)
					if !success {
						rejected.Add(1)
					} else {
						applied.Add(1)
					}
				}
			})
		}
	}
	budgetWait(t, "Search overload active", func() bool { a, _, _, _ := o.snapshot(); return a == expected })
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
			result, err := e.client.Mutate(ctx, request)
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
		stream, err := e.client.Bulk(ctx)
		if err != nil {
			t.Fatal(err)
		}
		opening := &pb.BulkOpen{Store: "weir://records"}
		variant := &pb.BulkRequestFrame_Open{Open: opening}
		frame := &pb.BulkRequestFrame{Frame: variant}
		if err := stream.Send(frame); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 4; i++ {
			request := searchBudgetPut(e.root, fmt.Sprintf("bulk%d-%d", e.concurrency, i))
			mutation := &pb.BulkOperation_Mutate{Mutate: request}
			op := &pb.BulkOperation{Index: uint64(i), Operation: mutation}
			item := &pb.BulkRequestFrame_Operation{Operation: op}
			frame := &pb.BulkRequestFrame{Frame: item}
			if err := stream.Send(frame); err != nil {
				t.Fatal(err)
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatal(err)
		}
		seen := make(map[uint64]bool)
		for range 4 {
			reply, err := stream.Recv()
			item := reply.GetResult()
			if err != nil || item == nil || item.Index >= 4 || seen[item.Index] {
				t.Fatal("Search Bulk association", reply, err)
			}
			seen[item.Index] = true
			result := item.GetMutation()
			status, _ := f.Admin.Do(t, "GET", fmt.Sprintf("/%s/_doc/bulk%d-%d", f.Backend.Index, e.concurrency, item.Index), "")
			switch result.GetOutcome() {
			case pb.MutationOutcome_APPLIED:
				if status != 200 {
					t.Fatal("false applied Bulk", status)
				}
			case pb.MutationOutcome_NOT_APPLIED:
				if status != 404 || result.GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE {
					t.Fatal("false not-applied Bulk", status, result)
				}
			default:
				t.Fatal("unexpected Bulk evidence", result)
			}
		}
		end, err := stream.Recv()
		if err != nil || end.GetEnd().GetResultCount() != 4 {
			t.Fatal("Search Bulk End", end, err)
		}
		if _, err := stream.Recv(); err != io.EOF {
			t.Fatal(err)
		}
	}
	f.Admin.Do(t, "POST", "/"+f.Backend.Index+"/_refresh", "")
	workers.Go(func() { searchBudgetNative(t, peers[0]) })
	workers.Go(func() {
		request := &pb.ScanRequest{Resource: peers[1].root, FetchItemsHint: 2}
		scan, err := peers[1].client.Scan(ctx, request)
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
			if end := frame.GetEnd(); end != nil {
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
	stream, err := e.client.Native(ctx)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := &spb.Request{Method: "GET", Path: "/_doc/native-probe"}
	encoded, err := proto.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{MediaType: search.NativeDescriptor, Data: encoded}
	opening := &pb.NativeOpen{Resource: e.root, Descriptor_: document}
	variant := &pb.NativeRequestFrame_Open{Open: opening}
	frame := &pb.NativeRequestFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	for {
		reply, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if end := reply.GetEnd(); end != nil {
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
func searchBudgetReplacement(t *testing.T, peers []*searchBudgetExecutor, opts searchBudgetStart) {
	t.Helper()
	old := peers[0]
	replacement := startSearchBudgetExecutor(t, opts)
	defer replacement.process.stop(t)
	t.Logf("3 -> 4 executors time=%s; replacement PID=%d has TWO Local adapters for same index", time.Now().UTC().Format(time.RFC3339Nano), replacement.process.command.Process.Pid)
	if testmetrics.Sum(testmetrics.Scrape(t, replacement.process.diagnostic), "weir_store_window_limit") != 2 {
		t.Fatal("two Search Local budgets merged")
	}
	n, peak := replacement.proxy.sockets()
	if n != 2 || peak > 4 {
		t.Fatal("two Search pools", n, peak)
	}
	t.Log("overlap: 4 executors / 5 Local adapters, sum C=9, ordinary+Native TCP budget=14; replacement 2 ordinary sockets, up to 4 with Native")
	oldBefore := old.proxy.mutations.Load()
	oldApplied := old.proxy.applied.Load()
	old.proxy.dropNext.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	response := make(chan *pb.MutationResult, 1)
	go func() {
		request := searchBudgetPut(old.root, "lost-reply")
		result, err := old.client.Mutate(ctx, request)
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
		_, _ = old.client.Mutate(queuedCtx, request)
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
	budgetWait(t, "Search old sockets closed", func() bool { n, _ := old.proxy.sockets(); return n == 0 })
	request := searchBudgetPut(replacement.root, "new-independent")
	result, err := replacement.client.Mutate(ctx, request)
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal(result, err)
	}
	if old.proxy.mutations.Load() != oldBefore+1 || replacement.proxy.mutations.Load() != 1 {
		t.Fatal("old mutation replay or queued write sent")
	}
	for _, id := range []string{"lost-reply", "queued-cancel", "new-independent"} {
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
	t.Logf("4 -> 3 time=%s old PID=%d Wait exited, sockets=0; acknowledged drop=1 -> UNKNOWN; queued write absent; new mutation count=1, versions=1; no replay", time.Now().UTC().Format(time.RFC3339Nano), old.process.command.Process.Pid)
}
