//go:build integration

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Real replies hold the initial read and subsequent Lua reads independently,
// allowing cancellation to be checked while all execution remains concurrent.
// Nothing changes the production adapter or scheduler to create this barrier.
type luaRPCReadGate struct {
	reads   atomic.Int32
	entered chan int
	release [2]chan struct{}
	once    [2]sync.Once
}

func newLuaRPCReadGate() *luaRPCReadGate {
	gate := &luaRPCReadGate{entered: make(chan int, 2)}
	for i := range gate.release {
		gate.release[i] = make(chan struct{})
	}
	return gate
}

func (g *luaRPCReadGate) observe(ctx context.Context) {
	read := int(g.reads.Add(1))
	if read <= len(g.release) {
		g.entered <- read
	}
	index := min(read-1, len(g.release)-1)
	select {
	case <-g.release[index]:
	case <-ctx.Done():
	}
}

func (g *luaRPCReadGate) unblock(index int) {
	g.once[index].Do(func() { close(g.release[index]) })
}

func (g *luaRPCReadGate) unblockAll() {
	for i := range g.release {
		g.unblock(i)
	}
}

type luaRPCCase struct {
	id       string
	program  string
	outcome  pb.MutationOutcome
	failure  pb.FailureCode
	wantN    int64
	canceled bool
}

func luaRPCIncrement(amount int64) string {
	return fmt.Sprintf(`return function(current, incoming) current.n = current.n + %d; return current end`, amount)
}

func luaRPCIndependentCases(count int) []luaRPCCase {
	cases := make([]luaRPCCase, count)
	for i := range cases {
		cases[i] = luaRPCCase{id: fmt.Sprintf("record-%02d", i), program: luaRPCIncrement(int64(i + 1)), outcome: pb.MutationOutcome_APPLIED, wantN: int64(i + 1)}
	}
	cases[count-4].program, cases[count-4].wantN = "return function(current, incoming) return weir.keep() end", 0
	cases[count-3].program, cases[count-3].wantN = `return function(current, incoming) return weir.reject("caller rejected") end`, 0
	cases[count-3].outcome, cases[count-3].failure = pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_PRECONDITION_FAILED
	cases[count-2].program, cases[count-2].wantN = "return function(current, incoming) return current.missing_method() end", 0
	cases[count-2].outcome, cases[count-2].failure = pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_INVALID_ARGUMENT
	cases[count-1].canceled, cases[count-1].wantN = true, 0
	return cases
}

func luaRPCMutation(resource, source string) *pb.MutateRequest {
	program := &pb.LuaTransform{Source: []byte(source)}
	form := &pb.Transform_Lua{Lua: program}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation := &pb.MutateRequest{Resource: resource, Action: action}
	return mutation
}

func startLuaRPCNode(t *testing.T, adapter execution.Adapter) *routeAcceptanceNode {
	t.Helper()
	limits := store.DefaultLimits()
	limits.BatchOperations = 32

	ingress := DefaultLimits()

	ingress.Stall = 5 * time.Second
	options := routeAcceptanceNodeOptions{adapter: adapter, store: limits, limits: ingress}
	return startRouteAcceptanceNode(t, options)
}

type luaRPCBatchOptions struct {
	node           *routeAcceptanceNode
	gate           *luaRPCReadGate
	prefix         string
	cases          []luaRPCCase
	allowConflicts bool
	allowCapacity  bool
}

type luaRPCCallResult struct {
	response []*pb.MutationResult
	err      error
}

func runLuaRPCBatch(t *testing.T, opts luaRPCBatchOptions) int {
	t.Helper()
	defer opts.gate.unblockAll()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	clients := make([]pb.StoreServiceClient, 4)
	for i := range clients {
		clients[i] = routeAcceptanceClient(t, opts.node.address)
	}
	blockerDone := make(chan error, 1)
	go func() {
		read := &pb.ReadRequest{Resource: opts.prefix + "blocker"}
		request := &pb.ExecuteRequest{StoreName: "records", Index: 1, Command: &pb.Command{Operation: &pb.Command_Read{Read: read}}}

		_, err := testutil.ReadRecords(ctx, clients[0], request.StoreName, []*pb.ReadRequest{request.Command.GetRead()})
		blockerDone <- err
	}()
	waitLuaRPCRead(t, ctx, opts.gate, 1)
	results := make([]luaRPCCallResult, len(opts.cases))
	done := make([]chan struct{}, len(opts.cases))
	cancels := make([]context.CancelFunc, len(opts.cases))
	var workers sync.WaitGroup
	for i, item := range opts.cases {
		caller, stop := context.WithCancel(ctx)
		cancels[i] = stop
		defer stop()
		done[i] = make(chan struct{})
		workers.Go(func() {
			defer close(done[i])
			mutation := luaRPCMutation(opts.prefix+item.id, item.program)
			request := &pb.ExecuteRequest{StoreName: "records", Index: 1, Command: &pb.Command{Operation: &pb.Command_Mutate{Mutate: mutation}}}

			response, err := testutil.MutateRecords(caller, clients[i%len(clients)], request.StoreName, []*pb.MutateRequest{request.Command.GetMutate()})
			results[i] = luaRPCCallResult{response: response, err: err}
		})
	}
	for opts.node.runtime.Snapshot().Retained != len(opts.cases)+1 || opts.node.runtime.Snapshot().Pending != 0 {
		select {
		case <-ctx.Done():
			t.Fatal("single-record Lua RPCs did not enter concurrent execution", opts.node.runtime.Snapshot())
		case <-time.After(time.Millisecond):
		}
	}
	opts.gate.unblock(0)
	waitLuaRPCRead(t, ctx, opts.gate, 2)
	if err := <-blockerDone; err != nil {
		t.Fatal("real blocker Read failed", err)
	}
	for i, item := range opts.cases {
		if item.canceled {
			cancels[i]()
			select {
			case <-done[i]:
			case <-ctx.Done():
				t.Fatal("canceled Lua RPC did not release its caller")
			}
			// The client's cancellation return alone does not prove the server
			// has received it. Other handlers remain held at the backend reply.
			for opts.node.server.Snapshot().ActiveRPCs != int64(len(opts.cases)-1) {
				select {
				case <-ctx.Done():
					t.Fatal("server did not observe the canceled Lua RPC")
				case <-time.After(time.Millisecond):
				}
			}
		}
	}
	opts.gate.unblock(1)
	workers.Wait()
	applied := 0
	for i, result := range results {
		item := opts.cases[i]
		if item.canceled {
			if status.Code(result.err) != codes.Canceled {
				t.Errorf("canceled RPC %d returned %v", i, result.err)
			}
			continue
		}
		if result.err != nil || result.response == nil || len(result.response) != 1 {
			t.Errorf("RPC %d did not return its single result: response=%v err=%v", i, result.response, result.err)
			continue
		}
		mutation := result.response[0]
		if mutation.Outcome == pb.MutationOutcome_APPLIED {
			applied++
		}
		if opts.allowConflicts && mutation.Outcome == pb.MutationOutcome_NOT_APPLIED && (mutation.GetFailure().GetCode() == pb.FailureCode_PRECONDITION_FAILED || mutation.GetFailure().GetCode() == pb.FailureCode_CONFLICT) {
			continue
		}
		if opts.allowCapacity && mutation.Outcome == pb.MutationOutcome_NOT_APPLIED && mutation.GetFailure().GetCode() == pb.FailureCode_UNAVAILABLE && mutation.GetFailure().GetMessage() == "backend capacity unavailable" {
			opts.cases[i].wantN = 0
			continue
		}
		if mutation.Outcome != item.outcome || mutation.GetFailure().GetCode() != item.failure {
			t.Errorf("RPC %d received another caller's result: got=%v want=%v/%v", i, mutation, item.outcome, item.failure)
		}
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{opts.node})
	return applied
}

func waitLuaRPCRead(t *testing.T, ctx context.Context, gate *luaRPCReadGate, want int) {
	t.Helper()
	select {
	case read := <-gate.entered:
		if read != want {
			t.Fatal("unexpected physical read sequence", read, want)
		}
	case <-ctx.Done():
		t.Fatal("physical backend read did not reach admission barrier", want)
	}
}

func TestRouteMongoLuaRPCsShareTransactionWithoutDocumentMetadata(t *testing.T) {
	fixture := testmongo.Open(t)
	collection := fixture.Admin.Database(fixture.DB).Collection("records")
	cases := luaRPCIndependentCases(16)
	for _, item := range cases {
		document := bson.D{{Key: "_id", Value: item.id}, {Key: "n", Value: int32(0)}, {Key: "marker", Value: item.id}}
		if _, err := collection.InsertOne(t.Context(), document); err != nil {
			t.Fatal(err)
		}
	}
	gate := newLuaRPCReadGate()
	defer gate.unblockAll()
	proxy := testmongo.StartProxy(t, fixture)
	monitor := &event.CommandMonitor{Succeeded: func(ctx context.Context, e *event.CommandSucceededEvent) {
		if e.CommandName == "find" {
			gate.observe(ctx)
		}
	}}
	proxy.Monitor = monitor
	config := mongodb.Config{Store: "records", URI: proxy.URI()}
	adapter, err := mongodb.Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	node := startLuaRPCNode(t, adapter)
	opts := luaRPCBatchOptions{node: node, gate: gate, prefix: fixture.DB + "/records/s:", cases: cases}
	runLuaRPCBatch(t, opts)
	counts := make(map[string]int)
	for _, observed := range proxy.Events() {
		counts[observed.Command]++
		if (observed.Command == "bulkWrite" || observed.Command == "commitTransaction") && observed.Session == "" {
			t.Fatal("Lua transaction lost its native session", observed)
		}
	}
	if counts["find"] < 2 || counts["bulkWrite"] < 1 || counts["commitTransaction"] != counts["bulkWrite"] || counts["update"] != 0 {
		t.Fatal("concurrent Lua RPCs lost transactional execution", counts, gate.reads.Load())
	}
	for _, item := range cases {
		filter := bson.D{{Key: "_id", Value: item.id}}
		raw, err := collection.FindOne(t.Context(), filter).Raw()
		if err != nil {
			t.Fatal(item.id, err)
		}
		elements, err := raw.Elements()
		if err != nil || len(elements) != 3 || raw.Lookup("_id").StringValue() != item.id || raw.Lookup("marker").StringValue() != item.id || raw.Lookup("n").AsInt64() != item.wantN {
			t.Fatal("Lua caller result or document fields changed", item.id, raw, err)
		}
	}
	t.Log("16 concurrent single-record RPCs: transactional native grouping; canceled/rejected/failed/keep items isolated; exactly the original document fields persisted")
}

type luaRPCSearchObserver struct {
	gate   *luaRPCReadGate
	reads  atomic.Int32
	writes atomic.Int32
}

func luaRPCSearchProxy(t *testing.T, backend *testsearch.Backend, observer *luaRPCSearchObserver) string {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_bulk" {
			observer.writes.Add(1)
		}
		request, err := http.NewRequestWithContext(r.Context(), r.Method, backend.URL+r.URL.RequestURI(), r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		request.Header, request.GetBody = r.Header.Clone(), nil
		response, err := backend.Client.Do(request)
		if err != nil {
			if r.Context().Err() == nil {
				t.Error("owned Search backend proxy failed", err)
			}
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(response.Body, 32<<20))
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/_mget") {
			observer.reads.Add(1)
			observer.gate.observe(r.Context())
		}
		for name, values := range response.Header {
			w.Header()[name] = append([]string(nil), values...)
		}
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(raw)
	})
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)
	return proxy.URL
}

func TestRouteSearchLuaRPCsShareOCCBatchWithoutDocumentMetadata(t *testing.T) {
	if os.Getenv("WEIR_SEARCH_INTEGRATION") == "" {
		t.Fatal("Lua RPC acceptance requires an explicit owned Search fixture profile")
	}
	backend := testsearch.Open(t)
	// Fourteen Lua sources fit one pre-read chunk even at the legal 2 MiB
	// per-source bound. The hot-URI test separately keeps sixteen callers.
	cases := luaRPCIndependentCases(14)
	for _, item := range cases {
		body := fmt.Sprintf(`{"n":0,"marker":%q}`, item.id)
		code, raw := backend.Do(t, "PUT", "/"+backend.Index+"/_doc/"+item.id, body)
		if code != http.StatusCreated {
			t.Fatal("single-record fixture setup failed", item.id, code, string(raw))
		}
	}
	gate := newLuaRPCReadGate()
	defer gate.unblockAll()
	observer := &luaRPCSearchObserver{gate: gate}
	proxyURL := luaRPCSearchProxy(t, backend, observer)
	config := search.Config{Store: "records", URL: proxyURL}
	adapter, err := search.Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	node := startLuaRPCNode(t, adapter)
	opts := luaRPCBatchOptions{node: node, gate: gate, prefix: backend.Index + "/s:", cases: cases, allowCapacity: true}
	runLuaRPCBatch(t, opts)
	if observer.reads.Load() < 2 || observer.writes.Load() < 1 {
		t.Fatal("concurrent Lua RPCs lost OCC execution", observer.reads.Load(), observer.writes.Load())
	}
	for _, item := range cases {
		code, raw := backend.Do(t, "GET", "/"+backend.Index+"/_doc/"+item.id, "")
		var stored struct {
			Source map[string]json.RawMessage `json:"_source"`
		}
		if code != http.StatusOK || json.Unmarshal(raw, &stored) != nil || len(stored.Source) != 2 || string(stored.Source["n"]) != fmt.Sprint(item.wantN) || string(stored.Source["marker"]) != fmt.Sprintf("%q", item.id) {
			t.Fatal("Lua caller result or document fields changed", item.id, code, string(raw))
		}
	}
	t.Log("14 concurrent single-record RPCs: native _mget/_bulk grouping; canceled/rejected/failed/keep items isolated; exactly the original source fields persisted")
}

func luaRPCHotCases() []luaRPCCase {
	cases := make([]luaRPCCase, 16)
	for i := range cases {
		cases[i] = luaRPCCase{id: "counter", program: luaRPCIncrement(1), outcome: pb.MutationOutcome_APPLIED}
	}
	return cases
}

func TestRouteMongoLuaSameURIRPCsPreserveAppliedIncrements(t *testing.T) {
	fixture := testmongo.Open(t)
	collection := fixture.Admin.Database(fixture.DB).Collection("records")
	document := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int32(0)}, {Key: "marker", Value: "untouched"}}
	if _, err := collection.InsertOne(t.Context(), document); err != nil {
		t.Fatal(err)
	}
	gate := newLuaRPCReadGate()
	defer gate.unblockAll()
	proxy := testmongo.StartProxy(t, fixture)
	monitor := &event.CommandMonitor{Succeeded: func(ctx context.Context, e *event.CommandSucceededEvent) {
		if e.CommandName == "find" {
			gate.observe(ctx)
		}
	}}
	proxy.Monitor = monitor
	config := mongodb.Config{Store: "records", URI: proxy.URI()}
	adapter, err := mongodb.Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	node := startLuaRPCNode(t, adapter)
	cases := luaRPCHotCases()
	opts := luaRPCBatchOptions{node: node, gate: gate, prefix: fixture.DB + "/records/s:", cases: cases, allowConflicts: true}
	applied := runLuaRPCBatch(t, opts)
	filter := bson.D{{Key: "_id", Value: "counter"}}
	raw, err := collection.FindOne(t.Context(), filter).Raw()
	if err != nil {
		t.Fatal(err)
	}
	elements, err := raw.Elements()
	if err != nil || len(elements) != 3 || raw.Lookup("n").AsInt64() != int64(applied) || raw.Lookup("marker").StringValue() != "untouched" {
		t.Fatal("concurrent same-URI Lua RPCs lost updates or added metadata", raw, err)
	}
	counts := make(map[string]int)
	for _, observed := range proxy.Events() {
		counts[observed.Command]++
	}
	if counts["find"] < len(cases)+1 || counts["bulkWrite"] < applied || counts["commitTransaction"] != applied {
		t.Fatal("same-URI Lua RPCs did not use successive transaction snapshots", counts)
	}
	t.Log("16 concurrent same-URI Lua RPCs: persisted increments match APPLIED outcomes; document fields unchanged")
}

func TestRouteSearchLuaSameURIRPCsPreserveAppliedIncrements(t *testing.T) {
	if os.Getenv("WEIR_SEARCH_INTEGRATION") == "" {
		t.Fatal("Lua RPC acceptance requires an explicit owned Search fixture profile")
	}
	backend := testsearch.Open(t)
	code, raw := backend.Do(t, "PUT", "/"+backend.Index+"/_doc/counter", `{"n":0,"marker":"untouched"}`)
	if code != http.StatusCreated {
		t.Fatal(code, string(raw))
	}
	gate := newLuaRPCReadGate()
	defer gate.unblockAll()
	observer := &luaRPCSearchObserver{gate: gate}
	proxyURL := luaRPCSearchProxy(t, backend, observer)
	config := search.Config{Store: "records", URL: proxyURL}
	adapter, err := search.Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	node := startLuaRPCNode(t, adapter)
	cases := luaRPCHotCases()
	opts := luaRPCBatchOptions{node: node, gate: gate, prefix: backend.Index + "/s:", cases: cases, allowConflicts: true, allowCapacity: true}
	applied := runLuaRPCBatch(t, opts)
	code, raw = backend.Do(t, "GET", "/"+backend.Index+"/_doc/counter", "")
	var stored struct {
		Version int64                      `json:"_version"`
		Source  map[string]json.RawMessage `json:"_source"`
	}
	if code != http.StatusOK || json.Unmarshal(raw, &stored) != nil || len(stored.Source) != 2 || stored.Version != int64(applied+1) || string(stored.Source["n"]) != fmt.Sprint(applied) || string(stored.Source["marker"]) != `"untouched"` {
		t.Fatal("concurrent same-URI Lua RPCs lost updates or added metadata", code, string(raw))
	}
	if observer.reads.Load() < int32(len(cases)+1) || observer.writes.Load() < int32(applied) {
		t.Fatal("same-URI Lua RPCs did not use successive OCC snapshots", observer.reads.Load(), observer.writes.Load())
	}
	t.Log("16 concurrent same-URI Lua RPCs: native versions and persisted increments match APPLIED outcomes; source fields unchanged")
}
