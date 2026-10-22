//go:build integration

package app

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc/status"
)

type luaMemoryPath struct {
	name, source, outcome, failure string
	n                              int64
}

// This separate opt-in extends the original, unchanged tiny-source OOM probe.
// All Lua executes inside one 512MiB CLI, never in the host test driver.
func TestCLILuaMemoryPaths(t *testing.T) {
	if os.Getenv("WEIR_LUA_MEMORY_PATHS_INTEGRATION") != "1" {
		t.Skip("requires WEIR_LUA_MEMORY_PATHS_INTEGRATION=1, Docker and the local Mongo and Elasticsearch fixtures")
	}
	if os.Getenv("WEIR_INTEGRATION") != "1" || os.Getenv("WEIR_SEARCH_INTEGRATION") != "elasticsearch" || os.Getenv("WEIR_MONGO_PROFILE") == "tls" {
		t.Fatal("requires isolated plain MongoDB and Elasticsearch 8.19.22 fixtures")
	}
	binary := buildLinuxCLI(t)
	mongo, search := testmongo.Open(t), testsearch.Open(t)
	searchURL := searchContainerURL(t, search)
	paths := luaMemoryPaths()
	mongoDataset := &nativeBurstDataset{store: "mongo", root: mongo.DB + "/records", media: "application/bson", mongo: mongo, expected: make(map[string]nativeBurstRecord)}
	searchDataset := &nativeBurstDataset{store: "search", root: search.Index, media: "application/json", search: search, expected: make(map[string]nativeBurstRecord)}
	datasets := []*nativeBurstDataset{mongoDataset, searchDataset}
	for _, dataset := range datasets {
		seedNativeBurstDataset(t, dataset)
		for _, path := range paths {
			luaMemoryPathsSeed(t, dataset, "lua-path-"+path.name)
		}
	}
	mongoBackend := &Mongo{URI: containerMongoURI}
	searchBackend := &Search{URL: searchURL}
	mongoLocal := &Local{Backend: BackendConfig{MongoDB: mongoBackend}}
	searchLocal := &Local{Backend: BackendConfig{Search: searchBackend}}
	mongoStore, searchStore := StoreConfig{Name: "mongo", Local: mongoLocal}, StoreConfig{Name: "search", Local: searchLocal}
	cfg := DefaultConfig()
	cfg.Routing.Stores = []StoreConfig{mongoStore, searchStore}
	if cfg.Basic.Transport.MaxPendingRecords != 32 || time.Duration(cfg.Basic.Transport.Timeouts.Stall) != 30*time.Second {
		t.Fatal("default transport policy changed")
	}
	opts := memoryRecoveryOptions{binary: binary, capacityMiB: 512, automatic: true, config: &cfg}
	process := startMemoryRecoveryCLI(t, opts)
	t.Logf("Lua_paths fixed512MiB/2CPU single_process=true backends=MongoDB,Elasticsearch cases_per_backend=%d caller3s retries=0 Search=%s", len(paths), searchURL)
	nativeBurstSnapshot(t, process)
	initial, err := ingressMemoryMetrics(process)
	if err != nil || testmetrics.Sum(initial, "weir_memory_go_runtime_managed") != 1 {
		t.Fatal("CLI default automatic Go memory policy inactive or unknown", err)
	}
	monitorContext, stopMonitor := context.WithCancel(context.Background())
	var monitor sync.WaitGroup
	monitor.Add(2)
	go func() { defer monitor.Done(); ingressMemorySample(t, monitorContext, process, false) }()
	go func() { defer monitor.Done(); ingressMemorySample(t, monitorContext, process, true) }()
	t.Cleanup(func() {
		stopMonitor()
		monitor.Wait()
		ingressMemoryEvents(t, process, "Lua_paths_cleanup")
		ingressMemoryDocker(t, process)
	})
	client, healthClient := luaMemoryPathsClient(t, process), luaMemoryPathsClient(t, process)
	// Each SDK opens a separate channel for each Store: two SDKs, two Stores.
	const liveStoreConnections = 4
	ingressMemoryIdle(t, process, time.Now().Add(5*time.Second), liveStoreConnections)
	ingressMemoryEvents(t, process, "Lua_paths_initial")
	for _, dataset := range datasets {
		for index, path := range paths {
			id := "lua-path-" + path.name
			before := luaMemoryPathsExpected(t, dataset, id, 1)
			luaMemoryPathsOracle(t, dataset, id, before)
			program := &weirclient.LuaTransform{Source: []byte(path.source)}
			request := &weirclient.AtomicTransformRequest{Resource: dataset.root + "/s:" + id, Lua: program}
			options := weirclient.AtomicTransformOptions{StoreName: dataset.store, Request: request}
			t.Logf("Lua_path_start store=%s case=%s resource=%s source_bytes=%d source_SHA256=%x expected_outcome=%s expected_failure=%s caller3s retries=0 host_executes_Lua=false", dataset.store, path.name, request.Resource, len(path.source), sha256.Sum256([]byte(path.source)), path.outcome, path.failure)
			call := luaMemoryPathsCall(t, client, options)
			luaMemoryPathsOutcome(t, dataset.store+"/"+path.name, call, path)
			// Failures must still collect effects, same-SDK recovery and health.
			expected := luaMemoryPathsExpected(t, dataset, id, path.n)
			luaMemoryPathsOracle(t, dataset, id, expected)
			dataset.expected[id] = nativeBurstRecord{N: path.n, Pad: strings.Repeat("x", 1<<10)}
			ingressMemoryDocker(t, process)
			ingressMemoryIdle(t, process, time.Now().Add(5*time.Second), liveStoreConnections)
			luaMemoryPathsRecovery(t, client, dataset, 2000000+index)
			ingressMemoryHealth(t, healthClient, dataset)
			ingressMemoryIdle(t, process, time.Now().Add(5*time.Second), liveStoreConnections)
		}
		verifyNativeBurstOracle(t, dataset)
	}
	closeIngressMemoryClient(t, client)
	closeIngressMemoryClient(t, healthClient)
	ingressMemoryIdle(t, process, time.Now().Add(5*time.Second), 0)
	ingressMemoryEvents(t, process, "Lua_paths_final")
	nativeBurstSnapshot(t, process)
	stopMonitor()
	monitor.Wait()
	ingressMemoryDocker(t, process)
}

func luaMemoryPaths() []luaMemoryPath {
	paths := []luaMemoryPath{
		{name: "increment", source: `return function(current, incoming) current.n=current.n+1; return current end`, outcome: "APPLIED", n: 2},
		{name: "pcall-concat", source: `return function() pcall(function() local s='x'; for i=1,29 do s=s..s end end); return weir.keep() end`, outcome: "NOT_APPLIED", failure: "RESOURCE_EXHAUSTED", n: 1},
		{name: "shared-dag", source: `return function() local t={x=1}; for i=1,40 do t={a=t,b=t} end; return t end`, outcome: "NOT_APPLIED", failure: "RESOURCE_EXHAUSTED", n: 1},
		{name: "large-error", source: `return function() local s='x'; for i=1,20 do s=s..s end; error(s) end`, outcome: "NOT_APPLIED", failure: "INVALID_ARGUMENT", n: 1},
		{name: "same-key-comments", source: "--[[" + strings.Repeat("x", 40<<10) + "]]\n" + `return function() local t={n=0}; for i=1,100000 do t.n=i end; assert(t.n==100000); return weir.keep() end`, outcome: "APPLIED", n: 1},
		{name: "control-quote", source: `return function() local s=string.char(0,1,49); assert(string.format('%q',s)==[["\0\0011"]]); for i=1,16 do s=s..s end; local q=string.format('%q',s); assert(#s==196608 and #q==458754); return weir.keep() end`, outcome: "APPLIED", n: 1},
		{name: "numeric-for", source: `return function() local s='0'; for i=1,16 do s=s..s end; s=s..'1.0'; for i=1,16384 do for j=s,0 do end end; return weir.keep() end`, outcome: "NOT_APPLIED", failure: "RESOURCE_EXHAUSTED", n: 1},
		{name: "numeric-concat", source: `return function() local s='0'; for i=1,16 do s=s..s end; s=s..'1.0'; for i=1,16384 do table.concat({},'',s,0) end; return weir.keep() end`, outcome: "NOT_APPLIED", failure: "RESOURCE_EXHAUSTED", n: 1},
	}
	return paths
}

func luaMemoryPathsClient(t *testing.T, process *memoryRecoveryCLI) *weirclient.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	options := weirclient.OpenOptions{Seed: process.address, Stores: []string{"mongo", "search"}}
	client, err := weirclient.Open(ctx, options)
	if err != nil {
		t.Fatal("real dual-Store SDK discovery failed", err)
	}
	t.Cleanup(func() { closeIngressMemoryClient(t, client) })
	return client
}

func luaMemoryPathsExpected(t *testing.T, dataset *nativeBurstDataset, id string, n int64) []byte {
	t.Helper()
	pad := strings.Repeat("x", 1<<10)
	var raw []byte
	var err error
	if dataset.mongo != nil {
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: n}, {Key: "pad", Value: pad}}
		raw, err = bson.Marshal(document)
	} else {
		document := nativeBurstRecord{N: n, Pad: pad}
		raw, err = json.Marshal(document)
	}
	if err != nil {
		t.Fatal("small independent document encoding failed", err)
	}
	return raw
}

func luaMemoryPathsSeed(t *testing.T, dataset *nativeBurstDataset, id string) {
	t.Helper()
	raw := luaMemoryPathsExpected(t, dataset, id, 1)
	if dataset.mongo != nil {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		_, err := dataset.mongo.Admin.Database(dataset.mongo.DB).Collection("records").InsertOne(ctx, bson.Raw(raw))
		if err != nil {
			t.Fatal("small independent Lua-path seed failed", id, err)
		}
	} else {
		code, _ := dataset.search.Do(t, http.MethodPut, "/"+dataset.root+"/_doc/"+id, string(raw))
		if code != http.StatusCreated {
			t.Fatal("small independent Lua-path Search seed failed", id, code)
		}
	}
	dataset.expected[id] = nativeBurstRecord{N: 1, Pad: strings.Repeat("x", 1<<10)}
}

func luaMemoryPathsCall(t *testing.T, client *weirclient.Client, options weirclient.AtomicTransformOptions) luaMemoryCall {
	t.Helper()
	started := time.Now()
	deadline := started.Add(3 * time.Second)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	done := make(chan luaMemoryCall, 1)
	go func() {
		call := luaMemoryCall{started: started, deadline: deadline}
		call.mutation, call.err = client.AtomicTransform(ctx, options)
		call.finished, call.returned = time.Now(), true
		done <- call
	}()
	select {
	case call := <-done:
		return call
	case <-time.After(time.Until(deadline.Add(time.Second))):
		t.Error("Lua path caller3s plus bounded1s join exhausted")
		call := luaMemoryCall{started: started, deadline: deadline, err: fmt.Errorf("no acknowledgement within original3s plus bounded1s join")}
		return call
	}
}

func luaMemoryPathsOutcome(t *testing.T, label string, call luaMemoryCall, expected luaMemoryPath) {
	t.Helper()
	outcome, failure, message := "UNCONFIRMED", "", ""
	if call.mutation != nil {
		outcome = call.mutation.Outcome.String()
		if call.mutation.Failure != nil {
			failure, message = call.mutation.Failure.Code.String(), call.mutation.Failure.Message
		}
	}
	timely := call.returned && !call.finished.IsZero() && !call.finished.After(call.deadline)
	t.Logf("Lua_path_outcome case=%s outcome=%s failure=%s RPC=%s error=%v returned=%t within_original_deadline=%t started=%s finished=%s deadline=%s elapsed=%s failure_message_bytes=%d failure_message_SHA256=%x", label, outcome, failure, status.Code(call.err), call.err, call.returned, timely, call.started.UTC().Format(time.RFC3339Nano), call.finished.UTC().Format(time.RFC3339Nano), call.deadline.UTC().Format(time.RFC3339Nano), call.finished.Sub(call.started), len(message), sha256.Sum256([]byte(message)))
	if !timely || call.err != nil || outcome != expected.outcome || failure != expected.failure || len(message) > 4<<10 {
		t.Error("Lua path lacked exact bounded business outcome or bounded diagnostic", label, outcome, failure, call.err, timely, len(message))
	}
}

func luaMemoryPathsRaw(dataset *nativeBurstDataset, id string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if dataset.mongo != nil {
		filter := bson.D{{Key: "_id", Value: id}}
		return dataset.mongo.Admin.Database(dataset.mongo.DB).Collection("records").FindOne(ctx, filter).Raw()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, dataset.search.URL+"/"+dataset.root+"/_doc/"+id, nil)
	if err != nil {
		return nil, err
	}
	request.GetBody = nil
	response, err := dataset.search.Client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, (4<<10)+1))
	if err != nil {
		return nil, err
	}
	var document struct {
		ID     string          `json:"_id"`
		Found  bool            `json:"found"`
		Source json.RawMessage `json:"_source"`
	}
	if response.StatusCode != http.StatusOK || len(raw) > 4<<10 || json.Unmarshal(raw, &document) != nil || !document.Found || document.ID != id || len(document.Source) == 0 {
		return nil, fmt.Errorf("independent Search source missing or invalid: status=%d response_bytes=%d", response.StatusCode, len(raw))
	}
	return document.Source, nil
}

func luaMemoryPathsOracle(t *testing.T, dataset *nativeBurstDataset, id string, expected []byte) {
	t.Helper()
	raw, err := luaMemoryPathsRaw(dataset, id)
	valid := err == nil && len(raw) == len(expected) && sha256.Sum256(raw) == sha256.Sum256(expected)
	t.Logf("Lua_path_independent_effect store=%s ID=%s bytes=%d expected_bytes=%d SHA256=%x expected_SHA256=%x full_raw_equal=%t error=%v", dataset.store, id, len(raw), len(expected), sha256.Sum256(raw), sha256.Sum256(expected), valid, err)
	if !valid {
		t.Error("Lua path independent full raw oracle differs or is unknown", dataset.store, id, err)
	}
}

func luaMemoryPathsRecovery(t *testing.T, client *weirclient.Client, dataset *nativeBurstDataset, sequence int) {
	t.Helper()
	job := nativeBurstResult{kind: 1, sequence: sequence}
	id := fmt.Sprintf("write-%d", sequence)
	expected := luaMemoryPathsExpected(t, dataset, id, int64(sequence+1000))
	started := time.Now()
	deadline := started.Add(3 * time.Second)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	done := make(chan nativeBurstResult, 1)
	go func() {
		job.err = executeNativeBurstCall(ctx, client, dataset, job)
		putErr := job.err
		request := &weirclient.ReadRequest{Resource: dataset.root + "/s:" + id}
		options := weirclient.ReadOneOptions{StoreName: dataset.store, Request: request}
		read, err := client.ReadOne(ctx, options)
		if err != nil || read == nil || read.Failure != nil || read.Document == nil || read.Document.ContentType != dataset.media || len(read.Document.Data) != len(expected) || sha256.Sum256(read.Document.Data) != sha256.Sum256(expected) {
			job.err = fmt.Errorf("same-SDK recovery Read invalid or failed: Put=%v Read=%v", putErr, err)
		}
		job.elapsed = time.Since(started)
		done <- job
	}()
	select {
	case result := <-done:
		t.Logf("Lua_path_same_SDK_recovery store=%s ID=%s client=%p combined_Put_Read_caller3s elapsed=%s error=%v full_raw_SHA256=%x TCP_connection_identity=not_observed", dataset.store, id, client, result.elapsed, result.err, sha256.Sum256(expected))
		if result.err != nil || result.elapsed > 3*time.Second {
			t.Error("Lua path same-SDK fresh Put/Read recovery failed", dataset.store, result.err, result.elapsed)
		}
	case <-time.After(time.Until(deadline.Add(time.Second))):
		t.Error("Lua path same-SDK recovery caller3s plus bounded1s join exhausted", dataset.store)
	}
	cancel()
	luaMemoryPathsOracle(t, dataset, id, expected)
	dataset.expected[id] = nativeBurstRecord{N: int64(sequence + 1000), Pad: strings.Repeat("x", 1<<10)}
}
