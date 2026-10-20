//go:build integration

package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type processFaultBackend struct {
	store, root, media string
	mongo              *testmongo.Fixture
	mongoProxy         *testmongo.Proxy
	search             *testsearch.Backend
	searchProxy        *processFaultHTTPProxy
}

type processFaultSuite struct {
	process        *memoryRecoveryCLI
	client         *weirclient.Client
	pid            string
	nativeShutdown func(*testing.T, *processFaultGate)
	nativePID      func(*testing.T) string
}

type processFaultGate struct {
	before, after chan struct{}
	started, ack  chan struct{}
	release       sync.Once
}

type processFaultCounts struct {
	writes, acks, drops int
}

type processFaultObservationOffset struct {
	events, commands int
}

// This suite owns its proxies, database names, indexes, and CLI container. It
// never stops a database or installs a server-global failpoint. Run separately
// from the sustained-load qualification so injected faults do not distort it.
func TestProductionProcessFaultQualification(t *testing.T) {
	if os.Getenv("WEIR_PRODUCTION_FAULT_QUALIFICATION") != "1" {
		t.Skip("requires explicit owned Linux CLI fault qualification")
	}
	execution := loadProductionExecution(t, "fault")
	profile := execution.Profile()
	binary, digest := execution.CLIPath(), execution.CLISHA256()
	adapter := inspectLegacyDesktop(t, execution)
	mongo, search := testmongo.Open(t), testsearch.Open(t)
	verifyLegacyMongoVersion(t, mongo)
	t.Logf("actual backend Elasticsearch=%s container=%s", search.Profile, adapter.searchID)
	proxyOptions := testmongo.ProxyOptions{Fixture: mongo, ListenAddress: "0.0.0.0:0"}
	mongoProxy := testmongo.StartProxyOn(t, proxyOptions)
	mongoURI, err := url.Parse(mongoProxy.URI())
	if err != nil {
		t.Fatal(err)
	}
	mongoURI.Host = adapter.proxyHost(mongoURI.Host)
	searchProxy := startProcessFaultHTTPProxy(t, search.URL)
	mongoConnection := &Mongo{URI: mongoURI.String()}
	searchConnection := &Search{URL: adapter.proxyURL(searchProxy)}
	mongoLocal := &Local{Backend: BackendConfig{MongoDB: mongoConnection}}
	searchLocal := &Local{Backend: BackendConfig{Search: searchConnection}}
	mongoStore, searchStore := StoreConfig{Name: "mongo", Local: mongoLocal}, StoreConfig{Name: "search", Local: searchLocal}
	cfg := DefaultConfig()
	cfg.Routing.Stores = []StoreConfig{mongoStore, searchStore}
	opts := memoryRecoveryOptions{binary: binary, capacityMiB: 512, automatic: true, config: &cfg, execution: execution}
	process := startMemoryRecoveryCLI(t, opts)
	version := packagedDocker(t, "exec", process.name, "/weir", "version")
	if err := execution.CheckVersion([]byte(version)); err != nil {
		t.Fatal("CLI version cross-check failed", err)
	}
	openContext, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	openOptions := weirclient.OpenOptions{Seed: process.address, Stores: []string{"mongo", "search"}}
	client, err := weirclient.Open(openContext, openOptions)
	cancel()
	if err != nil {
		t.Fatal("real SDK Resolve failed", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	suite := processFaultSuite{process: process, client: client, pid: processFaultPID(t, process)}
	backends := []processFaultBackend{
		{store: "mongo", root: mongo.DB + "/records", media: "application/bson", mongo: mongo, mongoProxy: mongoProxy},
		{store: "search", root: search.Index, media: "application/json", search: search, searchProxy: searchProxy},
	}
	t.Logf("qualification=false profile=%s intent=%s source=%s SHA256=%s version=%s container=%s PID=%s capacity=512MiB CPUs=2 default_store_execution=16 default_stall=30s", profile.ID, profile.Intent, execution.ServerSource().Revision, digest, strings.TrimSpace(version), process.name, suite.pid)
	t.Log("predeclared gates: healthy Read/Native <=3s during one held IO; cancellation and all resource credits <=3s; proxy recovery and same-SDK CRUD/Native <=5s; ack-loss native=INCOMPLETE, typed=UNKNOWN, oracle increment/version exactly one; fault SIGTERM <=8s exit=0 no OOM; no forced GC or database failpoints")
	for _, backend := range backends {
		if !t.Run(backend.store, func(t *testing.T) {
			processFaultHealthy(t, &suite, backend)
			processFaultLostAck(t, &suite, backend, false)
			processFaultLostAck(t, &suite, backend, true)
			processFaultCancelAfterAck(t, &suite, backend)
			processFaultSlowIO(t, &suite, backend)
			processFaultUnavailable(t, &suite, backend)
		}) {
			return
		}
	}
	processFaultShutdown(t, &suite, backends[0])
}

func processFaultPID(t *testing.T, process *memoryRecoveryCLI) string {
	t.Helper()
	pid := strings.TrimSpace(packagedDocker(t, "inspect", "--type", "container", "--format", "{{.State.Pid}}", process.name))
	if pid == "" || pid == "0" {
		t.Fatal("service process is not running", pid)
	}
	return pid
}

func processFaultSeed(t *testing.T, backend processFaultBackend, id string) int64 {
	t.Helper()
	if backend.mongo != nil {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int64(0)}}
		if _, err := backend.mongo.Admin.Database(backend.mongo.DB).Collection("records").InsertOne(ctx, document); err != nil {
			t.Fatal(err)
		}
		return 0
	}
	code, raw := backend.search.Do(t, http.MethodPut, "/"+backend.root+"/_doc/"+id, `{"n":0}`)
	var result struct {
		Version int64 `json:"_version"`
	}
	if code != http.StatusCreated || json.Unmarshal(raw, &result) != nil || result.Version != 1 {
		t.Fatal("independent Search seed failed", code, string(raw))
	}
	return result.Version
}

func processFaultOracle(t *testing.T, backend processFaultBackend, id string) (int64, int64) {
	t.Helper()
	if backend.mongo != nil {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		filter := bson.D{{Key: "_id", Value: id}}
		var result struct {
			N int64 `bson:"n"`
		}
		err := backend.mongo.Admin.Database(backend.mongo.DB).Collection("records").FindOne(ctx, filter).Decode(&result)
		if err != nil {
			t.Fatal("independent Mongo oracle failed", err)
		}
		return result.N, 0
	}
	code, raw := backend.search.Do(t, http.MethodGet, "/"+backend.root+"/_doc/"+id, "")
	var result struct {
		Version int64 `json:"_version"`
		Source  struct {
			N int64 `json:"n"`
		} `json:"_source"`
	}
	if code != http.StatusOK || json.Unmarshal(raw, &result) != nil {
		t.Fatal("independent Search oracle failed", code, string(raw))
	}
	return result.Source.N, result.Version
}

func processFaultNative(backend processFaultBackend, id string, increment bool) (*pb.NativeRequest, error) {
	if backend.mongo != nil {
		filter := bson.D{{Key: "_id", Value: id}}
		command := bson.D{{Key: "find", Value: "records"}, {Key: "filter", Value: filter}, {Key: "limit", Value: 1}, {Key: "singleBatch", Value: true}}
		if increment {
			amount := bson.D{{Key: "n", Value: int64(1)}}
			update := bson.D{{Key: "$inc", Value: amount}}
			command = bson.D{{Key: "findAndModify", Value: "records"}, {Key: "query", Value: filter}, {Key: "update", Value: update}, {Key: "new", Value: true}}
		}
		raw, err := bson.Marshal(command)
		if err != nil {
			return nil, err
		}
		document := &pb.Document{ContentType: backend.media, Data: raw}
		request := &pb.NativeRequest{Resource: backend.mongo.DB, Request: document}
		return request, nil
	}
	method, path, body := http.MethodGet, "/"+backend.root+"/_doc/"+id, ""
	if increment {
		method, path = http.MethodPost, "/"+backend.root+"/_update/"+id
		body = `{"script":{"source":"ctx._source.n += params.amount","params":{"amount":1}}}`
	}
	request, err := http.NewRequest(method, "http://weir.invalid"+path, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	return weirclient.NewHTTPNativeRequest("", request)
}

func processFaultRead(ctx context.Context, client *weirclient.Client, backend processFaultBackend, id string) error {
	request := &weirclient.ReadRequest{Resource: backend.root + "/s:" + id}
	opts := weirclient.ReadOneOptions{StoreName: backend.store, Request: request}
	result, err := client.ReadOne(ctx, opts)
	if err != nil {
		return err
	}
	if result == nil || result.Failure != nil || result.Document == nil {
		return fmt.Errorf("point read did not return the seeded document: %v", result)
	}
	var value struct {
		N int64 `json:"n" bson:"n"`
	}
	if backend.mongo != nil {
		err = bson.Unmarshal(result.Document.Data, &value)
	} else {
		err = json.Unmarshal(result.Document.Data, &value)
	}
	if err != nil || value.N != 0 {
		return fmt.Errorf("point read lost seeded value: n=%d error=%v", value.N, err)
	}
	return nil
}

func processFaultNativeRead(ctx context.Context, client *weirclient.Client, backend processFaultBackend, id string) error {
	request, err := processFaultNative(backend, id, false)
	if err != nil {
		return err
	}
	var body []byte
	consume := func(_ context.Context, _ *weirclient.NativeResponse, chunk []byte) error {
		if len(body)+len(chunk) > 64<<10 {
			return fmt.Errorf("small health Native exceeded its finite bound")
		}
		body = append(body, chunk...)
		return nil
	}
	opts := weirclient.NativeOptions{StoreName: backend.store, Request: request, Consume: consume}
	result, err := client.Native(ctx, opts)
	if err != nil {
		return err
	}
	if result == nil || result.Failure != nil || result.Completion != weirclient.NativeResponseComplete {
		return fmt.Errorf("health Native incomplete: %v", result)
	}
	if backend.mongo != nil {
		var reply struct {
			OK     float64 `bson:"ok"`
			Cursor struct {
				Batch []struct {
					N int64 `bson:"n"`
				} `bson:"firstBatch"`
			} `bson:"cursor"`
		}
		if bson.Unmarshal(body, &reply) != nil || reply.OK != 1 || len(reply.Cursor.Batch) != 1 || reply.Cursor.Batch[0].N != 0 {
			return fmt.Errorf("health Native Mongo content mismatch")
		}
		return nil
	}
	metadata, err := weirclient.ParseHTTPNativeResponse(result.Response)
	var reply struct {
		Found  bool `json:"found"`
		Source struct {
			N int64 `json:"n"`
		} `json:"_source"`
	}
	if err != nil || metadata.StatusCode != http.StatusOK || json.Unmarshal(body, &reply) != nil || !reply.Found || reply.Source.N != 0 {
		return fmt.Errorf("health Native Search status/content mismatch: %v", err)
	}
	return nil
}

func processFaultHealthy(t *testing.T, suite *processFaultSuite, backend processFaultBackend) {
	t.Helper()
	id := "health-" + backend.store
	processFaultSeed(t, backend, id)
	for _, kind := range []string{"Read", "Native"} {
		start := time.Now()
		ctx, cancel := context.WithDeadline(t.Context(), start.Add(3*time.Second))
		var err error
		if kind == "Read" {
			err = processFaultRead(ctx, suite.client, backend, id)
		} else {
			err = processFaultNativeRead(ctx, suite.client, backend, id)
		}
		elapsed := time.Since(start)
		cancel()
		if err != nil || elapsed > 3*time.Second {
			t.Fatal("baseline failed or exceeded 3s", backend.store, kind, elapsed, err)
		}
		t.Logf("baseline backend=%s method=%s elapsed=%s", backend.store, kind, elapsed)
	}
	processFaultCRUD(t, suite, backend, "baseline-crud")
	processFaultIdle(t, suite.process, 3*time.Second)
}

func processFaultArm(backend processFaultBackend, id, mode string) *processFaultGate {
	gate := &processFaultGate{started: make(chan struct{}, 1), ack: make(chan struct{}, 1)}
	if mode == "slow" {
		gate.before = make(chan struct{})
	}
	if mode == "cancel" || mode == "shutdown" {
		gate.after = make(chan struct{})
	}
	if backend.mongo != nil {
		command := "findAndModify"
		match := []byte(id)
		if mode == "slow" {
			command = "find"
		}
		if mode == "typed" {
			command = "bulkWrite"
		}
		fault := testmongo.ProxyFault{Command: command, Match: match, Before: gate.before, After: gate.after, Started: gate.started, Acknowledged: gate.ack, Drop: mode != "slow"}
		backend.mongoProxy.ArmFault(fault)
	} else {
		path := "/" + backend.root + "/_update/" + id
		if mode == "slow" {
			path = "/" + backend.root + "/_doc/" + id
		}
		if mode == "typed" {
			path = "/_bulk"
		}
		fault := processFaultHTTPFault{path: path, match: []byte(id), before: gate.before, after: gate.after, started: gate.started, ack: gate.ack, drop: mode != "slow"}
		backend.searchProxy.arm(fault)
	}
	return gate
}

func (gate *processFaultGate) close() {
	gate.release.Do(func() {
		if gate.before != nil {
			close(gate.before)
		}
		if gate.after != nil {
			close(gate.after)
		}
	})
}

func processFaultWait(t *testing.T, observed <-chan struct{}, boundary string) {
	t.Helper()
	select {
	case <-observed:
	case <-time.After(3 * time.Second):
		t.Fatal("owned proxy did not reach expected boundary", boundary)
	}
}

func processFaultCountsFor(backend processFaultBackend, offset processFaultObservationOffset, typed bool) processFaultCounts {
	counts := processFaultCounts{}
	if backend.mongo != nil {
		for _, command := range backend.mongoProxy.ForwardedCommands()[offset.commands:] {
			if command == "findAndModify" || command == "bulkWrite" {
				counts.writes++
			}
		}
		for _, event := range backend.mongoProxy.Events()[offset.events:] {
			if event.Command != "findAndModify" && event.Command != "bulkWrite" && event.Command != "commitTransaction" {
				continue
			}
			if event.Acknowledged && (event.Command == "findAndModify" || event.Command == "bulkWrite" && typed) {
				counts.acks++
			}
			if event.Dropped {
				counts.drops++
			}
		}
		return counts
	}
	for _, event := range backend.searchProxy.snapshot()[offset.events:] {
		if !event.mutation {
			continue
		}
		counts.writes++
		if event.acknowledged {
			counts.acks++
		}
		if event.dropped {
			counts.drops++
		}
	}
	return counts
}

func processFaultOffset(backend processFaultBackend) processFaultObservationOffset {
	if backend.mongo != nil {
		offset := processFaultObservationOffset{events: len(backend.mongoProxy.Events()), commands: len(backend.mongoProxy.ForwardedCommands())}
		return offset
	}
	offset := processFaultObservationOffset{events: len(backend.searchProxy.snapshot())}
	return offset
}

func processFaultLostAck(t *testing.T, suite *processFaultSuite, backend processFaultBackend, typed bool) {
	t.Helper()
	mode := "native"
	if typed {
		mode = "typed"
	}
	id := "lost-" + mode
	version := processFaultSeed(t, backend, id)
	offset := processFaultOffset(backend)
	gate := processFaultArm(backend, id, mode)
	defer gate.close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	start := time.Now()
	if typed {
		lua := &weirclient.LuaTransform{Source: []byte(`return function(current, incoming) current.n = current.n + 1; return current end`)}
		request := &weirclient.AtomicTransformRequest{Resource: backend.root + "/s:" + id, Lua: lua}
		if backend.mongo != nil {
			amount := bson.D{{Key: "n", Value: int64(1)}}
			update := bson.D{{Key: "$inc", Value: amount}}
			raw, err := bson.Marshal(update)
			if err != nil {
				t.Fatal(err)
			}
			// A native MongoDB update expression has a single write boundary.
			// Lua instead commits a transaction, whose same-transaction commit
			// retries can resolve a dropped commit ACK without replaying Lua.
			request.Lua = nil
			request.BackendExpression = &pb.Document{ContentType: "application/vnd.weir.mongodb-update.v1+bson", Data: raw}
		}
		opts := weirclient.AtomicTransformOptions{StoreName: backend.store, Request: request}
		result, err := suite.client.AtomicTransform(ctx, opts)
		if err != nil || result == nil || result.Outcome != weirclient.MutationUnknown {
			t.Fatal("acknowledged typed increment must be UNKNOWN", result, err)
		}
	} else {
		request, err := processFaultNative(backend, id, true)
		if err != nil {
			t.Fatal(err)
		}
		consume := func(context.Context, *weirclient.NativeResponse, []byte) error { return nil }
		opts := weirclient.NativeOptions{StoreName: backend.store, Request: request, Consume: consume}
		result, err := suite.client.Native(ctx, opts)
		if err != nil || result == nil || result.Completion != weirclient.NativeResponseIncomplete || result.Failure == nil {
			t.Fatal("acknowledged Native increment lost reply must be INCOMPLETE", result, err)
		}
	}
	processFaultWait(t, gate.ack, "real backend ack before drop")
	// Wait beyond an ordinary batch interval before the independent replay
	// observation. The SDK and service are never re-invoked for this mutation.
	time.Sleep(200 * time.Millisecond)
	n, actualVersion := processFaultOracle(t, backend, id)
	counts := processFaultCountsFor(backend, offset, typed)
	if n != 1 || backend.search != nil && actualVersion != version+1 || counts.writes != 1 || counts.acks != 1 || counts.drops != 1 {
		t.Fatal("non-idempotent mutation replay or missing ack-loss evidence", n, actualVersion, version, counts)
	}
	processFaultIdle(t, suite.process, 3*time.Second)
	t.Logf("ack_loss backend=%s method=%s elapsed=%s oracle_n=%d version_before=%d version_after=%d writes=%d acks=%d drops=%d no_replay=true", backend.store, mode, time.Since(start), n, version, actualVersion, counts.writes, counts.acks, counts.drops)
}

func processFaultCancelAfterAck(t *testing.T, suite *processFaultSuite, backend processFaultBackend) {
	t.Helper()
	id := "cancel-after-ack"
	version := processFaultSeed(t, backend, id)
	offset := processFaultOffset(backend)
	gate := processFaultArm(backend, id, "cancel")
	defer gate.close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	request, err := processFaultNative(backend, id, true)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		consume := func(context.Context, *weirclient.NativeResponse, []byte) error { return nil }
		opts := weirclient.NativeOptions{StoreName: backend.store, Request: request, Consume: consume}
		_, err := suite.client.Native(ctx, opts)
		done <- err
	}()
	processFaultWait(t, gate.ack, "ack held before caller cancellation")
	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if status.Code(err) != codes.Canceled && status.Code(err) != codes.DeadlineExceeded && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("cancelled caller must observe cancellation, not a mutation receipt", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("caller cancellation exceeded 3s")
	}
	processFaultIdle(t, suite.process, max(0, 3*time.Second-time.Since(start)))
	gate.close()
	time.Sleep(200 * time.Millisecond)
	n, actualVersion := processFaultOracle(t, backend, id)
	counts := processFaultCountsFor(backend, offset, false)
	if n != 1 || backend.search != nil && actualVersion != version+1 || counts.writes != 1 || counts.acks != 1 || counts.drops != 1 {
		t.Fatal("cancel-after-ack oracle or replay mismatch", n, actualVersion, counts)
	}
	t.Logf("cancel_after_ack backend=%s elapsed=%s caller=Canceled oracle_n=%d version=%d writes=%d acks=%d drops=%d all_credits=0", backend.store, time.Since(start), n, actualVersion, counts.writes, counts.acks, counts.drops)
}

func processFaultSlowIO(t *testing.T, suite *processFaultSuite, backend processFaultBackend) {
	t.Helper()
	id := "slow-io"
	processFaultSeed(t, backend, id)
	gate := processFaultArm(backend, id, "slow")
	defer gate.close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- processFaultNativeRead(ctx, suite.client, backend, id) }()
	processFaultWait(t, gate.started, "single Native before forwarding to real backend")
	metrics := suite.process.sample(t)
	if testmetrics.Sum(metrics, "weir_store_active_executions") < 1 || testmetrics.Sum(metrics, "weir_memory_latched") != 0 {
		t.Fatal("one real blocked IO must have active credit without memory pressure")
	}
	for _, kind := range []string{"Read", "Native"} {
		start := time.Now()
		health, stop := context.WithDeadline(t.Context(), start.Add(3*time.Second))
		var err error
		if kind == "Read" {
			err = processFaultRead(health, suite.client, backend, "health-"+backend.store)
		} else {
			err = processFaultNativeRead(health, suite.client, backend, "health-"+backend.store)
		}
		elapsed := time.Since(start)
		stop()
		if err != nil || elapsed > 3*time.Second {
			t.Fatal("single slow IO starved healthy work or exceeded 3s", backend.store, kind, elapsed, err)
		}
		t.Logf("one_held_IO backend=%s healthy_method=%s elapsed=%s active=%.0f RSS=%.0f WS=%.0f latch=0", backend.store, kind, elapsed, testmetrics.Sum(metrics, "weir_store_active_executions"), testmetrics.Sum(metrics, "process_resident_memory_bytes"), testmetrics.Sum(metrics, "weir_memory_cgroup_working_set_bytes"))
	}
	select {
	case <-gate.ack:
		t.Fatal("observer gate expired before healthy isolation observation completed")
	default:
	}
	cancelled := time.Now()
	cancel()
	select {
	case err := <-done:
		if status.Code(err) != codes.Canceled && !errors.Is(err, context.Canceled) {
			t.Fatal("held IO cancellation", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("held IO caller did not cancel")
	}
	processFaultIdle(t, suite.process, max(0, 3*time.Second-time.Since(cancelled)))
	gate.close()
	t.Logf("held_IO_cancel backend=%s elapsed=%s all_credits=0 observer_gate_released=true", backend.store, time.Since(cancelled))
}

func processFaultUnavailable(t *testing.T, suite *processFaultSuite, backend processFaultBackend) {
	t.Helper()
	if backend.mongo != nil {
		backend.mongoProxy.SetUnavailable(true)
		defer backend.mongoProxy.SetUnavailable(false)
	} else {
		backend.searchProxy.setUnavailable(true)
		defer backend.searchProxy.setUnavailable(false)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	err := processFaultRead(ctx, suite.client, backend, "health-"+backend.store)
	cancel()
	if err == nil {
		t.Fatal("owned proxy unavailable phase unexpectedly reached the backend")
	}
	t.Logf("proxy_unavailable backend=%s caller_error=%s backend_continues=true", backend.store, err)
	if backend.mongo != nil {
		backend.mongoProxy.SetUnavailable(false)
	} else {
		backend.searchProxy.setUnavailable(false)
	}
	start := time.Now()
	// This bounded fixture recovery observation issues only safe reads until
	// connectivity recovers; it does not replay an ambiguous mutation.
	deadline := start.Add(5 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(t.Context(), min(500*time.Millisecond, time.Until(deadline)))
		err = processFaultRead(ctx, suite.client, backend, "health-"+backend.store)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("same client failed fixed 5s fixture recovery gate", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	processFaultCRUD(t, suite, backend, "recovered-crud")
	ctx, cancel = context.WithDeadline(t.Context(), deadline)
	err = processFaultNativeRead(ctx, suite.client, backend, "health-"+backend.store)
	cancel()
	if err != nil || time.Now().After(deadline) {
		t.Fatal("recovered CRUD/Native exceeded fixed 5s fixture recovery gate", err)
	}
	actualPID := ""
	if suite.nativePID != nil {
		actualPID = suite.nativePID(t)
	} else {
		actualPID = processFaultPID(t, suite.process)
	}
	if actualPID != suite.pid {
		t.Fatal("recovery replaced the service process", suite.pid, actualPID)
	}
	processFaultIdle(t, suite.process, 3*time.Second)
	t.Logf("proxy_recovered backend=%s elapsed=%s same_PID=%s same_SDK=true healthy_CRUD_and_Native=true", backend.store, time.Since(start), suite.pid)
}

func processFaultCRUD(t *testing.T, suite *processFaultSuite, backend processFaultBackend, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	raw := []byte(`{"n":0}`)
	if backend.mongo != nil {
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int64(0)}}
		var err error
		raw, err = bson.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
	}
	document := &pb.Document{ContentType: backend.media, Data: raw}
	write := &weirclient.WriteRequest{Resource: backend.root + "/s:" + id, Document: document}
	writeOptions := weirclient.WriteOptions{StoreName: backend.store, Request: write}
	result, err := suite.client.Put(ctx, writeOptions)
	if err != nil || result == nil || result.Outcome != weirclient.MutationApplied || result.Failure != nil {
		t.Fatal("healthy CRUD write", result, err)
	}
	err = processFaultRead(ctx, suite.client, backend, id)
	if err != nil {
		t.Fatal("healthy CRUD read", err)
	}
	n, _ := processFaultOracle(t, backend, id)
	if n != 0 {
		t.Fatal("healthy CRUD independent oracle", n)
	}
	deleteRequest := &weirclient.DeleteRequest{Resource: write.Resource}
	deleteOptions := weirclient.DeleteOptions{StoreName: backend.store, Request: deleteRequest}
	result, err = suite.client.Delete(ctx, deleteOptions)
	if err != nil || result == nil || result.Outcome != weirclient.MutationApplied || result.Failure != nil {
		t.Fatal("healthy CRUD delete", result, err)
	}
	read := &weirclient.ReadRequest{Resource: write.Resource}
	readOptions := weirclient.ReadOneOptions{StoreName: backend.store, Request: read}
	missing, err := suite.client.ReadOne(ctx, readOptions)
	if err != nil || missing == nil || missing.Failure != nil || missing.Document != nil {
		t.Fatal("healthy CRUD deleted record still present", missing, err)
	}
	if backend.mongo != nil {
		filter := bson.D{{Key: "_id", Value: id}}
		count, err := backend.mongo.Admin.Database(backend.mongo.DB).Collection("records").CountDocuments(ctx, filter)
		if err != nil || count != 0 {
			t.Fatal("independent MongoDB delete oracle", count, err)
		}
	} else {
		code, _ := backend.search.Do(t, http.MethodGet, "/"+backend.root+"/_doc/"+id, "")
		if code != http.StatusNotFound {
			t.Fatal("independent Search delete oracle", code)
		}
	}
}

func processFaultIdle(t *testing.T, process *memoryRecoveryCLI, bound time.Duration) {
	t.Helper()
	start := time.Now()
	for {
		metrics := process.sample(t)
		nonzero := make(map[string]float64)
		for _, name := range []string{"weir_store_active_executions", "weir_store_paused_native_outputs", "weir_store_paused_native_reserved_bytes", "weir_store_pending_entries", "weir_store_pending_reserved_bytes", "weir_store_result_reserved_entries", "weir_store_result_reserved_bytes", "weir_store_retained_result_reserved_bytes", "weir_store_working_reserved_bytes", "weir_store_publishers", "weir_ingress_sessions", "weir_ingress_queued_response_bytes", "weir_memory_latched", "weir_execution_memory_input_reserved_bytes", "weir_execution_memory_input_used_bytes", "weir_execution_memory_wire_bytes", "weir_execution_memory_wire_buffers", "weir_execution_memory_leases", "weir_execution_memory_native_leases", "weir_execution_memory_starting", "weir_execution_memory_settled", "weir_execution_memory_borrowed", "weir_execution_memory_waiting", "weir_execution_memory_terminal", "weir_execution_memory_ingress_bytes", "weir_execution_memory_ingress_reserved_bytes", "weir_execution_memory_ingress_buffers", "weir_execution_memory_ingress_messages", "weir_execution_memory_ingress_waiting"} {
			if metrics[name] == nil {
				t.Fatal("missing cleanup gauge", name)
			}
			if value := testmetrics.Sum(metrics, name); value != 0 {
				nonzero[name] = value
			}
		}
		// This suite keeps its two Store SDK channels alive across every fault.
		connectionBytes := float64(2 * qualificationIngressConnectionBytes)
		for name, want := range map[string]float64{
			"weir_execution_memory_ingress_connections":      2,
			"weir_execution_memory_ingress_connection_bytes": connectionBytes,
			"weir_execution_memory_used_bytes":               connectionBytes,
			"weir_execution_memory_resident_bytes":           connectionBytes,
		} {
			if metrics[name] == nil {
				t.Fatal("missing retained-connection cleanup gauge", name)
			}
			if value := testmetrics.Sum(metrics, name); value != want {
				nonzero[name] = value
			}
		}
		if time.Since(start) >= bound {
			t.Fatal("credits failed fixed cleanup bound without forced GC", time.Since(start), nonzero)
		}
		if len(nonzero) == 0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func processFaultShutdown(t *testing.T, suite *processFaultSuite, backend processFaultBackend) {
	t.Helper()
	id := "shutdown-after-ack"
	processFaultSeed(t, backend, id)
	gate := processFaultArm(backend, id, "shutdown")
	defer gate.close()
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	request, err := processFaultNative(backend, id, true)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		consume := func(context.Context, *weirclient.NativeResponse, []byte) error { return nil }
		opts := weirclient.NativeOptions{StoreName: backend.store, Request: request, Consume: consume}
		_, err := suite.client.Native(ctx, opts)
		done <- err
	}()
	processFaultWait(t, gate.ack, "ack lost while service receives SIGTERM")
	select {
	case err := <-done:
		t.Fatal("fault caller retired before SIGTERM", err)
	default:
	}
	start := time.Now()
	if suite.nativeShutdown != nil {
		suite.nativeShutdown(t, gate)
	} else {
		packagedDocker(t, "kill", "--signal=TERM", suite.process.name)
		// Release the observer after SIGTERM so the service drains a real failed IO
		// rather than waiting for this test's own callback timeout.
		gate.close()
		waitContext, stopWait := context.WithDeadline(t.Context(), start.Add(8*time.Second))
		waitRaw, waitErr := exec.CommandContext(waitContext, "docker", "wait", suite.process.name).CombinedOutput()
		stopWait()
		state := strings.Fields(string(waitRaw))
		if waitErr != nil || len(state) != 1 || state[0] != "0" || time.Since(start) > 8*time.Second {
			t.Fatal("fault shutdown exceeded 8s or failed", state, waitErr, time.Since(start))
		}
		oom := strings.TrimSpace(packagedDocker(t, "inspect", "--type", "container", "--format", "{{.State.OOMKilled}}", suite.process.name))
		if oom != "false" {
			t.Fatal("fault shutdown OOM", oom)
		}
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fault caller did not retire after service shutdown")
	}
	n, _ := processFaultOracle(t, backend, id)
	if n != 1 {
		t.Fatal("shutdown mutation oracle", n)
	}
	deadline := time.Now().Add(time.Second)
	for {
		sockets, _ := backend.mongoProxy.Sockets()
		if sockets == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("observer-owned sockets outlived fault shutdown", sockets)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("fault_SIGTERM elapsed=%s exit=0 OOM=false oracle_n=1 observer_sockets=0; owned container removal verified by fixture cleanup", time.Since(start))
}

type processFaultHTTPFault struct {
	path          string
	match         []byte
	before, after <-chan struct{}
	started, ack  chan<- struct{}
	drop          bool
}

type processFaultHTTPEvent struct {
	path         string
	status       int
	mutation     bool
	acknowledged bool
	dropped      bool
	digest       [32]byte
}

type processFaultHTTPProxy struct {
	listener net.Listener
	server   *http.Server
	client   *http.Client
	upstream string
	done     chan struct{}
	mu       sync.Mutex
	fault    *processFaultHTTPFault
	events   []processFaultHTTPEvent
	conns    map[net.Conn]struct{}
	offline  bool
}

// The fixture must not manufacture EOF inside the shorter business gates.
const processFaultProxyTimeout = 20 * time.Second

func startProcessFaultHTTPProxy(t *testing.T, upstream string) *processFaultHTTPProxy {
	t.Helper()
	return startProcessFaultHTTPProxyOn(t, upstream, "0.0.0.0:0")
}

func startProcessFaultHTTPProxyOn(t *testing.T, upstream, address string) *processFaultHTTPProxy {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{Proxy: nil, DisableCompression: true, DisableKeepAlives: true, MaxConnsPerHost: 32}
	client := &http.Client{Transport: transport, Timeout: processFaultProxyTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	proxy := &processFaultHTTPProxy{listener: listener, client: client, upstream: upstream, done: make(chan struct{}), conns: make(map[net.Conn]struct{})}
	proxy.server = &http.Server{Handler: http.HandlerFunc(proxy.serve), ReadHeaderTimeout: time.Second, ReadTimeout: processFaultProxyTimeout, WriteTimeout: processFaultProxyTimeout, ConnState: proxy.connectionState}
	go func() { _ = proxy.server.Serve(listener) }()
	t.Cleanup(func() {
		close(proxy.done)
		_ = proxy.server.Close()
		transport.CloseIdleConnections()
		deadline := time.Now().Add(time.Second)
		for {
			proxy.mu.Lock()
			connections := len(proxy.conns)
			proxy.mu.Unlock()
			if connections == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Error("owned plaintext HTTP proxy still has connections", connections)
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	return proxy
}

func (proxy *processFaultHTTPProxy) connectionState(conn net.Conn, state http.ConnState) {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	if state == http.StateNew {
		proxy.conns[conn] = struct{}{}
	}
	if state == http.StateClosed || state == http.StateHijacked {
		delete(proxy.conns, conn)
	}
}

func (proxy *processFaultHTTPProxy) arm(fault processFaultHTTPFault) {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	proxy.fault = &fault
}

func (proxy *processFaultHTTPProxy) snapshot() []processFaultHTTPEvent {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	return append([]processFaultHTTPEvent(nil), proxy.events...)
}

func (proxy *processFaultHTTPProxy) setUnavailable(unavailable bool) {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	proxy.offline = unavailable
	if unavailable {
		for conn := range proxy.conns {
			_ = conn.Close()
		}
	}
}

func (proxy *processFaultHTTPProxy) wait(ctx context.Context, gate <-chan struct{}) bool {
	if gate == nil {
		return true
	}
	select {
	case <-gate:
		return true
	case <-ctx.Done():
	case <-proxy.done:
	case <-time.After(processFaultProxyTimeout):
	}
	return false
}

func processFaultHTTPDrop(writer http.ResponseWriter) {
	hijacker, ok := writer.(http.Hijacker)
	if !ok {
		panic("owned HTTP/1 fixture requires hijacking")
	}
	conn, _, err := hijacker.Hijack()
	if err == nil {
		_ = conn.Close()
	}
}

func (proxy *processFaultHTTPProxy) serve(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(io.LimitReader(request.Body, (2<<20)+1))
	_ = request.Body.Close()
	if err != nil || len(body) > 2<<20 {
		http.Error(writer, "owned fault fixture request exceeded finite bound", http.StatusBadRequest)
		return
	}
	proxy.mu.Lock()
	offline, fault := proxy.offline, proxy.fault
	if fault != nil && fault.path == request.URL.Path && (bytes.Contains(body, fault.match) || strings.Contains(request.URL.Path, string(fault.match))) {
		proxy.fault = nil
	} else {
		fault = nil
	}
	proxy.mu.Unlock()
	if offline {
		processFaultHTTPDrop(writer)
		return
	}
	if fault != nil {
		if fault.started != nil {
			select {
			case fault.started <- struct{}{}:
			case <-request.Context().Done():
				return
			case <-proxy.done:
				return
			}
		}
		if !proxy.wait(request.Context(), fault.before) {
			processFaultHTTPDrop(writer)
			return
		}
	}
	upstream, err := http.NewRequestWithContext(request.Context(), request.Method, proxy.upstream+request.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		http.Error(writer, "owned fixture forwarding error", http.StatusBadGateway)
		return
	}
	upstream.GetBody = nil
	upstream.Header = request.Header.Clone()
	upstream.Header.Del("Connection")
	mutation := request.URL.Path == "/_bulk" || strings.Contains(request.URL.Path, "/_update/") || strings.Contains(request.URL.Path, "/_doc/") && (request.Method == http.MethodPut || request.Method == http.MethodDelete)
	event := processFaultHTTPEvent{path: request.URL.Path, mutation: mutation}
	proxy.mu.Lock()
	index := len(proxy.events)
	proxy.events = append(proxy.events, event)
	proxy.mu.Unlock()
	response, err := proxy.client.Do(upstream)
	if err != nil {
		http.Error(writer, "owned real backend transport failed", http.StatusBadGateway)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, (2<<20)+1))
	_ = response.Body.Close()
	if err != nil || len(raw) > 2<<20 {
		http.Error(writer, "owned fixture reply exceeded finite bound", http.StatusBadGateway)
		return
	}
	acknowledged := response.StatusCode >= 200 && response.StatusCode < 300
	if request.URL.Path == "/_bulk" {
		var bulk struct {
			Errors *bool `json:"errors"`
		}
		acknowledged = acknowledged && json.Unmarshal(raw, &bulk) == nil && bulk.Errors != nil && !*bulk.Errors
	}
	event.status, event.acknowledged = response.StatusCode, acknowledged
	event.dropped, event.digest = fault != nil && fault.drop, sha256.Sum256(raw)
	proxy.mu.Lock()
	proxy.events[index] = event
	proxy.mu.Unlock()
	if fault != nil {
		if fault.ack != nil {
			select {
			case fault.ack <- struct{}{}:
			case <-request.Context().Done():
				return
			case <-proxy.done:
				return
			}
		}
		if !proxy.wait(request.Context(), fault.after) || fault.drop {
			processFaultHTTPDrop(writer)
			return
		}
	}
	for name, values := range response.Header {
		if strings.EqualFold(name, "Connection") || strings.EqualFold(name, "Transfer-Encoding") {
			continue
		}
		writer.Header()[name] = values
	}
	writer.WriteHeader(response.StatusCode)
	_, _ = writer.Write(raw)
}
