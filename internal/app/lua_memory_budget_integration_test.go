//go:build integration

package app

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc/status"
)

type luaMemoryCall struct {
	mutation          *weirclient.MutationResult
	err               error
	started, finished time.Time
	deadline          time.Time
	returned          bool
}

// This tiny request executes only in the resource-limited Linux CLI. A bounded
// final Lua string is not a bound on transient allocation: old/new concatenation
// strings can coexist. Default tests never run the Lua source on the host.
func TestCLILuaMemoryBudget(t *testing.T) {
	if os.Getenv("WEIR_LUA_MEMORY_INTEGRATION") != "1" {
		t.Skip("requires explicit owned CLI bounded Lua memory qualification")
	}
	if os.Getenv("WEIR_INTEGRATION") != "1" || os.Getenv("WEIR_MONGO_PROFILE") == "tls" {
		t.Fatal("requires the isolated plain Mongo fixture")
	}
	binary, revision, digest := os.Getenv("WEIR_LUA_MEMORY_BINARY"), os.Getenv("WEIR_LUA_MEMORY_SOURCE"), os.Getenv("WEIR_LUA_MEMORY_SHA256")
	if !filepath.IsAbs(binary) || len(revision) != 40 || len(digest) != 64 {
		t.Fatal("requires absolute CLI path and exact clean source/SHA256 receipt")
	}
	artifact, err := os.ReadFile(binary)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(artifact)) != digest {
		t.Fatal("CLI differs from exact build receipt", err)
	}
	engine := strings.TrimSpace(packagedDocker(t, "info", "--format", "{{.OSType}}/{{.Architecture}}/{{.CgroupVersion}} kernel={{.KernelVersion}}"))
	imageRaw := packagedDocker(t, "image", "inspect", "--format", "{{.Os}}/{{.Architecture}} {{.Id}}", "busybox:1.37.0")
	image := strings.TrimSpace(imageRaw)
	if !strings.HasPrefix(engine, "linux/aarch64/2 ") || image != "linux/arm64 sha256:ad4f11f7c85f87c0a6d6c0fe492ad53f9cdcff6e8c88b881360c70eff701c67b" {
		t.Fatal("requires the pinned native Linux arm64 fixture", engine, image)
	}
	fixture := testmongo.Open(t)
	dataset := &nativeBurstDataset{store: "mongo", root: fixture.DB + "/records", media: "application/bson", mongo: fixture, expected: make(map[string]nativeBurstRecord)}
	seedNativeBurstDataset(t, dataset)
	for _, id := range []string{"lua-control", "lua-bound-target"} {
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int64(1)}, {Key: "pad", Value: strings.Repeat("x", 1<<10)}}
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		_, err := fixture.Admin.Database(fixture.DB).Collection("records").InsertOne(ctx, document)
		cancel()
		if err != nil {
			t.Fatal("small independent seed failed", id, err)
		}
	}
	filter := bson.D{{Key: "_id", Value: "lua-bound-target"}}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	before, err := fixture.Admin.Database(fixture.DB).Collection("records").FindOne(ctx, filter).Raw()
	cancel()
	if err != nil || len(before) > 4<<10 {
		t.Fatal("small original document oracle failed", err)
	}
	beforeHash := sha256.Sum256(before)
	backend := &Mongo{URI: "mongodb://host.docker.internal:27028/?directConnection=true&serverMonitoringMode=poll"}
	local := &Local{Backend: BackendConfig{MongoDB: backend}}
	store := StoreConfig{Name: "mongo", Local: local}
	cfg := DefaultConfig()
	cfg.Routing.Stores = []StoreConfig{store}
	if cfg.Basic.Transport.MaxPendingRecords != 32 || time.Duration(cfg.Basic.Transport.Timeouts.Stall) != 30*time.Second {
		t.Fatal("default transport policy changed")
	}
	opts := memoryRecoveryOptions{binary: binary, capacityMiB: 512, automatic: true, config: &cfg}
	process := startMemoryRecoveryCLI(t, opts)
	version := strings.TrimSpace(packagedDocker(t, "exec", process.name, "/weir", "version"))
	var identity map[string]string
	if json.Unmarshal([]byte(version), &identity) != nil || identity["state"] != "clean-commit" || identity["dirty"] != "false" || identity["target"] != "linux/arm64" || identity["revision"] != revision {
		t.Fatal("requires the clean exact Linux CLI", version)
	}
	t.Logf("Lua fixed512MiB/2CPU single_process=true source=%s binary_SHA256=%s version=%s engine=%s image=%s", revision, digest, version, engine, image)
	initial, err := ingressMemoryMetrics(process)
	if err != nil {
		t.Fatal("initial process metrics unavailable", err)
	}
	labels := map[string]string{"store": "mongo"}
	for name, want := range map[string]float64{"weir_store_active_executions_limit": 16, "weir_store_pending_entries_limit": 1024, "weir_store_pending_reserved_bytes_limit": 32 << 20, "weir_store_batch_max_operations": 128, "weir_store_batch_max_bytes": 4 << 20, "weir_store_batch_max_wait_seconds": .02} {
		if qualificationGauge(t, initial, name, labels) != want {
			t.Fatal("default Store policy changed", name, want)
		}
	}
	qualificationExecutionMemory(t, initial, process.capacity)
	if testmetrics.Sum(initial, "weir_memory_go_runtime_managed") != 1 {
		t.Fatal("CLI default automatic Go memory policy inactive")
	}
	monitorContext, stopMonitor := context.WithCancel(context.Background())
	var monitor sync.WaitGroup
	monitor.Add(2)
	// The shared sample helpers retain their ingress_* log labels; every such
	// line in this test belongs to this one Lua CLI, with no upload workload.
	go func() { defer monitor.Done(); ingressMemorySample(t, monitorContext, process, false) }()
	go func() { defer monitor.Done(); ingressMemorySample(t, monitorContext, process, true) }()
	t.Cleanup(func() {
		stopMonitor()
		monitor.Wait()
		ingressMemoryEvents(t, process, "Lua_final_cleanup")
		ingressMemoryDocker(t, process)
	})
	client, healthClient := openIngressMemoryClient(t, process), openIngressMemoryClient(t, process)
	ingressMemoryEvents(t, process, "Lua_initial")
	ingressMemoryIdle(t, process, time.Now().Add(5*time.Second), 2)
	if t.Failed() || !luaMemoryControl(t, client, fixture) {
		return
	}
	ingressMemoryIdle(t, process, time.Now().Add(5*time.Second), 2)
	if t.Failed() {
		return
	}
	const source = `return function(current, incoming) local s='x'; for i=1,29 do s=s..s end; return weir.keep() end`
	const finalStringBytes = uint64(1) << 29
	t.Logf("Lua_probe tiny_source_bytes=%d source_SHA256=%x iterations=29 final_string_bytes=%d transient_peak_may_exceed_final=true caller3s retries=0 host_executes_Lua=false", len(source), sha256.Sum256([]byte(source)), finalStringBytes)
	program := &weirclient.LuaTransform{Source: []byte(source)}
	request := &weirclient.AtomicTransformRequest{Resource: dataset.root + "/s:lua-bound-target", Lua: program}
	callOptions := weirclient.AtomicTransformOptions{StoreName: "mongo", Request: request}
	started := time.Now()
	deadline := started.Add(3 * time.Second)
	callContext, stopCall := context.WithDeadline(t.Context(), deadline)
	done := make(chan luaMemoryCall, 1)
	go func() {
		result := luaMemoryCall{started: started, deadline: deadline}
		result.mutation, result.err = client.AtomicTransform(callContext, callOptions)
		result.finished = time.Now()
		result.returned = true
		done <- result
	}()
	result := luaMemoryCall{started: started, deadline: deadline, err: fmt.Errorf("caller3s plus bounded1s join exhausted; no acknowledgement")}
	select {
	case result = <-done:
	case <-time.After(time.Until(deadline.Add(time.Second))):
		t.Error("Lua SDK call failed to join within original caller plus bounded1s")
	}
	cancelledAt := time.Now()
	if deadline.Before(cancelledAt) {
		cancelledAt = deadline
	}
	stopCall()
	if !result.returned {
		go func() { _ = client.Close() }()
	}
	luaMemoryOutcome(t, result)
	// Do not Fatal after launching the probe: RPC failure/OOM must not skip
	// independent effects, fresh health calls, or Docker state collection.
	ingressMemoryDocker(t, process)
	healthDone := make(chan struct{})
	go func() {
		defer close(healthDone)
		ingressMemoryHealth(t, healthClient, dataset)
	}()
	oracleDone := make(chan bool, 1)
	go func() {
		oracleContext, stopOracle := context.WithTimeout(context.Background(), 3*time.Second)
		defer stopOracle()
		after, oracleErr := fixture.Admin.Database(fixture.DB).Collection("records").FindOne(oracleContext, filter).Raw()
		unchanged := oracleErr == nil && len(after) == len(before) && sha256.Sum256(after) == beforeHash
		t.Logf("Lua_independent_unchanged ID=lua-bound-target bytes_before=%d bytes_after=%d before_SHA256=%x after_SHA256=%x unchanged=%t error=%v", len(before), len(after), beforeHash, sha256.Sum256(after), unchanged, oracleErr)
		oracleDone <- unchanged
	}()
	select {
	case <-healthDone:
	case <-time.After(4100 * time.Millisecond):
		t.Error("fresh Read/Native3s plus bounded1s join did not complete")
		closeIngressMemoryClient(t, healthClient)
	}
	select {
	case unchanged := <-oracleDone:
		if !unchanged {
			t.Error("bounded Keep probe changed original backend document or oracle unknown")
		}
	case <-time.After(time.Until(cancelledAt.Add(5 * time.Second))):
		t.Error("independent unchanged oracle did not join within recovery window")
	}
	ingressMemoryIdle(t, process, cancelledAt.Add(5*time.Second), 2)
	ingressMemoryEvents(t, process, "Lua_after_probe")
	stopMonitor()
	monitor.Wait()
	ingressMemoryDocker(t, process)
}

func luaMemoryControl(t *testing.T, client *weirclient.Client, fixture *testmongo.Fixture) bool {
	t.Helper()
	program := &weirclient.LuaTransform{Source: []byte(`return function(current, incoming) current.n=current.n+1; return current end`)}
	request := &weirclient.AtomicTransformRequest{Resource: fixture.DB + "/records/s:lua-control", Lua: program}
	opts := weirclient.AtomicTransformOptions{StoreName: "mongo", Request: request}
	started := time.Now()
	deadline := started.Add(3 * time.Second)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	done := make(chan luaMemoryCall, 1)
	go func() {
		result := luaMemoryCall{started: started, deadline: deadline}
		result.mutation, result.err = client.AtomicTransform(ctx, opts)
		result.finished, result.returned = time.Now(), true
		done <- result
	}()
	call := luaMemoryCall{started: started, deadline: deadline}
	select {
	case call = <-done:
	case <-time.After(time.Until(deadline.Add(time.Second))):
		t.Error("normal Lua control caller3s plus bounded1s join exhausted")
	}
	cancel()
	mutation, err := call.mutation, call.err
	timely := call.returned && !call.finished.IsZero() && !call.finished.After(call.deadline)
	valid := timely && err == nil && mutation != nil && mutation.Failure == nil && mutation.Outcome == weirclient.MutationApplied
	if !valid {
		t.Error("normal business Lua control lacked complete APPLIED within original3s caller deadline", mutation, err, call.returned, call.finished, call.deadline)
	}
	filter := bson.D{{Key: "_id", Value: "lua-control"}}
	ctx, cancel = context.WithTimeout(t.Context(), 3*time.Second)
	raw, err := fixture.Admin.Database(fixture.DB).Collection("records").FindOne(ctx, filter).Raw()
	cancel()
	id, idOK := raw.Lookup("_id").StringValueOK()
	n, nOK := raw.Lookup("n").Int64OK()
	pad, padOK := raw.Lookup("pad").StringValueOK()
	if err != nil || !idOK || id != "lua-control" || !nOK || n != 2 || !padOK || pad != strings.Repeat("x", 1<<10) {
		t.Error("normal Lua control differs from independent small-document oracle", err)
		return false
	}
	t.Logf("Lua_normal_control Transform_APPLIED=%t ID_n_pad_valid=true bytes=%d SHA256=%x caller3s", valid, len(raw), sha256.Sum256(raw))
	return valid
}

func luaMemoryOutcome(t *testing.T, result luaMemoryCall) {
	t.Helper()
	outcome, failure := "UNCONFIRMED", ""
	if result.mutation != nil {
		outcome = result.mutation.Outcome.String()
		if result.mutation.Failure != nil {
			failure = result.mutation.Failure.Code.String()
		}
	}
	timely := result.returned && !result.finished.IsZero() && !result.deadline.IsZero() && !result.finished.After(result.deadline)
	successfulKeep := timely && outcome == "APPLIED" && failure == "" && result.err == nil
	safeRejection := timely && (outcome == "NOT_STARTED" || outcome == "NOT_APPLIED") && failure == "RESOURCE_EXHAUSTED" && result.err == nil
	elapsed := time.Since(result.started)
	if result.returned {
		elapsed = result.finished.Sub(result.started)
	}
	t.Logf("Lua_probe_outcome outcome=%s failure=%s RPC=%s error=%v returned=%t started=%s finished=%s deadline=%s within_original_deadline=%t elapsed_or_observation_wait=%s confirmed_successful_Keep=%t safe_capacity_rejection=%t timeout_or_UNKNOWN_is_not_Keep=true", outcome, failure, status.Code(result.err), result.err, result.returned, result.started.UTC().Format(time.RFC3339Nano), result.finished.UTC().Format(time.RFC3339Nano), result.deadline.UTC().Format(time.RFC3339Nano), timely, elapsed, successfulKeep, safeRejection)
	if !successfulKeep && !safeRejection {
		t.Error("Lua allocation probe lacked complete Keep or explicit safe capacity outcome within original3s caller deadline", outcome, failure, result.err, result.returned, result.finished, result.deadline)
	}
}
