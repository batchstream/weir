//go:build integration

package app

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/net/http2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type ingressMemoryArtifact struct{ binary, revision, digest string }

type ingressMemoryDocument struct {
	id       string
	raw      []byte
	digest   [32]byte
	padBytes int
}

type ingressMemoryResult struct {
	Index                     int       `json:"index"`
	ID                        string    `json:"id"`
	Started                   time.Time `json:"started,omitzero"`
	Finished                  time.Time `json:"finished,omitzero"`
	Outcome, Failure, Error   string
	RPCCode                   string
	Received                  bool
	TransportObserved         bool
	ResponseMessages          int
	DataWritten               int64
	PrefixWritten, AllWritten int64
	CallerCanceled            bool
	FirstResponseHeaders      time.Time `json:"first_response_headers,omitzero"`
}

// A uses the public SDK without retries. Explicit capacity rejection can satisfy
// memory safety, but never counts as a successful ordinary-write profile.
func TestCLIIngressConcurrentPut(t *testing.T) {
	if os.Getenv("WEIR_INGRESS_MEMORY_INTEGRATION") != "1" {
		t.Skip("requires explicit owned CLI ingress Put qualification")
	}
	artifact := ingressMemoryReceipt(t)
	for _, capacity := range []int{512, 1024} {
		if !t.Run(fmt.Sprintf("%dMiB", capacity), func(t *testing.T) {
			runIngressMemoryCase(t, artifact, capacity, false)
		}) {
			return
		}
	}
}

// B has a separate opt-in so failure in A cannot silently start a new profile.
func TestCLIIngressPartialUploads(t *testing.T) {
	if os.Getenv("WEIR_INGRESS_MEMORY_PARTIAL_INTEGRATION") != "1" {
		t.Skip("requires separate explicit partial-upload ingress qualification")
	}
	artifact := ingressMemoryReceipt(t)
	for _, capacity := range []int{512, 1024} {
		if !t.Run(fmt.Sprintf("%dMiB", capacity), func(t *testing.T) {
			runIngressMemoryCase(t, artifact, capacity, true)
		}) {
			return
		}
	}
}

func ingressMemoryReceipt(t *testing.T) ingressMemoryArtifact {
	t.Helper()
	if os.Getenv("WEIR_INTEGRATION") != "1" || os.Getenv("WEIR_MONGO_PROFILE") == "tls" {
		t.Fatal("requires the isolated plain Mongo fixture")
	}
	artifact := ingressMemoryArtifact{binary: os.Getenv("WEIR_INGRESS_MEMORY_BINARY"), revision: os.Getenv("WEIR_INGRESS_MEMORY_SOURCE"), digest: os.Getenv("WEIR_INGRESS_MEMORY_SHA256")}
	if !filepath.IsAbs(artifact.binary) || len(artifact.revision) != 40 || len(artifact.digest) != 64 {
		t.Fatal("requires absolute CLI path, full source revision and binary SHA256")
	}
	raw, err := os.ReadFile(artifact.binary)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != artifact.digest {
		t.Fatal("CLI differs from clean exact build receipt", err)
	}
	engine := strings.TrimSpace(packagedDocker(t, "info", "--format", "{{.OSType}}/{{.Architecture}}/{{.CgroupVersion}} kernel={{.KernelVersion}}"))
	imageRaw := packagedDocker(t, "image", "inspect", "--format", "{{.Os}}/{{.Architecture}} {{.Id}}", "busybox:1.37.0")
	image := strings.TrimSpace(imageRaw)
	if !strings.HasPrefix(engine, "linux/aarch64/2 ") || image != "linux/arm64 sha256:ad4f11f7c85f87c0a6d6c0fe492ad53f9cdcff6e8c88b881360c70eff701c67b" {
		t.Fatal("requires the pinned native Linux arm64 fixture", engine, image)
	}
	t.Logf("exact_CLI source=%s SHA256=%s engine=%s image=%s", artifact.revision, artifact.digest, engine, image)
	return artifact
}

func runIngressMemoryCase(t *testing.T, artifact ingressMemoryArtifact, capacity int, partial bool) {
	t.Helper()
	fixture := testmongo.Open(t)
	dataset := &nativeBurstDataset{store: "mongo", root: fixture.DB + "/records", media: "application/bson", mongo: fixture, expected: make(map[string]nativeBurstRecord)}
	seedNativeBurstDataset(t, dataset)
	documents := ingressMemoryDocuments(t)
	backend := &Mongo{URI: "mongodb://host.docker.internal:27028/?directConnection=true&serverMonitoringMode=poll"}
	local := &Local{Backend: BackendConfig{MongoDB: backend}}
	store := StoreConfig{Name: "mongo", Local: local}
	cfg := DefaultConfig()
	cfg.Routing.Stores = []StoreConfig{store}
	if cfg.Basic.Transport.MaxPendingRecords != 32 || time.Duration(cfg.Basic.Transport.Timeouts.Stall) != 30*time.Second {
		t.Fatal("default transport policy changed")
	}
	opts := memoryRecoveryOptions{binary: artifact.binary, capacityMiB: capacity, automatic: true, config: &cfg}
	process := startMemoryRecoveryCLI(t, opts)
	version := strings.TrimSpace(packagedDocker(t, "exec", process.name, "/weir", "version"))
	var identity map[string]string
	if json.Unmarshal([]byte(version), &identity) != nil || identity["state"] != "clean-commit" || identity["dirty"] != "false" || identity["revision"] != artifact.revision || identity["target"] != "linux/arm64" {
		t.Fatal("requires the clean exact Linux CLI identity", version)
	}
	t.Logf("ingress profile_partial=%t capacity=%dMiB CPUs=2 records=32 exact_BSON_bytes=%d common_caller=5s retries=0 version=%s", partial, capacity, 12<<20, version)
	initial, err := ingressMemoryMetrics(process)
	if err != nil {
		t.Fatal("initial metrics unavailable", err)
	}
	labels := map[string]string{"store": "mongo"}
	for name, want := range map[string]float64{"weir_store_active_executions_limit": 16, "weir_store_pending_entries_limit": 1024, "weir_store_pending_reserved_bytes_limit": 32 << 20, "weir_store_batch_max_operations": 128, "weir_store_batch_max_bytes": 4 << 20, "weir_store_batch_max_wait_seconds": .02} {
		got := qualificationGauge(t, initial, name, labels)
		if got != want {
			t.Fatal("default Store policy changed", name, want)
		}
	}
	qualificationExecutionMemory(t, initial, process.capacity)
	ingressMemoryIdle(t, process, time.Now().Add(5*time.Second), 0)
	client, healthClient := openIngressMemoryClient(t, process), openIngressMemoryClient(t, process)
	ingressMemoryIdle(t, process, time.Now().Add(5*time.Second), 2)
	monitorContext, stopMonitor := context.WithCancel(context.Background())
	var monitor sync.WaitGroup
	monitor.Add(2)
	go func() { defer monitor.Done(); ingressMemorySample(t, monitorContext, process, false) }()
	go func() { defer monitor.Done(); ingressMemorySample(t, monitorContext, process, true) }()
	t.Cleanup(func() {
		stopMonitor()
		monitor.Wait()
		ingressMemoryDocker(t, process)
	})
	ingressMemoryEvents(t, process, "initial")
	if partial {
		options := ingressMemoryPartialOptions{fixture: fixture, process: process, dataset: dataset, documents: documents, client: client, healthClient: healthClient}
		runIngressMemoryPartial(t, options)
		ingressMemoryEvents(t, process, "final")
		stopMonitor()
		monitor.Wait()
		ingressMemoryDocker(t, process)
		return
	}
	results := make(chan ingressMemoryResult, 32)
	started := time.Now()
	deadline := started.Add(5 * time.Second)
	t.Logf("ingress_launch profile=ordinary_full_write t0=%s common_deadline=%s", started.UTC().Format(time.RFC3339Nano), deadline.UTC().Format(time.RFC3339Nano))
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	barrier := make(chan struct{})
	var calls sync.WaitGroup
	var returned atomic.Int64
	for index := range documents {
		calls.Add(1)
		go func() {
			defer calls.Done()
			<-barrier
			result := ingressMemoryResult{Index: index, ID: documents[index].id, Started: time.Now(), TransportObserved: false}
			document := &pb.Document{ContentType: "application/bson", Data: documents[index].raw}
			request := &weirclient.WriteRequest{Resource: dataset.root + "/s:" + documents[index].id, Document: document}
			options := weirclient.WriteOptions{StoreName: "mongo", Request: request}
			mutation, callErr := client.Put(ctx, options)
			result.Finished, result.Received, result.RPCCode = time.Now(), true, status.Code(callErr).String()
			if callErr != nil {
				result.Error = callErr.Error()
			}
			if mutation != nil {
				result.Outcome = mutation.Outcome.String()
				if mutation.Failure != nil {
					result.Failure = mutation.Failure.Code.String()
				}
			}
			returned.Add(1)
			results <- result
		}()
	}
	healthDone := make(chan struct{})
	go func() {
		defer close(healthDone)
		timer := time.NewTimer(25 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		pending := 32 - returned.Load()
		t.Logf("ordinary_upload_health_start elapsed=%s calls_not_yet_returned=%d", time.Since(started), pending)
		if pending == 0 {
			t.Error("ordinary calls finished before health probe; concurrent-health condition not reached")
		}
		ingressMemoryHealth(t, healthClient, dataset)
	}()
	sameClientHealthDone := make(chan struct{})
	go func() {
		defer close(sameClientHealthDone)
		timer := time.NewTimer(time.Until(started.Add(time.Second)))
		defer timer.Stop()
		select {
		case <-t.Context().Done():
			return
		case <-timer.C:
		}
		t.Logf("same_SDK_health_start elapsed=%s original_upload_client=%p calls_not_yet_returned=%d TCP_connection_identity=not_observed", time.Since(started), client, 32-returned.Load())
		ingressMemoryHealth(t, client, dataset)
	}()
	close(barrier)
	joined := make(chan struct{})
	go func() { calls.Wait(); close(joined) }()
	collected := make([]ingressMemoryResult, 32)
	for index := range documents {
		collected[index] = ingressMemoryResult{Index: index, ID: documents[index].id, Outcome: "UNCONFIRMED", Error: "SDK did not return within caller plus bounded1s join"}
	}
	joinTimer := time.NewTimer(time.Until(deadline.Add(time.Second)))
	defer joinTimer.Stop()
	received := 0
collect:
	for received < 32 {
		select {
		case result := <-results:
			collected[result.Index] = result
			received++
			if result.RPCCode == "OK" && result.Finished.After(deadline) {
				t.Error("ordinary successful RPC returned after the original common5s deadline", result.ID)
			}
		case <-joinTimer.C:
			t.Error("bounded caller join lost results", received)
			break collect
		}
	}
	cancelledAt := time.Now()
	if deadline.Before(cancelledAt) {
		cancelledAt = deadline
	}
	cancel()
	select {
	case <-healthDone:
	case <-time.After(time.Until(started.Add(7100 * time.Millisecond))):
		t.Error("health did not join after common caller cancellation")
	}
	select {
	case <-joined:
	case <-time.After(time.Until(deadline.Add(time.Second))):
		t.Error("some SDK calls remain outside the bounded collection; unreturned IDs remain UNKNOWN")
	}
	select {
	case <-sameClientHealthDone:
	case <-time.After(time.Until(deadline.Add(time.Second))):
		t.Error("same-SDK health did not join within original common5s plus bounded1s")
	}
	ingressMemoryIdle(t, process, cancelledAt.Add(5*time.Second), 2)
	ingressMemoryOracle(t, fixture, documents, collected)
	for _, result := range collected {
		raw, _ := json.Marshal(result)
		t.Logf("ingress_result %s SDK_send_end_and_first_response_unknown=true", raw)
	}
	t.Logf("unconditional_same_SDK_recovery prior_failure=%t original_upload_client=%p TCP_connection_identity=not_observed", t.Failed(), client)
	ingressMemoryRecovery(t, client, dataset)
	ingressMemoryIdle(t, process, time.Now().Add(5*time.Second), 2)
	closeIngressMemoryClient(t, client)
	closeIngressMemoryClient(t, healthClient)
	ingressMemoryIdle(t, process, time.Now().Add(5*time.Second), 0)
	ingressMemoryEvents(t, process, "final")
	stopMonitor()
	monitor.Wait()
	ingressMemoryDocker(t, process)
}

func ingressMemoryDocuments(t *testing.T) []ingressMemoryDocument {
	t.Helper()
	documents := make([]ingressMemoryDocument, 32)
	for index := range documents {
		id := fmt.Sprintf("ingress-%02d", index)
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int64(1)}, {Key: "pad", Value: ""}}
		empty, err := bson.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		pad := (12 << 20) - len(empty)
		document[2].Value = strings.Repeat(string(rune('a'+index%26)), pad)
		raw, err := bson.Marshal(document)
		if err != nil || len(raw) != 12<<20 {
			t.Fatal("synthetic document is not exact legal12MiB BSON", err)
		}
		documents[index] = ingressMemoryDocument{id: id, raw: raw, digest: sha256.Sum256(raw), padBytes: pad}
	}
	return documents
}

func openIngressMemoryClient(t *testing.T, process *memoryRecoveryCLI) *weirclient.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	opts := weirclient.OpenOptions{Seed: process.address, Stores: []string{"mongo"}}
	client, err := weirclient.Open(ctx, opts)
	if err != nil {
		t.Fatal("real SDK discovery failed", err)
	}
	t.Cleanup(func() { closeIngressMemoryClient(t, client) })
	return client
}

func closeIngressMemoryClient(t *testing.T, client *weirclient.Client) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- client.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Error("SDK close", err)
		}
	case <-time.After(time.Second):
		t.Error("SDK close exceeded bounded1s")
	}
}

func ingressMemoryHealth(t *testing.T, client *weirclient.Client, dataset *nativeBurstDataset) {
	t.Helper()
	results := make(chan nativeBurstResult, 2)
	started := time.Now()
	deadline := started.Add(3 * time.Second)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	for _, kind := range []int{0, 2} {
		go func() {
			job := nativeBurstResult{kind: kind}
			job.err = executeNativeBurstCall(ctx, client, dataset, job)
			job.elapsed = time.Since(started)
			results <- job
		}()
	}
	timer := time.NewTimer(time.Until(deadline.Add(time.Second)))
	defer timer.Stop()
	for range 2 {
		select {
		case job := <-results:
			t.Logf("ingress_health SDK_client=%p kind=%d caller3s elapsed=%s error=%v", client, job.kind, job.elapsed, job.err)
			if job.err != nil || job.elapsed > 3*time.Second {
				t.Error("healthy small Read/Native failed or exceeded original3s caller deadline during uploads", job.kind, job.elapsed, job.err)
			}
		case <-timer.C:
			t.Error("health caller3s plus bounded1s join exhausted")
			return
		}
	}
}

func ingressMemoryRecovery(t *testing.T, client *weirclient.Client, dataset *nativeBurstDataset) {
	t.Helper()
	job := nativeBurstResult{kind: 1, sequence: 999999}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	putErr := executeNativeBurstCall(ctx, client, dataset, job)
	cancel()
	if putErr != nil {
		t.Error("fresh small Put after recovery failed; still checking same-SDK Read", putErr)
	}
	request := &weirclient.ReadRequest{Resource: dataset.root + "/s:write-999999"}
	opts := weirclient.ReadOneOptions{StoreName: "mongo", Request: request}
	ctx, cancel = context.WithTimeout(t.Context(), 3*time.Second)
	result, err := client.ReadOne(ctx, opts)
	cancel()
	var actual nativeBurstRecord
	if err != nil || result == nil || result.Failure != nil || result.Document == nil || result.Document.ContentType != "application/bson" {
		t.Error("fresh small Read after recovery failed", err)
		return
	}
	err = bson.Unmarshal(result.Document.Data, &actual)
	if err != nil || actual.N != 1000999 || actual.Pad != strings.Repeat("x", 1<<10) {
		t.Error("recovered process changed small recovery document", err)
		return
	}
	t.Logf("same_running_process_and_SDK fresh small Put_APPLIED=%t Read_n_pad_valid=true caller3s TCP_connection_identity=not_observed", putErr == nil)
}

func ingressMemoryOracle(t *testing.T, fixture *testmongo.Fixture, documents []ingressMemoryDocument, results []ingressMemoryResult) {
	t.Helper()
	applied, denied, ambiguous, present, completedRPCs := 0, 0, 0, 0, 0
	ids := make([]string, 0, len(documents))
	for index, expected := range documents {
		ids = append(ids, expected.id)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		filter := bson.D{{Key: "_id", Value: expected.id}}
		raw, err := fixture.Admin.Database(fixture.DB).Collection("records").FindOne(ctx, filter).Raw()
		cancel()
		exists := err == nil
		valid := exists && len(raw) == 12<<20 && sha256.Sum256(raw) == expected.digest
		if exists {
			id, idOK := raw.Lookup("_id").StringValueOK()
			n, nOK := raw.Lookup("n").Int64OK()
			pad, padOK := raw.Lookup("pad").StringValueOK()
			valid = valid && idOK && id == expected.id && nOK && n == 1 && padOK && len(pad) == expected.padBytes
			present++
		}
		if err != nil && !errors.Is(err, mongo.ErrNoDocuments) || exists && !valid {
			t.Error("independent per-ID ingress effect/hash oracle failed", expected.id, err)
		}
		result := results[index]
		if result.Received && result.Error == "" && result.RPCCode == "OK" {
			completedRPCs++
		} else {
			t.Error("ordinary Put lacked a complete successful RPC; business evidence retained", expected.id, result.Received, result.RPCCode, result.Error)
		}
		switch result.Outcome {
		case "APPLIED":
			applied++
			if !valid {
				t.Error("confirmed APPLIED missing/different at backend", expected.id)
			}
			if result.Failure != "" || result.Error != "" {
				t.Error("APPLIED evidence retained but ordinary RPC/failure was not successful", expected.id, result.Failure, result.Error)
			}
		case "NOT_STARTED", "NOT_APPLIED":
			denied++
			if exists || result.Failure != "RESOURCE_EXHAUSTED" {
				t.Error("non-applied fresh Put had effect or unexpected failure", expected.id, result.Outcome, result.Failure, exists)
			}
		default:
			ambiguous++
			t.Error("ordinary Put remained ambiguous; backend observation cannot create an acknowledgement", expected.id, result.Outcome, result.Error)
		}
		t.Logf("independent_effect id=%s present=%t ID_n_len_SHA256_valid=%t actual_bytes=%d expected_SHA256=%x actual_SHA256=%x outcome=%s rpc=%s", expected.id, exists, valid, len(raw), expected.digest, sha256.Sum256(raw), result.Outcome, result.RPCCode)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	in := bson.D{{Key: "$in", Value: ids}}
	filter := bson.D{{Key: "_id", Value: in}}
	count, err := fixture.Admin.Database(fixture.DB).Collection("records").CountDocuments(ctx, filter)
	if err != nil || count != int64(present) {
		t.Error("independent cardinality differs from per-ID effects", count, present, err)
	}
	t.Logf("ingress_effect_summary offered=32 APPLIED=%d capacity_denied=%d ambiguous=%d independent_present=%d completed_RPC=%d all_ordinary_writes_APPLIED=%t capacity_survival_does_not_imply_business_completeness=true", applied, denied, ambiguous, present, completedRPCs, applied == 32 && completedRPCs == 32)
}

func ingressMemoryMetrics(process *memoryRecoveryCLI) (map[string]*dto.MetricFamily, error) {
	client := &http.Client{Timeout: 100 * time.Millisecond}
	response, err := client.Get("http://" + process.diagnostic + "/metrics")
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics HTTP status=%d", response.StatusCode)
	}
	parser := expfmt.NewTextParser(model.LegacyValidation)
	return parser.TextToMetricFamilies(io.LimitReader(response.Body, 4<<20))
}

func ingressMemorySample(t *testing.T, ctx context.Context, process *memoryRecoveryCLI, docker bool) {
	ticker := time.NewTicker(25 * time.Millisecond)
	if docker {
		ticker.Reset(100 * time.Millisecond)
	}
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case scheduled := <-ticker.C:
			at := time.Now()
			if docker {
				ingressMemoryDocker(t, process)
				continue
			}
			metrics, err := ingressMemoryMetrics(process)
			values := make(map[string]float64)
			for name := range metrics {
				if strings.HasPrefix(name, "weir_execution_memory_") || strings.HasPrefix(name, "weir_memory_") || strings.HasPrefix(name, "weir_store_") || strings.HasPrefix(name, "weir_ingress_") || strings.HasPrefix(name, "go_memstats_") || name == "process_resident_memory_bytes" || name == "process_cpu_seconds_total" {
					values[name] = testmetrics.Sum(metrics, name)
				}
			}
			gc := metrics["go_gc_duration_seconds"].GetMetric()
			if len(gc) == 1 {
				values["go_gc_cycles"] = float64(gc[0].GetSummary().GetSampleCount())
				values["go_gc_pause_seconds"] = gc[0].GetSummary().GetSampleSum()
			}
			encoded, _ := json.Marshal(values)
			t.Logf("ingress_sample at=%s lag=%s duration=%s error=%v metrics=%s", at.UTC().Format(time.RFC3339Nano), at.Sub(scheduled), time.Since(at), err, encoded)
		}
	}
}

func ingressMemoryDocker(t *testing.T, process *memoryRecoveryCLI) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	format := `{{index .Config.Labels "weir.owner"}} {{.State.Running}} {{.State.OOMKilled}} {{.State.ExitCode}}`
	raw, err := exec.CommandContext(ctx, "docker", "inspect", "--type", "container", "--format", format, process.name).CombinedOutput()
	fields := strings.Fields(string(raw))
	t.Logf("ingress_Docker at=%s state=%s error=%v", time.Now().UTC().Format(time.RFC3339Nano), strings.TrimSpace(string(raw)), err)
	if err != nil || len(fields) != 4 || fields[0] != process.name {
		t.Error("independent Docker ownership/state unknown", err)
	} else if fields[1] != "true" || fields[2] != "false" || fields[3] != "0" {
		t.Error("CLI stopped/OOM/nonzero exit during ingress profile", fields)
	}
}

func ingressMemoryEvents(t *testing.T, process *memoryRecoveryCLI, phase string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	raw, err := exec.CommandContext(ctx, "docker", "exec", process.name, "cat", "/sys/fs/cgroup/memory.events").CombinedOutput()
	t.Logf("cgroup_memory_events phase=%s value=%q error=%v", phase, strings.TrimSpace(string(raw)), err)
}

func ingressMemoryIdle(t *testing.T, process *memoryRecoveryCLI, deadline time.Time, connections int) {
	t.Helper()
	for {
		metrics, err := ingressMemoryMetrics(process)
		idle := err == nil
		for _, suffix := range []string{"input_reserved_bytes", "input_used_bytes", "leases", "native_leases", "starting", "settled", "borrowed", "waiting", "terminal", "wire_bytes", "wire_buffers", "ingress_bytes", "ingress_reserved_bytes", "ingress_buffers", "ingress_messages", "ingress_waiting"} {
			name := "weir_execution_memory_" + suffix
			idle = idle && metrics[name] != nil && testmetrics.Sum(metrics, name) == 0
		}
		for _, name := range []string{"weir_store_pending_entries", "weir_store_pending_reserved_bytes", "weir_store_active_executions", "weir_store_paused_native_outputs", "weir_store_paused_native_reserved_bytes", "weir_store_result_reserved_entries", "weir_store_result_reserved_bytes", "weir_store_retained_result_reserved_bytes", "weir_store_working_reserved_bytes", "weir_store_publishers", "weir_ingress_sessions", "weir_ingress_queued_response_bytes", "weir_memory_latched", "weir_memory_unknown"} {
			idle = idle && metrics[name] != nil && testmetrics.Sum(metrics, name) == 0
		}
		for suffix, want := range map[string]float64{"used_bytes": float64(connections * qualificationIngressConnectionBytes), "resident_bytes": float64(connections * qualificationIngressConnectionBytes), "ingress_connections": float64(connections), "ingress_connection_bytes": float64(connections * qualificationIngressConnectionBytes)} {
			name := "weir_execution_memory_" + suffix
			idle = idle && metrics[name] != nil && testmetrics.Sum(metrics, name) == want
		}
		idle = idle && metrics["weir_execution_memory_budget_known"] != nil && testmetrics.Sum(metrics, "weir_execution_memory_budget_known") == 1 && metrics["weir_memory_cgroup_valid"] != nil && testmetrics.Sum(metrics, "weir_memory_cgroup_valid") == 1 && metrics["weir_memory_cgroup_limit_bytes"] != nil && testmetrics.Sum(metrics, "weir_memory_cgroup_limit_bytes") == process.capacity && metrics["weir_memory_cgroup_working_set_bytes"] != nil && testmetrics.Sum(metrics, "weir_memory_cgroup_working_set_bytes") <= process.capacity*.70
		if idle && time.Now().Before(deadline) {
			t.Logf("ingress_final5s temporary_Node/Store/RPC/ingress/wire/Q/I_ownership0 latch0 connections=%d connection_bytes=%d Node_used=resident=connection_bytes elapsed=%s", connections, connections*qualificationIngressConnectionBytes, time.Since(deadline.Add(-5*time.Second)))
			return
		}
		if time.Now().After(deadline) {
			t.Error("fixed absolute5s ingress ownership recovery failed", "expected_connections", connections, "expected_used_resident_connection_bytes", connections*qualificationIngressConnectionBytes, err)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Each partial upload owns an official HTTP/2 connection. Gating its request
// body leaves SETTINGS/PING/WINDOW_UPDATE processing in the HTTP/2 transport.
type ingressMemoryUpload struct {
	connection *http2.ClientConn
	observer   *ingressMemoryWire
	address    string
	frame      []byte
}

func newIngressMemoryUpload(t *testing.T, process *memoryRecoveryCLI, document ingressMemoryDocument, resource string) *ingressMemoryUpload {
	t.Helper()
	payload := &pb.Document{ContentType: "application/bson", Data: document.raw}
	action := &pb.MutateRequest_Put{Put: payload}
	mutation := &pb.MutateRequest{Resource: resource, Action: action}
	operation := &pb.Command_Mutate{Mutate: mutation}
	command := &pb.Command{Operation: operation}
	request := &pb.ExecuteRequest{StoreName: "mongo", Index: 1, Command: command}
	encoded, err := ingressMemoryRequestFrame(request)
	if err != nil {
		t.Fatal("synthetic upload is not a legal Put", err)
	}
	decoded := &pb.ExecuteRequest{}
	err = proto.Unmarshal(encoded[5:], decoded)
	if err != nil || !proto.Equal(request, decoded) || protocol.ValidateExecuteRequest(decoded) != nil || decoded.StoreName != "mongo" || decoded.Index != 1 || len(encoded)-5 != int(binary.BigEndian.Uint32(encoded[1:5])) || decoded.Command.GetMutate().Resource != resource || sha256.Sum256(decoded.Command.GetMutate().GetPut().Data) != document.digest {
		t.Fatal("gRPC frame did not round-trip the same SDK Put command/store/index/document", document.id, err)
	}
	t.Logf("partial_upload_local_protocol_valid id=%s Store=mongo index=1 Put=true message_bytes=%d BSON_bytes=%d sha256=%x END_STREAM=after_single_record_body_EOF", document.id, len(encoded)-5, len(document.raw), document.digest)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	dialer := &net.Dialer{}
	connection, err := dialer.DialContext(ctx, "tcp", process.address)
	if err != nil {
		t.Fatal(err)
	}
	observer := &ingressMemoryWire{Conn: connection, prefix: int64(len(encoded) - (16 << 10)), total: int64(len(encoded))}
	transport := &http2.Transport{}
	h2, err := transport.NewClientConn(observer)
	if err != nil {
		_ = connection.Close()
		t.Fatal(err)
	}
	upload := &ingressMemoryUpload{connection: h2, observer: observer, address: process.address, frame: encoded}
	return upload
}

type ingressMemoryBody struct {
	frame  []byte
	offset int
	ctx    context.Context
	closed chan struct{}
	once   sync.Once
}

func (body *ingressMemoryBody) Read(destination []byte) (int, error) {
	if len(destination) == 0 {
		return 0, nil
	}
	limit := len(body.frame) - (16 << 10)
	if body.offset >= limit {
		select {
		case <-body.ctx.Done():
			return 0, body.ctx.Err()
		case <-body.closed:
			return 0, io.ErrClosedPipe
		}
	}
	n := copy(destination, body.frame[body.offset:limit])
	body.offset += n
	return n, nil
}

func (body *ingressMemoryBody) Close() error {
	body.once.Do(func() { close(body.closed) })
	return nil
}

func (upload *ingressMemoryUpload) put(ctx context.Context, result *ingressMemoryResult) (*pb.MutationResult, error) {
	body := &ingressMemoryBody{frame: upload.frame, ctx: ctx, closed: make(chan struct{})}
	defer func() {
		result.DataWritten = upload.observer.written.Load()
		result.PrefixWritten = upload.observer.prefixAt.Load()
		result.AllWritten = upload.observer.allAt.Load()
	}()
	options := ingressMemoryExchangeOptions{connection: upload.connection, address: upload.address, body: body, length: int64(len(upload.frame))}
	exchange, err := ingressMemoryExchange(ctx, options)
	result.FirstResponseHeaders = exchange.headers
	result.ResponseMessages = len(exchange.messages)
	var mutation *pb.MutationResult
	if len(exchange.messages) == 1 {
		mutation = exchange.messages[0].GetEvent().GetMutationResult()
	}
	if err != nil {
		return mutation, err
	}
	if mutation == nil {
		return nil, fmt.Errorf("gRPC OK without exactly one mutation acknowledgement")
	}
	if upload.observer.invalid.Load() {
		return mutation, fmt.Errorf("unexpected outgoing HTTP/2 framing; payload byte observation invalid")
	}
	return mutation, nil
}

type ingressMemoryExchangeOptions struct {
	connection *http2.ClientConn
	address    string
	body       io.Reader
	length     int64
}

type ingressMemoryExchangeResult struct {
	messages []*pb.ExecuteResponse
	headers  time.Time
}

// Reuse the official HTTP/2 transport and protobuf codec. The response bound is
// only for this test's small health queries and mutation acknowledgements.
func ingressMemoryExchange(ctx context.Context, options ingressMemoryExchangeOptions) (ingressMemoryExchangeResult, error) {
	var result ingressMemoryExchangeResult
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+options.address+pb.StoreService_Execute_FullMethodName, options.body)
	if err != nil {
		return result, err
	}
	request.ContentLength = options.length
	request.Header.Set("content-type", "application/grpc")
	request.Header.Set("te", "trailers")
	deadline, ok := ctx.Deadline()
	if !ok {
		return result, fmt.Errorf("qualification RPC requires an absolute deadline")
	}
	remaining := max(int64(1), (time.Until(deadline).Nanoseconds()+999)/1000)
	request.Header.Set("grpc-timeout", fmt.Sprintf("%du", remaining))
	response, err := options.connection.RoundTrip(request)
	if err != nil {
		return result, err
	}
	result.headers = time.Now()
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || !strings.HasPrefix(response.Header.Get("content-type"), "application/grpc") {
		return result, fmt.Errorf("invalid gRPC HTTP response status=%d", response.StatusCode)
	}
	total := 0
	for {
		var header [5]byte
		_, readErr := io.ReadFull(response.Body, header[:])
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return result, readErr
		}
		length := binary.BigEndian.Uint32(header[1:])
		if header[0] != 0 || length > 64<<10 || total+5+int(length) > 64<<10 {
			return result, fmt.Errorf("unexpected compressed/oversized qualification response")
		}
		total += 5 + int(length)
		encoded := make([]byte, length)
		_, err = io.ReadFull(response.Body, encoded)
		if err != nil {
			return result, err
		}
		wire := &pb.ExecuteResponse{}
		if err := proto.Unmarshal(encoded, wire); err != nil {
			return result, err
		}
		if err := protocol.ValidateExecuteResponse(wire); err != nil {
			return result, err
		}
		if wire.Index != 1 {
			return result, fmt.Errorf("response differs from requested index1")
		}
		result.messages = append(result.messages, wire)
	}
	code := response.Trailer.Get("grpc-status")
	message := response.Trailer.Get("grpc-message")
	if code == "" {
		code, message = response.Header.Get("grpc-status"), response.Header.Get("grpc-message")
	}
	parsed, err := strconv.Atoi(code)
	if err != nil || parsed < 0 || parsed > 16 {
		return result, fmt.Errorf("missing/invalid final gRPC status")
	}
	if parsed != 0 {
		message, _ = url.PathUnescape(message)
		return result, status.Error(codes.Code(parsed), message)
	}
	return result, nil
}

func ingressMemoryRequestFrame(request *pb.ExecuteRequest) ([]byte, error) {
	if err := protocol.ValidateExecuteRequest(request); err != nil {
		return nil, err
	}
	frame := make([]byte, 5, proto.Size(request)+5)
	marshal := proto.MarshalOptions{}
	encoded, err := marshal.MarshalAppend(frame, request)
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint32(encoded[1:5], uint32(len(encoded)-5))
	return encoded, nil
}

// Observe only successful socket writes. Keep nine header bytes and counters,
// never a copy of DATA. Counts are local writes, not proof of server receipt.
// x/net/http2 v0.58.0 flushes the initial preface before starting readLoop;
// subsequent DATA and control writes/flushes hold ClientConn.wmu in transport.go
// (writeRequestBody, processPing, transportResponseBody.Read). Thus this parser
// has one serialized writer; only its published counters need atomics.
type ingressMemoryWire struct {
	net.Conn
	header        [9]byte
	headerBytes   int
	prefaceBytes  int
	remaining     int
	data          bool
	prefix, total int64
	written       atomic.Int64
	prefixAt      atomic.Int64
	allAt         atomic.Int64
	invalid       atomic.Bool
}

func (wire *ingressMemoryWire) Write(data []byte) (int, error) {
	n, err := wire.Conn.Write(data)
	if n > 0 {
		wire.observe(data[:n])
	}
	if err != nil {
		wire.invalid.Store(true)
	}
	return n, err
}

func (wire *ingressMemoryWire) observe(data []byte) {
	for len(data) > 0 {
		if wire.prefaceBytes < len(http2.ClientPreface) {
			n := min(len(data), len(http2.ClientPreface)-wire.prefaceBytes)
			if string(data[:n]) != http2.ClientPreface[wire.prefaceBytes:wire.prefaceBytes+n] {
				wire.invalid.Store(true)
			}
			wire.prefaceBytes += n
			data = data[n:]
			continue
		}
		if wire.headerBytes < 9 {
			n := copy(wire.header[wire.headerBytes:], data)
			wire.headerBytes += n
			data = data[n:]
			if wire.headerBytes < 9 {
				continue
			}
			wire.remaining = int(wire.header[0])<<16 | int(wire.header[1])<<8 | int(wire.header[2])
			stream := binary.BigEndian.Uint32(wire.header[5:]) & 0x7fffffff
			wire.data = wire.header[3] == byte(http2.FrameData) && stream == 1
			if wire.data && wire.header[4]&byte(http2.FlagDataPadded) != 0 {
				wire.invalid.Store(true)
			}
		}
		n := min(len(data), wire.remaining)
		if wire.data && n > 0 {
			written := wire.written.Add(int64(n))
			if written >= wire.prefix {
				wire.prefixAt.CompareAndSwap(0, time.Now().UnixNano())
			}
			if written >= wire.total {
				wire.allAt.CompareAndSwap(0, time.Now().UnixNano())
			}
		}
		wire.remaining -= n
		data = data[n:]
		if wire.remaining == 0 {
			wire.headerBytes = 0
		}
	}
}
