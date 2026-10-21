//go:build integration

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type qualificationRecord struct {
	N   int64  `json:"n" bson:"n"`
	Pad string `json:"pad" bson:"pad"`
}

type qualificationDataset struct {
	store, root, media string
	mongo              *testmongo.Fixture
	search             *testsearch.Backend
	pads               []string
	native             []*pb.NativeRequest
	expected           map[string]qualificationRecord
}

type qualificationJob struct {
	sequence, backend, kind, payload, counter int
	scheduled                                 time.Time
	phase                                     string
}

type qualificationResult struct {
	job                   qualificationJob
	latency, service, lag time.Duration
	err                   error
}

type qualificationStats struct {
	offered, completed       int
	latencies, service, lags []time.Duration
	payloads                 [3]int
	maximum                  qualificationResult
}

type qualificationSlowResult struct {
	message string
	err     error
}

// Explicit legacy Desktop reproduction retains the workload gates. Neither its
// 600-second measurement nor its 120-second smoke confers formal qualification.
func TestProductionProcessQualification(t *testing.T) {
	if os.Getenv("WEIR_PRODUCTION_QUALIFICATION") != "1" {
		t.Skip("requires explicit native Linux CLI production qualification")
	}
	execution := loadProductionExecution(t, "load")
	profile := execution.Profile()
	if profile.NativePlainLoad() || profile.NativeTLSLoad() {
		if os.Getenv("WEIR_NATIVE_LOAD") != "1" {
			t.Fatal("formal load requires the owned native Linux controller")
		}
		nativeLoadInputs(t)
		runNativeProductionLoad(t, execution)
		return
	}
	duration := profile.Duration()
	binary, digest := execution.CLIPath(), execution.CLISHA256()
	adapter := inspectLegacyDesktop(t, execution)
	mongo := testmongo.Open(t)
	verifyLegacyMongoVersion(t, mongo)
	search := testsearch.Open(t)
	t.Logf("Search service_transport=%s host_oracle=%s container=%s image=%s", adapter.searchURL(), search.URL, adapter.searchID, legacySearchImage)
	datasets := seedProductionQualificationDatasets(t, mongo, search)
	mongoBackend := &Mongo{URI: adapter.mongoURL()}
	searchBackend := &Search{URL: adapter.searchURL()}
	mongoLocal := &Local{Backend: BackendConfig{MongoDB: mongoBackend}}
	searchLocal := &Local{Backend: BackendConfig{Search: searchBackend}}
	mongoStore, searchStore := StoreConfig{Name: "mongo", Local: mongoLocal}, StoreConfig{Name: "search", Local: searchLocal}
	cfg := DefaultConfig()
	cfg.Routing.Stores = []StoreConfig{mongoStore, searchStore}
	opts := memoryRecoveryOptions{binary: binary, capacityMiB: 1024, automatic: true, config: &cfg, execution: execution}
	process := startMemoryRecoveryCLI(t, opts)
	version := packagedDocker(t, "exec", process.name, "/weir", "version")
	if err := execution.CheckVersion([]byte(version)); err != nil {
		t.Fatal("CLI version cross-check failed", err)
	}
	t.Logf("qualification=false profile=%s intent=%s duration=%s warmup=30s fixed_arrival=200RPC/s capacity=1073741824 CPUs=2 driver=%s/%s/%s engine=%s image=%s SHA256=%s version=%s", profile.ID, profile.Intent, duration, runtime.Version(), runtime.GOOS, runtime.GOARCH, adapter.engine, adapter.image, digest, strings.TrimSpace(version))
	t.Log("driver_inflight_limit=512; service MaxInFlight=16/store, queue=1024 entries/32MiB, Node Native invocations=1024/total leases=32768/shared observed byte budget, batching=128/4MiB/20ms, stall=30s unchanged")
	runProductionQualification(t, process, datasets, duration)
}

// The controller owns process lifecycle; the workload needs only its verified
// loopback endpoints and capacity through the existing sampling wrapper.
func runProductionQualification(t *testing.T, process *memoryRecoveryCLI, datasets []*qualificationDataset, duration time.Duration) {
	t.Helper()
	initial := process.sample(t)
	native := qualificationExecutionMemory(t, initial, process.capacity)
	if testmetrics.Sum(initial, "weir_memory_go_runtime_managed") != 1 {
		t.Fatal("CLI automatic memory management was not installed")
	}
	for _, dataset := range datasets {
		labels := map[string]string{"store": dataset.store}
		for name, want := range map[string]float64{"weir_store_active_executions_limit": 16, "weir_store_pending_entries_limit": 1024, "weir_store_pending_reserved_bytes_limit": 32 << 20, "weir_store_batch_max_operations": 128, "weir_store_batch_max_bytes": 4 << 20, "weir_store_batch_max_wait_seconds": .02} {
			actual := qualificationGauge(t, initial, name, labels)
			if actual != want {
				t.Fatal("qualification changed a default Store policy", dataset.store, name, want)
			}
		}
	}
	openContext, stop := context.WithTimeout(t.Context(), 3*time.Second)
	openOptions := weirclient.OpenOptions{Seed: process.address, Stores: []string{"mongo", "search"}}
	client, err := weirclient.Open(openContext, openOptions)
	stop()
	if err != nil {
		t.Fatal("real SDK Resolve initialization failed", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	workload := qualificationWorkload{process: process, client: client, datasets: datasets, duration: duration, executionRejections: native["budget_rejections_total"]}
	runQualificationWorkload(t, workload)
	for _, dataset := range datasets {
		verifyQualificationOracle(t, dataset)
	}
}

// Both execution adapters supply already-verified fixtures to the same dataset
// construction and seeding policy, including backend order and payload sizes.
func seedProductionQualificationDatasets(t *testing.T, mongo *testmongo.Fixture, search *testsearch.Backend) []*qualificationDataset {
	t.Helper()
	pads := []string{strings.Repeat("x", 1<<10), strings.Repeat("x", 256<<10), strings.Repeat("x", 2<<20), strings.Repeat("x", 8<<20)}
	datasets := []*qualificationDataset{
		{store: "mongo", root: mongo.DB + "/records", media: "application/bson", mongo: mongo, pads: pads, expected: make(map[string]qualificationRecord)},
		{store: "search", root: search.Index, media: "application/json", search: search, pads: pads, expected: make(map[string]qualificationRecord)},
	}
	for _, dataset := range datasets {
		seedQualificationDataset(t, dataset)
	}
	return datasets
}

func seedQualificationDataset(t *testing.T, dataset *qualificationDataset) {
	t.Helper()
	if dataset.search != nil {
		status, _ := dataset.search.Do(t, http.MethodPut, "/"+dataset.root+"/_mapping", `{"properties":{"pad":{"type":"keyword","index":false,"doc_values":false}}}`)
		if status != http.StatusOK {
			t.Fatal("owned large-payload mapping failed", status)
		}
	}
	for index := range 132 {
		id, n, size := fmt.Sprintf("lua-%03d", index-4), int64(0), 0
		if index < 4 {
			id, n, size = fmt.Sprintf("hot-%d", index), 1, index
		}
		record := qualificationRecord{N: n, Pad: dataset.pads[size]}
		if dataset.mongo != nil {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: n}, {Key: "pad", Value: record.Pad}}
			_, err := dataset.mongo.Admin.Database(dataset.mongo.DB).Collection("records").InsertOne(ctx, document)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
		} else {
			raw, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			status, _ := dataset.search.Do(t, http.MethodPut, "/"+dataset.root+"/_doc/"+id, string(raw))
			if status != http.StatusCreated {
				t.Fatal("owned Search seed failed", status)
			}
		}
		dataset.expected[id] = record
	}
	for payload := range 4 {
		id := fmt.Sprintf("hot-%d", payload)
		var native *pb.NativeRequest
		var err error
		if dataset.mongo != nil {
			filter := bson.D{{Key: "_id", Value: id}}
			command := bson.D{{Key: "find", Value: "records"}, {Key: "filter", Value: filter}, {Key: "limit", Value: 1}, {Key: "singleBatch", Value: true}}
			raw, marshalErr := bson.Marshal(command)
			err = marshalErr
			document := &pb.Document{ContentType: dataset.media, Data: raw}
			native = &pb.NativeRequest{Resource: dataset.mongo.DB, Request: document}
		} else {
			request, requestErr := http.NewRequest(http.MethodGet, "http://weir.invalid/"+dataset.root+"/_doc/"+id, nil)
			err = requestErr
			if err == nil {
				native, err = weirclient.NewHTTPNativeRequest("", request)
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		dataset.native = append(dataset.native, native)
	}
}

type qualificationWorkload struct {
	process             *memoryRecoveryCLI
	client              *weirclient.Client
	datasets            []*qualificationDataset
	duration            time.Duration
	ctx                 context.Context
	results             chan qualificationResult
	tokens              chan struct{}
	pending             *sync.WaitGroup
	inFlight            *atomic.Int64
	executionRejections float64
}

func runQualificationWorkload(t *testing.T, work qualificationWorkload) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), work.duration+40*time.Second)
	var pending sync.WaitGroup
	defer func() { cancel(); pending.Wait() }()
	results := make(chan qualificationResult, 512)
	tokens := make(chan struct{}, 512)
	var inFlight atomic.Int64
	work.ctx, work.results, work.tokens, work.pending, work.inFlight = ctx, results, tokens, &pending, &inFlight
	started := time.Now()
	warmup := 30 * time.Second
	measurement := started.Add(warmup)
	total := int((warmup + work.duration).Seconds()) * 200
	produceDone := make(chan struct{})
	pending.Add(1)
	go func() {
		defer pending.Done()
		defer close(produceDone)
		lua := [2]int{}
		for sequence := range total {
			job := qualificationJob{sequence: sequence, backend: (sequence / 20) % 2, scheduled: started.Add(time.Duration(sequence) * 5 * time.Millisecond), phase: "steady"}
			if job.scheduled.Before(measurement) {
				job.phase = "warmup"
			}
			position := sequence % 20
			switch {
			case position < 10:
				job.kind = 0
			case position < 14:
				job.kind = 1
			case position < 17:
				job.kind = 2
			case position < 19:
				job.kind = 3
				job.counter = lua[job.backend] % 128
				lua[job.backend]++
			default:
				job.kind = 4
			}
			random := uint64(sequence+1) * 0x9e3779b97f4a7c15
			random ^= random >> 32
			if random%1000 == 0 {
				job.payload = 2
			} else if random%1000 < 11 {
				job.payload = 1
			}
			if job.kind == 3 {
				job.payload = 0
			}
			if remaining := time.Until(job.scheduled); remaining > 0 {
				timer := time.NewTimer(remaining)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			launchQualificationJob(work, job)
		}
	}()
	slowEvents := make(chan qualificationSlowResult, 1)
	pending.Add(1)
	go func() {
		defer pending.Done()
		message, err := qualificationSlowConsumers(ctx, work, measurement)
		result := qualificationSlowResult{message: message, err: err}
		slowEvents <- result
	}()
	burstAt := measurement.Add(work.duration / 2)
	burst := false
	stats := make(map[string]*qualificationStats)
	minutes := make(map[string]*qualificationStats)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	received, expected, slowFinished := 0, total, false
	for received < expected || !slowFinished {
		select {
		case <-ctx.Done():
			reportQualificationStats(t, stats, false)
			t.Fatal("fixed workload deadline exhausted", received, expected, ctx.Err())
		case result := <-results:
			received++
			job := result.job
			key := job.phase + "/" + work.datasets[job.backend].store + "/" + qualificationMethod(job.kind)
			appendQualificationStats(stats, key, result)
			minute := int(job.scheduled.Sub(measurement) / time.Minute)
			appendQualificationStats(minutes, fmt.Sprintf("minute=%d/%s", minute, key), result)
			if result.err != nil {
				t.Logf("failed sequence=%d backend=%s method=%s payload=%d phase=%s scheduled=%s service=%s e2e=%s driver_lag=%s", job.sequence, work.datasets[job.backend].store, qualificationMethod(job.kind), job.payload, job.phase, job.scheduled.UTC().Format(time.RFC3339Nano), result.service, result.latency, result.lag)
				reportQualificationStats(t, stats, false)
				t.Fatal(result.err)
			}
			rememberQualificationMutation(work.datasets[job.backend], job)
		case result := <-slowEvents:
			slowFinished = true
			if result.err != nil {
				t.Fatal("continuous slow-consumer phase failed", result.err)
			}
			t.Logf("all 32 slow consumers canceled and joined at %s; %s", time.Since(measurement), result.message)
		case <-ticker.C:
			metrics := work.process.sample(t)
			native := qualificationExecutionMemory(t, metrics, work.process.capacity)
			if native["budget_rejections_total"] != work.executionRejections {
				t.Fatal("normal workload increased Execution memory budget rejections", native)
			}
			for _, dataset := range work.datasets {
				labels := map[string]string{"store": dataset.store}
				active := qualificationGauge(t, metrics, "weir_store_active_executions", labels)
				pendingEntries := qualificationGauge(t, metrics, "weir_store_pending_entries", labels)
				pendingBytes := qualificationGauge(t, metrics, "weir_store_pending_reserved_bytes", labels)
				if active > 16 || pendingEntries > 1024 || pendingBytes > 32<<20 {
					t.Fatal("default Store resource bounds exceeded", dataset.store, active, pendingEntries, pendingBytes)
				}
			}
			gc := metrics["go_gc_duration_seconds"].GetMetric()[0].GetSummary()
			t.Logf("trend second=%d inflight=%d RSS=%.0f working=%.0f heap=%.0f CPU=%.4f GC=%d GC_pause=%.6f pending=%.0f active=%.0f paused=%.0f pause_bytes=%.0f working_bytes=%.0f wire_bytes=%.0f sessions=%.0f latch=%.0f", int(time.Since(measurement).Seconds()), inFlight.Load(), testmetrics.Sum(metrics, "process_resident_memory_bytes"), testmetrics.Sum(metrics, "weir_memory_cgroup_working_set_bytes"), testmetrics.Sum(metrics, "go_memstats_heap_alloc_bytes"), testmetrics.Sum(metrics, "process_cpu_seconds_total"), gc.GetSampleCount(), gc.GetSampleSum(), testmetrics.Sum(metrics, "weir_store_pending_entries"), testmetrics.Sum(metrics, "weir_store_active_executions"), testmetrics.Sum(metrics, "weir_store_paused_native_outputs"), testmetrics.Sum(metrics, "weir_store_paused_native_reserved_bytes"), testmetrics.Sum(metrics, "weir_store_working_reserved_bytes"), testmetrics.Sum(metrics, "weir_ingress_queued_response_bytes"), testmetrics.Sum(metrics, "weir_ingress_sessions"), testmetrics.Sum(metrics, "weir_memory_latched"))
			t.Logf("ExecutionMemory second=%d snapshot=%v", int(time.Since(measurement).Seconds()), native)
			if !burst && !time.Now().Before(burstAt) {
				burst = true
				expected += 256
				for index := range 256 {
					job := qualificationJob{sequence: total + index, backend: index % 2, kind: index % 3, scheduled: burstAt, phase: "burst"}
					launchQualificationJob(work, job)
				}
				t.Log("offered burst=256 without changing the fixed steady arrival rate")
			}
		}
	}
	<-produceDone
	if remaining := time.Until(measurement.Add(work.duration)); remaining > 0 {
		time.Sleep(remaining)
	}
	reportQualificationStats(t, stats, true)
	reportQualificationStats(t, minutes, false)
	if err := work.client.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		metrics := work.process.sample(t)
		native := qualificationExecutionMemory(t, metrics, work.process.capacity)
		if native["budget_rejections_total"] != work.executionRejections {
			t.Fatal("Execution memory budget rejections changed during final recovery", native)
		}
		idle := testmetrics.Sum(metrics, "weir_memory_latched") == 0 && testmetrics.Sum(metrics, "weir_memory_cgroup_working_set_bytes") <= work.process.capacity*.70
		for _, name := range []string{"used_bytes", "leases", "native_leases", "resident_bytes", "input_reserved_bytes", "input_used_bytes", "wire_bytes", "wire_buffers", "starting", "settled", "borrowed", "waiting", "terminal", "ingress_bytes", "ingress_reserved_bytes", "ingress_buffers", "ingress_messages", "ingress_waiting", "ingress_connections", "ingress_connection_bytes"} {
			idle = idle && native[name] == 0
		}
		for _, name := range []string{"weir_store_pending_entries", "weir_store_pending_reserved_bytes", "weir_store_active_executions", "weir_store_paused_native_outputs", "weir_store_paused_native_reserved_bytes", "weir_store_result_reserved_entries", "weir_store_result_reserved_bytes", "weir_store_retained_result_reserved_bytes", "weir_store_working_reserved_bytes", "weir_store_publishers", "weir_ingress_sessions", "weir_ingress_queued_response_bytes"} {
			idle = idle && metrics[name] != nil && testmetrics.Sum(metrics, name) == 0
		}
		if time.Now().After(deadline) {
			t.Fatal("fixed 5s final resource recovery bound exceeded")
		}
		if idle {
			t.Logf("final all queue/execution/publication/session/byte gauges=0; Node ExecutionMemory ownership=0; known memory recovered; ExecutionMemory=%v", native)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func launchQualificationJob(work qualificationWorkload, job qualificationJob) {
	ctx, results, tokens, pending, inFlight := work.ctx, work.results, work.tokens, work.pending, work.inFlight
	select {
	case tokens <- struct{}{}:
	case <-ctx.Done():
		return
	default:
		result := qualificationResult{job: job, err: fmt.Errorf("fixed arrival exceeded driver in-flight bound; offer was not hidden")}
		select {
		case results <- result:
		case <-ctx.Done():
		}
		return
	}
	pending.Add(1)
	inFlight.Add(1)
	go func() {
		defer pending.Done()
		defer func() { <-tokens; inFlight.Add(-1) }()
		started := time.Now()
		call, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := executeQualificationJob(call, work.client, work.datasets[job.backend], job)
		cancel()
		result := qualificationResult{job: job, latency: time.Since(job.scheduled), service: time.Since(started), lag: started.Sub(job.scheduled), err: err}
		select {
		case results <- result:
		case <-ctx.Done():
		}
	}()
}

func qualificationMethod(kind int) string {
	return []string{"Read", "Put", "Native", "Lua", "Batch4"}[kind]
}

func executeQualificationJob(ctx context.Context, client *weirclient.Client, dataset *qualificationDataset, job qualificationJob) error {
	id := fmt.Sprintf("hot-%d", job.payload)
	switch job.kind {
	case 0:
		request := &weirclient.ReadRequest{Resource: dataset.root + "/s:" + id}
		opts := weirclient.ReadOneOptions{StoreName: dataset.store, Request: request}
		result, err := client.ReadOne(ctx, opts)
		if err != nil {
			return err
		}
		if result == nil || result.Failure != nil || result.Document == nil {
			return fmt.Errorf("point read failed: %v", result)
		}
		want := qualificationRecord{N: 1, Pad: dataset.pads[job.payload]}
		return validateQualificationDocument(result.Document.Data, dataset.media, want)
	case 2:
		var body []byte
		consume := func(_ context.Context, _ *weirclient.NativeResponse, chunk []byte) error {
			if len(body)+len(chunk) > 3<<20 {
				return fmt.Errorf("normal Native response exceeded expected finite bound")
			}
			body = append(body, chunk...)
			return nil
		}
		opts := weirclient.NativeOptions{StoreName: dataset.store, Request: dataset.native[job.payload], Consume: consume}
		result, err := client.Native(ctx, opts)
		if err != nil {
			return err
		}
		if result == nil || result.Failure != nil || result.Completion != weirclient.NativeResponseComplete {
			return fmt.Errorf("Native incomplete: %v", result)
		}
		var actual qualificationRecord
		if dataset.mongo != nil {
			var response struct {
				Cursor struct {
					Batch []qualificationRecord `bson:"firstBatch"`
				} `bson:"cursor"`
				OK float64 `bson:"ok"`
			}
			if err := bson.Unmarshal(body, &response); err != nil || response.OK != 1 || len(response.Cursor.Batch) != 1 {
				return fmt.Errorf("invalid native Mongo reply: %v", err)
			}
			actual = response.Cursor.Batch[0]
		} else {
			var response struct {
				Found  bool                `json:"found"`
				Source qualificationRecord `json:"_source"`
			}
			metadata, err := weirclient.ParseHTTPNativeResponse(result.Response)
			if err != nil {
				return fmt.Errorf("invalid native HTTP metadata: %w", err)
			}
			if metadata.StatusCode != http.StatusOK {
				return fmt.Errorf("native Search response: %w", testsearch.ResponseError(metadata.StatusCode, body))
			}
			if err := json.Unmarshal(body, &response); err != nil || !response.Found {
				return fmt.Errorf("invalid native Search reply: %v", err)
			}
			actual = response.Source
		}
		if actual.N != 1 || actual.Pad != dataset.pads[job.payload] {
			return fmt.Errorf("Native lost immutable document content")
		}
		return nil
	case 3:
		lua := &weirclient.LuaTransform{Source: []byte(`return function(current, incoming) current.n = current.n + 1; return current end`)}
		request := &weirclient.AtomicTransformRequest{Resource: fmt.Sprintf("%s/s:lua-%03d", dataset.root, job.counter), Lua: lua}
		opts := weirclient.AtomicTransformOptions{StoreName: dataset.store, Request: request}
		result, err := client.AtomicTransform(ctx, opts)
		return qualificationMutationEvidence(result, err)
	default:
		count := 1
		if job.kind == 4 {
			count = 4
		}
		requests := make([]*weirclient.MutateRequest, count)
		for index := range count {
			id := qualificationWriteID(job, index)
			record := qualificationRecord{N: int64(job.sequence + 1000), Pad: dataset.pads[job.payload]}
			var raw []byte
			var err error
			if dataset.mongo != nil {
				document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: record.N}, {Key: "pad", Value: record.Pad}}
				raw, err = bson.Marshal(document)
			} else {
				raw, err = json.Marshal(record)
			}
			if err != nil {
				return err
			}
			document := &pb.Document{ContentType: dataset.media, Data: raw}
			requests[index] = &weirclient.MutateRequest{Resource: dataset.root + "/s:" + id, Action: weirclient.MutationPut, Document: document}
		}
		if job.kind == 1 {
			request := &weirclient.WriteRequest{Resource: requests[0].Resource, Document: requests[0].Document}
			opts := weirclient.WriteOptions{StoreName: dataset.store, Request: request}
			result, err := client.Put(ctx, opts)
			return qualificationMutationEvidence(result, err)
		}
		opts := weirclient.MutateOptions{StoreName: dataset.store, Requests: requests}
		results, err := client.Mutate(ctx, opts)
		if err != nil {
			return err
		}
		if len(results) != count {
			return fmt.Errorf("bulk lost per-record results")
		}
		for _, result := range results {
			if err := qualificationMutationEvidence(result, nil); err != nil {
				return err
			}
		}
		return nil
	}
}

func qualificationMutationEvidence(result *weirclient.MutationResult, err error) error {
	if err != nil {
		return err
	}
	if result == nil || result.Failure != nil || result.Outcome != weirclient.MutationApplied {
		return fmt.Errorf("mutation lacked application evidence: %v", result)
	}
	return nil
}

func qualificationWriteID(job qualificationJob, index int) string {
	return fmt.Sprintf("write-%09d-%d", job.sequence, index)
}

func rememberQualificationMutation(dataset *qualificationDataset, job qualificationJob) {
	if job.kind == 3 {
		id := fmt.Sprintf("lua-%03d", job.counter)
		record := dataset.expected[id]
		record.N++
		dataset.expected[id] = record
	} else if job.kind == 1 || job.kind == 4 {
		count := 1
		if job.kind == 4 {
			count = 4
		}
		for index := range count {
			dataset.expected[qualificationWriteID(job, index)] = qualificationRecord{N: int64(job.sequence + 1000), Pad: dataset.pads[job.payload]}
		}
	}
}

func validateQualificationDocument(raw []byte, media string, want qualificationRecord) error {
	var actual qualificationRecord
	var err error
	if media == "application/bson" {
		err = bson.Unmarshal(raw, &actual)
	} else {
		err = json.Unmarshal(raw, &actual)
	}
	if err != nil {
		return err
	}
	if actual.N != want.N || actual.Pad != want.Pad {
		return fmt.Errorf("document content differs from acknowledged effect")
	}
	return nil
}

func appendQualificationStats(stats map[string]*qualificationStats, key string, result qualificationResult) {
	entry := stats[key]
	if entry == nil {
		entry = &qualificationStats{}
		stats[key] = entry
	}
	entry.offered++
	if result.err == nil {
		entry.completed++
	}
	entry.latencies = append(entry.latencies, result.latency)
	entry.service = append(entry.service, result.service)
	entry.lags = append(entry.lags, result.lag)
	entry.payloads[result.job.payload]++
	if result.latency > entry.maximum.latency {
		entry.maximum = result
	}
}

func reportQualificationStats(t *testing.T, stats map[string]*qualificationStats, gate bool) {
	t.Helper()
	keys := make([]string, 0, len(stats))
	for key := range stats {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		entry := stats[key]
		slices.Sort(entry.latencies)
		slices.Sort(entry.service)
		slices.Sort(entry.lags)
		n := len(entry.latencies)
		p99 := entry.latencies[(n*99+99)/100-1]
		recordsPerRPC := 1
		if strings.HasSuffix(key, "/Batch4") {
			recordsPerRPC = 4
		}
		job := entry.maximum.job
		t.Logf("distribution %s offered=%d completed=%d records_per_RPC=%d payload_counts_1KiB_256KiB_2MiB=%v p50=%s p95=%s p99=%s max=%s service_p99=%s driver_lag_p99=%s driver_lag_max=%s max_sequence=%d max_payload=%d max_scheduled=%s", key, entry.offered, entry.completed, recordsPerRPC, entry.payloads, entry.latencies[(n*50+99)/100-1], entry.latencies[(n*95+99)/100-1], p99, entry.latencies[n-1], entry.service[(n*99+99)/100-1], entry.lags[(n*99+99)/100-1], entry.lags[n-1], job.sequence, job.payload, job.scheduled.UTC().Format(time.RFC3339Nano))
		limit := 250 * time.Millisecond
		if strings.HasSuffix(key, "/Read") {
			limit = 100 * time.Millisecond
		}
		if strings.HasSuffix(key, "/Lua") || strings.HasSuffix(key, "/Batch4") {
			limit = 500 * time.Millisecond
		}
		if gate && strings.HasPrefix(key, "steady/") && p99 > limit {
			t.Errorf("predeclared local p99 gate failed for %s: %s > %s", key, p99, limit)
		}
		if gate && entry.latencies[n-1] > 3*time.Second {
			t.Errorf("fixed end-to-end 3s budget exceeded for %s: %s", key, entry.latencies[n-1])
		}
	}
}

type qualificationStreamContextKey int

const qualificationStreamKey qualificationStreamContextKey = 0

// InPayload observes completed receive/decode, not raw HTTP/2 arrival. Records
// contain only timings, message classes and lengths, never payloads or IDs.
type qualificationReceiveRecord struct {
	Kind               string
	Offset             time.Duration
	Length, WireLength int
}

type qualificationReceiveStream struct {
	mu                              sync.Mutex
	counts                          [4]int64 // head, chunk, native end, other
	records                         [16]qualificationReceiveRecord
	next                            uint64
	lastChunk, lastReturn           time.Duration
	hasChunk, hasReturn             bool
	attributed                      int64
	maxGap, preReceive, postReceive time.Duration
}

type qualificationReceiveStats struct {
	start   time.Time
	streams [32]qualificationReceiveStream
}

func (s *qualificationReceiveStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}
func (s *qualificationReceiveStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (s *qualificationReceiveStats) HandleConn(context.Context, stats.ConnStats) {}

func (s *qualificationReceiveStats) record(ctx context.Context, record qualificationReceiveRecord, class int) {
	index, ok := ctx.Value(qualificationStreamKey).(int)
	if !ok || index < 0 || index >= len(s.streams) {
		return
	}
	stream := &s.streams[index]
	stream.mu.Lock()
	if class >= 0 {
		stream.counts[class]++
	}
	if record.Kind == "recv_chunk" {
		stream.lastChunk, stream.hasChunk = record.Offset, true
	}
	if record.Kind == "callback_enter" && stream.hasChunk {
		if stream.hasReturn && stream.lastChunk >= stream.lastReturn && record.Offset >= stream.lastChunk {
			stream.attributed++
			gap := record.Offset - stream.lastReturn
			if gap > stream.maxGap {
				stream.maxGap, stream.preReceive, stream.postReceive = gap, stream.lastChunk-stream.lastReturn, record.Offset-stream.lastChunk
			}
		}
		stream.hasChunk = false
	}
	if record.Kind == "callback_return" {
		stream.lastReturn, stream.hasReturn = record.Offset, true
	}
	stream.records[stream.next%uint64(len(stream.records))] = record
	stream.next++
	stream.mu.Unlock()
}

func (s *qualificationReceiveStats) HandleRPC(ctx context.Context, event stats.RPCStats) {
	payload, ok := event.(*stats.InPayload)
	if !ok {
		return
	}
	kind, class := "recv_other", 3
	if response, ok := payload.Payload.(*pb.ExecuteResponse); ok && response != nil && response.Event != nil {
		switch response.Event.Value.(type) {
		case *pb.Event_Head:
			kind, class = "recv_head", 0
		case *pb.Event_Chunk:
			kind, class = "recv_chunk", 1
		case *pb.Event_NativeEnd:
			kind, class = "recv_native_end", 2
		}
	}
	record := qualificationReceiveRecord{Kind: kind, Offset: payload.RecvTime.Sub(s.start), Length: payload.Length, WireLength: payload.WireLength}
	s.record(ctx, record, class)
}

func (s *qualificationReceiveStats) callback(ctx context.Context, kind string, at time.Time) {
	record := qualificationReceiveRecord{Kind: kind, Offset: at.Sub(s.start)}
	s.record(ctx, record, -1)
}

func (s *qualificationReceiveStats) summary(index int) string {
	stream := &s.streams[index]
	stream.mu.Lock()
	counts, records, next := stream.counts, stream.records, stream.next
	attributed, gap, pre, post := stream.attributed, stream.maxGap, stream.preReceive, stream.postReceive
	stream.mu.Unlock()
	ordered := make([]qualificationReceiveRecord, 0, len(records))
	for n := next - min(next, uint64(len(records))); n < next; n++ {
		ordered = append(ordered, records[n%uint64(len(records))])
	}
	return fmt.Sprintf("completed_receive_counts_head_chunk_native_end_other=%v attributed_callback_gaps=%d max_attributed_gap=%s return_to_completed_receive=%s completed_receive_to_callback=%s boundary_recent=%+v boundary_note=completed_receive_decode_not_raw_HTTP2_arrival", counts, attributed, gap, pre, post, ordered)
}

type qualificationStreamFailure struct {
	index int
	err   error
}

// Only the first failure is needed; all 32 workers and the monitor must be
// able to finish before the coordinator reads the bounded channel.
func publishQualificationStreamFailure(failures chan<- qualificationStreamFailure, failure qualificationStreamFailure) {
	select {
	case failures <- failure:
	default:
	}
}

func qualificationNativeFailure(index int, result *weirclient.NativeResult, err error) qualificationStreamFailure {
	completion := weirclient.NativeCompletionUnconfirmed
	failureCode := pb.FailureCode_FAILURE_CODE_UNSPECIFIED
	failurePresent := result != nil && result.Failure != nil
	if result != nil {
		completion = result.Completion
		if failurePresent {
			failureCode = result.Failure.Code
		}
	}
	// Print only status/enum values and presence flags, not error messages,
	// response metadata, bodies, or backend failure messages.
	failure := qualificationStreamFailure{
		index: index,
		err:   fmt.Errorf("slow stream %d failed: grpc_code=%s result_present=%t native_completion=%d failure_present=%t failure_code=%d", index, status.Code(err), result != nil, completion, failurePresent, failureCode),
	}
	return failure
}

func qualificationSlowConsumers(ctx context.Context, work qualificationWorkload, measurement time.Time) (string, error) {
	start := measurement.Add(work.duration / 4)
	if remaining := time.Until(start); remaining > 0 {
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	duration := min(120*time.Second, work.duration/2)
	phase, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	receiveStats := &qualificationReceiveStats{start: start}
	connection, err := grpc.NewClient("passthrough:///"+work.process.address, grpc.WithStatsHandler(receiveStats), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithStaticStreamWindowSize(64<<10), grpc.WithStaticConnWindowSize(4<<20))
	if err != nil {
		return "", err
	}
	defer connection.Close()
	client := pb.NewStoreServiceClient(connection)
	failures := make(chan qualificationStreamFailure, 1)
	progress := make([]atomic.Int64, 32)
	callbacks := make([]atomic.Int64, 32)
	lastProgress := make([]atomic.Int64, 32)
	type streamTiming struct {
		waitCount, gapCount         int64
		waitSum, waitMax            time.Duration
		gapSum, gapMax              time.Duration
		firstCallback, lastCallback time.Duration
		lastReturn                  time.Time
	}
	// Each worker owns its timing counters; inspect them only after pending.Wait.
	timings := make([]streamTiming, 32)
	var offered, completed, maximumGap atomic.Int64
	var pending sync.WaitGroup
	for index := range 32 {
		pending.Add(1)
		go func() {
			defer pending.Done()
			rpcContext := context.WithValue(phase, qualificationStreamKey, index)
			dataset := work.datasets[index%2]
			for phase.Err() == nil {
				offered.Add(1)
				consume := func(ctx context.Context, _ *weirclient.NativeResponse, chunk []byte) error {
					callbackStart := time.Now()
					receiveStats.callback(rpcContext, "callback_enter", callbackStart)
					timing := &timings[index]
					if timing.lastReturn.IsZero() {
						timing.firstCallback = callbackStart.Sub(start)
					} else {
						gap := callbackStart.Sub(timing.lastReturn)
						timing.gapCount++
						timing.gapSum += gap
						timing.gapMax = max(timing.gapMax, gap)
					}
					timing.lastCallback = callbackStart.Sub(start)
					defer func() {
						timing.lastReturn = time.Now()
						receiveStats.callback(rpcContext, "callback_return", timing.lastReturn)
					}()
					progress[index].Add(int64(len(chunk)))
					callbacks[index].Add(1)
					previous := lastProgress[index].Swap(time.Now().UnixNano())
					if previous != 0 {
						gap := time.Now().UnixNano() - previous
						for observed := maximumGap.Load(); gap > observed; observed = maximumGap.Load() {
							if maximumGap.CompareAndSwap(observed, gap) {
								break
							}
						}
					}
					timer := time.NewTimer(500 * time.Millisecond)
					defer timer.Stop()
					waitStart := time.Now()
					var waitErr error
					select {
					case <-ctx.Done():
						waitErr = ctx.Err()
					case <-timer.C:
					}
					wait := time.Since(waitStart)
					timing.waitCount++
					timing.waitSum += wait
					timing.waitMax = max(timing.waitMax, wait)
					return waitErr
				}
				opts := weirclient.NativeOptions{StoreName: dataset.store, Request: dataset.native[3], Consume: consume}
				result, err := weirclient.Native(rpcContext, client, opts)
				if phase.Err() != nil {
					return
				}
				if err != nil || result == nil || result.Failure != nil || result.Completion != weirclient.NativeResponseComplete {
					failure := qualificationNativeFailure(index, result, err)
					publishQualificationStreamFailure(failures, failure)
					cancel()
					return
				}
				completed.Add(1)
			}
		}()
	}
	monitor := time.NewTicker(time.Second)
	defer monitor.Stop()
	observing := true
	for observing {
		select {
		case <-phase.Done():
			observing = false
		case <-monitor.C:
			if time.Since(start) < 3*time.Second {
				continue
			}
			for index := range lastProgress {
				previous := lastProgress[index].Load()
				if previous == 0 || time.Since(time.Unix(0, previous)) > 2*time.Second {
					failure := qualificationStreamFailure{index: index, err: fmt.Errorf("slow stream %d lost continuous progress for over 2s", index)}
					publishQualificationStreamFailure(failures, failure)
					cancel()
					observing = false
					break
				}
			}
		}
	}
	pending.Wait()
	select {
	case failure := <-failures:
		return "", fmt.Errorf("%w: %s", failure.err, receiveStats.summary(failure.index))
	default:
	}
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	minimumCallbacks, minimumBytes := int64(1<<62), int64(1<<62)
	for index := range progress {
		if callbacks[index].Load() < int64(duration/(500*time.Millisecond)-8) {
			timing := timings[index]
			return "", fmt.Errorf("slow stream %d failed to demonstrate continued chunk delivery: callbacks=%d bytes=%d callback_wait_count=%d callback_wait_sum=%s callback_wait_max=%s callback_return_gap_count=%d callback_return_gap_sum=%s callback_return_gap_max=%s first_callback_offset=%s last_callback_offset=%s %s", index, callbacks[index].Load(), progress[index].Load(), timing.waitCount, timing.waitSum, timing.waitMax, timing.gapCount, timing.gapSum, timing.gapMax, timing.firstCallback, timing.lastCallback, receiveStats.summary(index))
		}
		minimumCallbacks = min(minimumCallbacks, callbacks[index].Load())
		minimumBytes = min(minimumBytes, progress[index].Load())
	}
	message := fmt.Sprintf("slow_duration=%s offered=%d completed=%d canceled=%d minimum_callbacks_per_stream=%d minimum_bytes_per_stream=%d maximum_progress_gap=%s", duration, offered.Load(), completed.Load(), offered.Load()-completed.Load(), minimumCallbacks, minimumBytes, time.Duration(maximumGap.Load()))
	return message, nil
}

func verifyQualificationOracle(t *testing.T, dataset *qualificationDataset) {
	t.Helper()
	seen := make(map[string]bool, len(dataset.expected))
	if dataset.mongo != nil {
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		filter := bson.D{}
		cursor, err := dataset.mongo.Admin.Database(dataset.mongo.DB).Collection("records").Find(ctx, filter)
		if err != nil {
			t.Fatal(err)
		}
		defer cursor.Close(ctx)
		for cursor.Next(ctx) {
			id, valid := cursor.Current.Lookup("_id").StringValueOK()
			if !valid {
				t.Fatal("independent MongoDB oracle found an unexpected identity type")
			}
			want, exists := dataset.expected[id]
			if !exists {
				t.Fatal("unacknowledged MongoDB effect", id)
			}
			if err := validateQualificationDocument(cursor.Current, dataset.media, want); err != nil {
				t.Fatal("independent MongoDB oracle", id, err)
			}
			seen[id] = true
		}
		if err := cursor.Err(); err != nil {
			t.Fatal(err)
		}
	} else {
		ids := make([]string, 0, len(dataset.expected))
		for id := range dataset.expected {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		for begin := 0; begin < len(ids); begin += 250 {
			request := struct {
				IDs []string `json:"ids"`
			}{IDs: ids[begin:min(begin+250, len(ids))]}
			raw, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			httpRequest, err := http.NewRequest(http.MethodPost, dataset.search.URL+"/"+dataset.root+"/_mget", bytes.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			if dataset.search.Username != "" {
				httpRequest.SetBasicAuth(dataset.search.Username, dataset.search.Password)
			}
			httpRequest.Header.Set("Content-Type", "application/json")
			response, err := dataset.search.Client.Do(httpRequest)
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(io.LimitReader(response.Body, 64<<20))
			_ = response.Body.Close()
			if readErr != nil || response.StatusCode != http.StatusOK {
				t.Fatal("independent Search oracle read failed", readErr, response.StatusCode)
			}
			var reply struct {
				Docs []struct {
					ID     string              `json:"_id"`
					Found  bool                `json:"found"`
					Source qualificationRecord `json:"_source"`
				} `json:"docs"`
			}
			if err := json.Unmarshal(body, &reply); err != nil {
				t.Fatal(err)
			}
			if len(reply.Docs) != len(request.IDs) {
				t.Fatal("independent Search oracle lost per-ID results", len(reply.Docs), len(request.IDs))
			}
			requested := make(map[string]bool, len(request.IDs))
			for _, id := range request.IDs {
				requested[id] = true
			}
			for _, document := range reply.Docs {
				want, exists := dataset.expected[document.ID]
				if !requested[document.ID] || seen[document.ID] || !exists || !document.Found || document.Source != want {
					t.Fatal("independent Search oracle differs", document.ID)
				}
				seen[document.ID] = true
			}
		}
		status, _ := dataset.search.Do(t, http.MethodPost, "/"+dataset.root+"/_refresh", "")
		if status != http.StatusOK {
			t.Fatal("independent Search oracle refresh failed", status)
		}
		status, body := dataset.search.Do(t, http.MethodGet, "/"+dataset.root+"/_count", "")
		var count struct {
			Count int `json:"count"`
		}
		if status != http.StatusOK || json.Unmarshal(body, &count) != nil || count.Count != len(dataset.expected) {
			t.Fatal("independent Search oracle found unacknowledged effects", count.Count, len(dataset.expected))
		}
	}
	if len(seen) != len(dataset.expected) {
		t.Fatal("independent oracle lost acknowledged effects", dataset.store, len(seen), len(dataset.expected))
	}
	t.Logf("independent oracle %s verified_all_documents=%d including every acknowledged write/bulk/Lua effect", dataset.store, len(seen))
}
