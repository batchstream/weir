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
	dto "github.com/prometheus/client_model/go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type loadRecord struct {
	N   int64  `json:"n" bson:"n"`
	Pad string `json:"pad" bson:"pad"`
}

type loadDataset struct {
	store, root, media string
	mongo              *testmongo.Fixture
	search             *testsearch.Backend
	pads               []string
	native             []*pb.NativeRequest
	expected           map[string]loadRecord
}

type loadJob struct {
	sequence, backend, kind, payload, counter int
	scheduled                                 time.Time
	phase                                     string
}

type loadResult struct {
	job                   loadJob
	latency, service, lag time.Duration
	err                   error
}

type loadStats struct {
	offered, completed       int
	latencies, service, lags []time.Duration
	payloads                 [3]int
	maximum                  loadResult
}

type loadSlowResult struct {
	message string
	err     error
}

// TestProcessLoad drives a fixed-arrival SDK workload with slow consumers and
// a burst through one CLI container that keeps the default Store policy. It
// gates latency, resource bounds and recovery, then checks every acknowledged
// effect against the backends. It needs Docker, scripts/mongo-local.sh
// and scripts/search-local.sh.
func TestProcessLoad(t *testing.T) {
	if os.Getenv("WEIR_LOAD_INTEGRATION") != "1" {
		t.Skip("requires WEIR_LOAD_INTEGRATION=1, Docker and the local Mongo and Search fixtures")
	}
	duration := loadMeasurement(t)
	mongo := testmongo.Open(t)
	verifyMongoVersion(t, mongo)
	search := testsearch.Open(t)
	searchURL := searchContainerURL(t, search)
	binary := buildLinuxCLI(t)
	t.Logf("Search service_transport=%s host_oracle=%s", searchURL, search.URL)
	datasets := seedLoadDatasets(t, mongo, search)
	mongoBackend := &MongoDB{URI: containerMongoURI}
	searchBackend := &Search{URL: searchURL}
	mongoStore := StoreConfig{Name: "mongo", MongoDB: mongoBackend}
	searchStore := StoreConfig{Name: "search", Search: searchBackend}
	cfg := Config{}
	cfg.Stores = []StoreConfig{mongoStore, searchStore}
	opts := cliContainerOptions{binary: binary, capacityMiB: 1024, config: cfg}
	process := startCLIContainer(t, opts)
	t.Logf("measurement=%s warmup=30s fixed_arrival=200RPC/s capacity=1073741824 CPUs=2 driver=%s/%s/%s", duration, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	t.Log("driver_inflight_limit=512; service MaxInFlight=16/store, Native=64/store, queue=1024 entries/32MiB, batching=128/4MiB/1ms, stall=30s unchanged")
	runProcessLoad(t, process, datasets, duration)
}

// loadMeasurement is 600s unless WEIR_LOAD_MEASUREMENT selects a shorter run.
func loadMeasurement(t *testing.T) time.Duration {
	t.Helper()
	value := os.Getenv("WEIR_LOAD_MEASUREMENT")
	if value == "" {
		return 600 * time.Second
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < time.Minute {
		t.Fatal("WEIR_LOAD_MEASUREMENT must be a duration of at least 1m", value, err)
	}
	return duration
}

func verifyMongoVersion(t *testing.T, fixture *testmongo.Fixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	command := bson.D{{Key: "buildInfo", Value: 1}}
	var result struct {
		Version string `bson:"version"`
	}
	err := fixture.Admin.Database("admin").RunCommand(ctx, command).Decode(&result)
	if err != nil || result.Version != "8.0.32" {
		t.Fatal("local MongoDB fixture version mismatch", result.Version, err)
	}
}

func runProcessLoad(t *testing.T, process *cliContainer, datasets []*loadDataset, duration time.Duration) {
	t.Helper()
	initial := process.sample(t)
	for _, dataset := range datasets {
		labels := map[string]string{"store": dataset.store}
		limit := requireGauge(t, initial, "weir_store_active_executions_limit", labels)
		if limit != 16 {
			t.Fatal("load test changed the default Store execution limit", dataset.store, limit)
		}
	}
	client, err := weirclient.New(process.address)
	if err != nil {
		t.Fatal("SDK client", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	workload := loadWorkload{process: process, client: client, datasets: datasets, duration: duration, rejections: loadRejections(initial)}
	runLoadWorkload(t, workload)
	for _, dataset := range datasets {
		verifyLoadOracle(t, dataset)
	}
}

func seedLoadDatasets(t *testing.T, mongo *testmongo.Fixture, search *testsearch.Backend) []*loadDataset {
	t.Helper()
	pads := []string{strings.Repeat("x", 1<<10), strings.Repeat("x", 256<<10), strings.Repeat("x", 2<<20), strings.Repeat("x", 8<<20)}
	datasets := []*loadDataset{
		{store: "mongo", root: mongo.DB + "/records", media: "application/bson", mongo: mongo, pads: pads, expected: make(map[string]loadRecord)},
		{store: "search", root: search.Index, media: "application/json", search: search, pads: pads, expected: make(map[string]loadRecord)},
	}
	for _, dataset := range datasets {
		seedLoadDataset(t, dataset)
	}
	return datasets
}

func seedLoadDataset(t *testing.T, dataset *loadDataset) {
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
		record := loadRecord{N: n, Pad: dataset.pads[size]}
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

type loadWorkload struct {
	process    *cliContainer
	client     *weirclient.Client
	datasets   []*loadDataset
	duration   time.Duration
	ctx        context.Context
	results    chan loadResult
	tokens     chan struct{}
	pending    *sync.WaitGroup
	inFlight   *atomic.Int64
	rejections float64
}

func runLoadWorkload(t *testing.T, work loadWorkload) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), work.duration+40*time.Second)
	var pending sync.WaitGroup
	defer func() { cancel(); pending.Wait() }()
	results := make(chan loadResult, 512)
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
			job := loadJob{sequence: sequence, backend: (sequence / 20) % 2, scheduled: started.Add(time.Duration(sequence) * 5 * time.Millisecond), phase: "steady"}
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
			launchLoadJob(work, job)
		}
	}()
	slowEvents := make(chan loadSlowResult, 1)
	pending.Add(1)
	go func() {
		defer pending.Done()
		message, err := runSlowConsumers(ctx, work, measurement)
		result := loadSlowResult{message: message, err: err}
		slowEvents <- result
	}()
	burstAt := measurement.Add(work.duration / 2)
	burst := false
	stats := make(map[string]*loadStats)
	minutes := make(map[string]*loadStats)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	received, expected, slowFinished := 0, total, false
	for received < expected || !slowFinished {
		select {
		case <-ctx.Done():
			reportLoadStats(t, stats, false)
			t.Fatal("fixed workload deadline exhausted", received, expected, ctx.Err())
		case result := <-results:
			received++
			job := result.job
			key := job.phase + "/" + work.datasets[job.backend].store + "/" + loadMethod(job.kind)
			appendLoadStats(stats, key, result)
			minute := int(job.scheduled.Sub(measurement) / time.Minute)
			appendLoadStats(minutes, fmt.Sprintf("minute=%d/%s", minute, key), result)
			if result.err != nil {
				t.Logf("failed sequence=%d backend=%s method=%s payload=%d phase=%s service=%s e2e=%s driver_lag=%s", job.sequence, work.datasets[job.backend].store, loadMethod(job.kind), job.payload, job.phase, result.service, result.latency, result.lag)
				reportLoadStats(t, stats, false)
				t.Fatal(result.err)
			}
			rememberLoadMutation(work.datasets[job.backend], job)
		case result := <-slowEvents:
			slowFinished = true
			if result.err != nil {
				t.Fatal("continuous slow-consumer phase failed", result.err)
			}
			t.Logf("all 32 slow consumers canceled and joined at %s; %s", time.Since(measurement), result.message)
		case <-ticker.C:
			metrics := work.process.sample(t)
			if rejections := loadRejections(metrics); rejections != work.rejections {
				t.Fatal("normal workload was rejected", rejections-work.rejections)
			}
			for _, dataset := range work.datasets {
				labels := map[string]string{"store": dataset.store}
				active := requireGauge(t, metrics, "weir_store_active_executions", labels)
				queued := requireGauge(t, metrics, "weir_store_queued_operations", labels)
				queuedBytes := requireGauge(t, metrics, "weir_store_queued_bytes", labels)
				if active > 16 || queued > 1024 || queuedBytes > 32<<20 {
					t.Fatal("default Store resource bounds exceeded", dataset.store, active, queued, queuedBytes)
				}
			}
			gc := metrics["go_gc_duration_seconds"].GetMetric()[0].GetSummary()
			t.Logf("trend second=%d inflight=%d RSS=%.0f live=%.0f heap=%.0f CPU=%.4f GC=%d GC_pause=%.6f queued=%.0f active=%.0f sessions=%.0f overloaded=%.0f", int(time.Since(measurement).Seconds()), inFlight.Load(), testmetrics.Sum(metrics, "process_resident_memory_bytes"), testmetrics.Sum(metrics, "weir_memory_live_bytes"), testmetrics.Sum(metrics, "go_memstats_heap_alloc_bytes"), testmetrics.Sum(metrics, "process_cpu_seconds_total"), gc.GetSampleCount(), gc.GetSampleSum(), testmetrics.Sum(metrics, "weir_store_queued_operations"), testmetrics.Sum(metrics, "weir_store_active_executions"), testmetrics.Sum(metrics, "weir_ingress_sessions"), testmetrics.Sum(metrics, "weir_memory_overloaded"))
			if !burst && !time.Now().Before(burstAt) {
				burst = true
				expected += 256
				for index := range 256 {
					job := loadJob{sequence: total + index, backend: index % 2, kind: index % 3, scheduled: burstAt, phase: "burst"}
					launchLoadJob(work, job)
				}
				t.Log("offered burst=256 without changing the fixed steady arrival rate")
			}
		}
	}
	<-produceDone
	if remaining := time.Until(measurement.Add(work.duration)); remaining > 0 {
		time.Sleep(remaining)
	}
	reportLoadStats(t, stats, true)
	reportLoadStats(t, minutes, false)
	if err := work.client.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		metrics := work.process.sample(t)
		if rejections := loadRejections(metrics); rejections != work.rejections {
			t.Fatal("work was rejected during final recovery", rejections-work.rejections)
		}
		idle := testmetrics.Sum(metrics, "weir_memory_overloaded") == 0
		for _, name := range []string{"weir_store_queued_operations", "weir_store_queued_bytes", "weir_store_active_executions", "weir_store_active_natives", "weir_ingress_sessions"} {
			idle = idle && metrics[name] != nil && testmetrics.Sum(metrics, name) == 0
		}
		if time.Now().After(deadline) {
			t.Fatal("fixed 5s final resource recovery bound exceeded")
		}
		if idle {
			t.Log("final queue, execution and session gauges=0; memory recovered")
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// loadRejections counts work refused before execution.
func loadRejections(metrics map[string]*dto.MetricFamily) float64 {
	rejections := testmetrics.Sum(metrics, "weir_store_rejections_total") + testmetrics.Sum(metrics, "weir_admission_rejections_total")
	return rejections
}

func requireGauge(t *testing.T, metrics map[string]*dto.MetricFamily, name string, labels map[string]string) float64 {
	t.Helper()
	metric := testmetrics.Sample(metrics, name, labels)
	if metric == nil || metric.Gauge == nil {
		t.Fatal("required gauge is missing", name, labels)
	}
	value := metric.GetGauge().GetValue()
	return value
}

func launchLoadJob(work loadWorkload, job loadJob) {
	ctx, results, tokens, pending, inFlight := work.ctx, work.results, work.tokens, work.pending, work.inFlight
	select {
	case tokens <- struct{}{}:
	case <-ctx.Done():
		return
	default:
		result := loadResult{job: job, err: fmt.Errorf("fixed arrival exceeded driver in-flight bound; offer was not hidden")}
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
		err := executeLoadJob(call, work.client, work.datasets[job.backend], job)
		cancel()
		result := loadResult{job: job, latency: time.Since(job.scheduled), service: time.Since(started), lag: started.Sub(job.scheduled), err: err}
		select {
		case results <- result:
		case <-ctx.Done():
		}
	}()
}

func loadMethod(kind int) string {
	return []string{"Read", "Put", "Native", "Lua", "Batch4"}[kind]
}

func executeLoadJob(ctx context.Context, client *weirclient.Client, dataset *loadDataset, job loadJob) error {
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
		want := loadRecord{N: 1, Pad: dataset.pads[job.payload]}
		return validateLoadDocument(result.Document.Data, dataset.media, want)
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
		var actual loadRecord
		if dataset.mongo != nil {
			var response struct {
				Cursor struct {
					Batch []loadRecord `bson:"firstBatch"`
				} `bson:"cursor"`
				OK float64 `bson:"ok"`
			}
			if err := bson.Unmarshal(body, &response); err != nil || response.OK != 1 || len(response.Cursor.Batch) != 1 {
				return fmt.Errorf("invalid native Mongo reply: %v", err)
			}
			actual = response.Cursor.Batch[0]
		} else {
			var response struct {
				Found  bool       `json:"found"`
				Source loadRecord `json:"_source"`
			}
			metadata, err := weirclient.ParseHTTPNativeResponse(result.Response)
			if err != nil {
				return fmt.Errorf("invalid native HTTP metadata: %w", err)
			}
			if metadata.StatusCode != http.StatusOK {
				return fmt.Errorf("native HTTP status %d: %.512s", metadata.StatusCode, body)
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
		return requireMutationApplied(result, err)
	default:
		count := 1
		if job.kind == 4 {
			count = 4
		}
		requests := make([]*weirclient.MutateRequest, count)
		for index := range count {
			id := loadWriteID(job, index)
			record := loadRecord{N: int64(job.sequence + 1000), Pad: dataset.pads[job.payload]}
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
			return requireMutationApplied(result, err)
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
			if err := requireMutationApplied(result, nil); err != nil {
				return err
			}
		}
		return nil
	}
}

func requireMutationApplied(result *weirclient.MutationResult, err error) error {
	if err != nil {
		return err
	}
	if result == nil || result.Failure != nil || result.Outcome != weirclient.MutationApplied {
		return fmt.Errorf("mutation lacked application evidence: %v", result)
	}
	return nil
}

func loadWriteID(job loadJob, index int) string {
	return fmt.Sprintf("write-%09d-%d", job.sequence, index)
}

func rememberLoadMutation(dataset *loadDataset, job loadJob) {
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
			dataset.expected[loadWriteID(job, index)] = loadRecord{N: int64(job.sequence + 1000), Pad: dataset.pads[job.payload]}
		}
	}
}

func validateLoadDocument(raw []byte, media string, want loadRecord) error {
	var actual loadRecord
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

func appendLoadStats(stats map[string]*loadStats, key string, result loadResult) {
	entry := stats[key]
	if entry == nil {
		entry = &loadStats{}
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

func reportLoadStats(t *testing.T, stats map[string]*loadStats, gate bool) {
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
		t.Logf("distribution %s offered=%d completed=%d records_per_RPC=%d payload_counts_1KiB_256KiB_2MiB=%v p50=%s p95=%s p99=%s max=%s service_p99=%s driver_lag_p99=%s driver_lag_max=%s max_sequence=%d max_payload=%d", key, entry.offered, entry.completed, recordsPerRPC, entry.payloads, entry.latencies[(n*50+99)/100-1], entry.latencies[(n*95+99)/100-1], p99, entry.latencies[n-1], entry.service[(n*99+99)/100-1], entry.lags[(n*99+99)/100-1], entry.lags[n-1], job.sequence, job.payload)
		limit := 250 * time.Millisecond
		if strings.HasSuffix(key, "/Read") {
			limit = 100 * time.Millisecond
		}
		if strings.HasSuffix(key, "/Lua") || strings.HasSuffix(key, "/Batch4") {
			limit = 500 * time.Millisecond
		}
		if gate && strings.HasPrefix(key, "steady/") && p99 > limit {
			t.Errorf("p99 gate failed for %s: %s > %s", key, p99, limit)
		}
		if gate && entry.latencies[n-1] > 3*time.Second {
			t.Errorf("fixed end-to-end 3s budget exceeded for %s: %s", key, entry.latencies[n-1])
		}
	}
}

func runSlowConsumers(ctx context.Context, work loadWorkload, measurement time.Time) (string, error) {
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
	connection, err := grpc.NewClient("passthrough:///"+work.process.address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithStaticStreamWindowSize(64<<10), grpc.WithStaticConnWindowSize(4<<20))
	if err != nil {
		return "", err
	}
	defer connection.Close()
	client := pb.NewStoreServiceClient(connection)
	errors := make(chan error, 32)
	progress := make([]atomic.Int64, 32)
	callbacks := make([]atomic.Int64, 32)
	lastProgress := make([]atomic.Int64, 32)
	var offered, completed, maximumGap atomic.Int64
	var pending sync.WaitGroup
	for index := range 32 {
		pending.Add(1)
		go func() {
			defer pending.Done()
			dataset := work.datasets[index%2]
			for phase.Err() == nil {
				offered.Add(1)
				consume := func(ctx context.Context, _ *weirclient.NativeResponse, chunk []byte) error {
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
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-timer.C:
						return nil
					}
				}
				opts := weirclient.NativeOptions{StoreName: dataset.store, Request: dataset.native[3], Consume: consume}
				result, err := weirclient.Native(phase, client, opts)
				if phase.Err() != nil {
					return
				}
				if err != nil || result == nil || result.Failure != nil || result.Completion != weirclient.NativeResponseComplete {
					errors <- fmt.Errorf("slow stream %d failed: %v %v", index, result, err)
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
					errors <- fmt.Errorf("slow stream %d lost continuous progress for over 2s", index)
					cancel()
					observing = false
					break
				}
			}
		}
	}
	pending.Wait()
	select {
	case err := <-errors:
		return "", err
	default:
	}
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	minimumCallbacks, minimumBytes := int64(1<<62), int64(1<<62)
	for index := range progress {
		if callbacks[index].Load() < int64(duration/(500*time.Millisecond)-8) {
			return "", fmt.Errorf("slow stream %d failed to demonstrate continued chunk delivery: callbacks=%d bytes=%d", index, callbacks[index].Load(), progress[index].Load())
		}
		minimumCallbacks = min(minimumCallbacks, callbacks[index].Load())
		minimumBytes = min(minimumBytes, progress[index].Load())
	}
	message := fmt.Sprintf("slow_duration=%s offered=%d completed=%d canceled=%d minimum_callbacks_per_stream=%d minimum_bytes_per_stream=%d maximum_progress_gap=%s", duration, offered.Load(), completed.Load(), offered.Load()-completed.Load(), minimumCallbacks, minimumBytes, time.Duration(maximumGap.Load()))
	return message, nil
}

func verifyLoadOracle(t *testing.T, dataset *loadDataset) {
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
			if err := validateLoadDocument(cursor.Current, dataset.media, want); err != nil {
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
					ID     string     `json:"_id"`
					Found  bool       `json:"found"`
					Source loadRecord `json:"_source"`
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
