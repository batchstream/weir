//go:build integration

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	dto "github.com/prometheus/client_model/go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// This focuses on one Store: distributing slow replies across Stores would not
// establish whether that Store's bounded output reservations preserve progress.
func TestCLILargeNativeOutputBudgetPreservesHealthyQueries(t *testing.T) {
	if os.Getenv("WEIR_NATIVE_OUTPUT_BUDGET_INTEGRATION") != "1" {
		t.Skip("requires WEIR_NATIVE_OUTPUT_BUDGET_INTEGRATION=1, Docker and the local Mongo fixture")
	}
	fixture := testmongo.Open(t)
	binary := buildLinuxCLI(t)
	seedContext, stopSeed := context.WithTimeout(t.Context(), 5*time.Second)
	large := bson.D{{Key: "_id", Value: "large"}, {Key: "n", Value: int64(1)}, {Key: "pad", Value: strings.Repeat("x", 8<<20)}}
	health := bson.D{{Key: "_id", Value: "health"}, {Key: "n", Value: int64(42)}, {Key: "pad", Value: strings.Repeat("h", 2048)}}
	_, err := fixture.Admin.Database(fixture.DB).Collection("records").InsertMany(seedContext, []any{large, health})
	stopSeed()
	if err != nil {
		t.Fatal(err)
	}
	largeNative, err := nativeOutputBudgetRequest(fixture.DB, "large")
	if err != nil {
		t.Fatal(err)
	}
	healthNative, err := nativeOutputBudgetRequest(fixture.DB, "health")
	if err != nil {
		t.Fatal(err)
	}
	backend := &Mongo{URI: containerMongoURI}
	connection := BackendConfig{MongoDB: backend}
	local := &Local{Backend: connection}
	store := StoreConfig{Name: "mongo", Local: local}
	cfg := DefaultConfig()
	cfg.Routing.Stores = []StoreConfig{store}
	opts := memoryRecoveryOptions{binary: binary, capacityMiB: 1024, automatic: true, config: &cfg}
	service := startMemoryRecoveryCLI(t, opts)
	t.Log("capacity=1GiB CPUs=2 stores=1 slow_streams=32 response_pad=8MiB stream_window=64KiB conn_window=4MiB caller_probe_budget=3s")
	initial := nativeOutputBudgetSample(t, service, "initial")
	for name, want := range map[string]float64{"weir_store_active_executions_limit": 16, "weir_store_pending_entries_limit": 1024, "weir_store_pending_reserved_bytes_limit": 32 << 20, "weir_execution_memory_leases_limit": 32768, "weir_execution_memory_native_leases_limit": 1024, "weir_execution_memory_budget_known": 1} {
		if testmetrics.Sum(initial, name) != want {
			t.Fatal("focused test changed a default Store policy", name, want)
		}
	}
	budget := testmetrics.Sum(initial, "weir_execution_memory_budget_bytes")
	if budget <= 0 || budget > service.capacity*.70*2/3 {
		t.Fatal("shared Native budget exceeded its default recovery-capacity bound", budget)
	}
	if initial["weir_store_paused_native_reserved_bytes_limit"] != nil {
		t.Fatal("Native output still advertises the old input-queue byte limit")
	}
	healthClient := endpointProcessClient(t, service.address)
	baseline, stopBaseline := context.WithTimeout(t.Context(), 3*time.Second)
	err = nativeOutputBudgetRead(baseline, healthClient, fixture.DB+"/records/s:health")
	if err == nil {
		err = nativeOutputBudgetNative(baseline, healthClient, healthNative)
	}
	stopBaseline()
	if err != nil {
		t.Fatal("healthy probes failed before slow consumers", err)
	}
	dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithStaticStreamWindowSize(64 << 10), grpc.WithStaticConnWindowSize(4 << 20)}
	slowConnection, err := grpc.NewClient("passthrough:///"+service.address, dialOptions...)
	if err != nil {
		t.Fatal(err)
	}
	slowClient := pb.NewStoreServiceClient(slowConnection)
	slowContext, stopSlow := context.WithTimeout(t.Context(), 20*time.Second)
	var pending sync.WaitGroup
	var ready atomic.Int64
	var consumed atomic.Int64
	results := make(chan error, 32)
	started := time.Now()
	for index := range 32 {
		pending.Add(1)
		go func() {
			defer pending.Done()
			consume := func(ctx context.Context, response *weirclient.NativeResponse, chunk []byte) error {
				if response == nil || response.BodyContentType != "application/bson" || len(chunk) == 0 {
					return fmt.Errorf("slow stream %d received invalid first chunk metadata", index)
				}
				ready.Add(1)
				consumed.Add(int64(len(chunk)))
				<-ctx.Done()
				return ctx.Err()
			}
			options := weirclient.NativeOptions{StoreName: "mongo", Request: largeNative, Consume: consume}
			result, err := weirclient.Native(slowContext, slowClient, options)
			if slowContext.Err() == nil {
				err = fmt.Errorf("slow stream %d ended before cancellation: result=%v RPC=%v", index, result, err)
			}
			results <- err
		}()
	}
	joined := make(chan struct{})
	go func() { pending.Wait(); close(joined) }()
	t.Cleanup(func() {
		stopSlow()
		_ = slowConnection.Close()
		select {
		case <-joined:
		case <-time.After(3 * time.Second):
			t.Error("slow Native calls did not join after cancellation")
		}
		canceled := time.Now()
		deadline := canceled.Add(5 * time.Second)
		for {
			metrics := service.sample(t)
			// The healthy probe connection may stay open with its fixed ingress
			// charge; the closed slow connection must not keep one.
			idle := true
			for _, name := range []string{"weir_execution_memory_ingress_connections", "weir_execution_memory_ingress_connection_bytes", "weir_execution_memory_used_bytes", "weir_execution_memory_resident_bytes"} {
				idle = idle && metrics[name] != nil
			}
			connections := testmetrics.Sum(metrics, "weir_execution_memory_ingress_connections")
			connectionBytes := connections * ingressConnectionBytes
			idle = idle && connections <= 1 && testmetrics.Sum(metrics, "weir_execution_memory_ingress_connection_bytes") == connectionBytes
			idle = idle && testmetrics.Sum(metrics, "weir_execution_memory_used_bytes") == connectionBytes && testmetrics.Sum(metrics, "weir_execution_memory_resident_bytes") == connectionBytes
			for _, name := range []string{"weir_store_pending_entries", "weir_store_pending_reserved_bytes", "weir_store_active_executions", "weir_store_paused_native_outputs", "weir_store_paused_native_reserved_bytes", "weir_store_result_reserved_entries", "weir_store_result_reserved_bytes", "weir_store_retained_result_reserved_bytes", "weir_store_working_reserved_bytes", "weir_store_publishers", "weir_ingress_sessions", "weir_ingress_queued_response_bytes", "weir_execution_memory_input_reserved_bytes", "weir_execution_memory_input_used_bytes", "weir_execution_memory_wire_bytes", "weir_execution_memory_wire_buffers", "weir_execution_memory_leases", "weir_execution_memory_native_leases", "weir_execution_memory_starting", "weir_execution_memory_settled", "weir_execution_memory_borrowed", "weir_execution_memory_waiting", "weir_execution_memory_terminal"} {
				idle = idle && metrics[name] != nil && testmetrics.Sum(metrics, name) == 0
			}
			if idle || time.Now().After(deadline) {
				nativeOutputBudgetLog(t, metrics, fmt.Sprintf("after_cancel elapsed=%s", time.Since(canceled)))
				if !idle {
					t.Error("Native-output cancellation exceeded fixed 5s resource recovery bound")
				}
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
	last := [4]float64{}
	stableSince := time.Now()
	lastLog := time.Time{}
	var saturated map[string]*dto.MetricFamily
	for time.Since(started) < 10*time.Second {
		metrics := service.sample(t)
		state := [4]float64{testmetrics.Sum(metrics, "weir_store_active_executions"), testmetrics.Sum(metrics, "weir_store_paused_native_outputs"), testmetrics.Sum(metrics, "weir_store_pending_entries"), float64(ready.Load())}
		if state != last {
			last, stableSince = state, time.Now()
		}
		if time.Since(lastLog) >= time.Second {
			nativeOutputBudgetLog(t, metrics, fmt.Sprintf("slow_ready=%d consumed=%d elapsed=%s", ready.Load(), consumed.Load(), time.Since(started)))
			lastLog = time.Now()
		}
		// Every large reply must reach its blocked consumer, with all IO
		// slots free. A silently rejected or queued slow stream cannot pass.
		if ready.Load() == 32 && state[0] == 0 && state[1] == 32 && state[2] == 0 && time.Since(started) >= 2*time.Second && time.Since(stableSince) >= time.Second {
			saturated = metrics
			break
		}
		select {
		case err := <-results:
			t.Fatal("slow call ended before the progress probes", err)
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	if saturated == nil {
		t.Fatal("slow Native state did not become stable within 10s", ready.Load())
	}
	nativeOutputBudgetLog(t, saturated, fmt.Sprintf("stable_before_probes ready=%d consumed=%d", ready.Load(), consumed.Load()))
	if testmetrics.Sum(saturated, "weir_execution_memory_used_bytes") > testmetrics.Sum(saturated, "weir_execution_memory_budget_bytes") || testmetrics.Sum(saturated, "weir_execution_memory_leases") != 32 {
		t.Fatal("large replies escaped the one Node memory ledger")
	}
	if testmetrics.Sum(saturated, "weir_memory_latched") != 0 || testmetrics.Sum(saturated, "weir_memory_cgroup_working_set_bytes") >= service.capacity*.80 {
		t.Fatal("focused scenario unexpectedly reached actual memory pressure")
	}
	type probeResult struct {
		method   string
		duration time.Duration
		err      error
	}
	probeResults := make(chan probeResult, 2)
	checks := []struct {
		name string
		run  func(context.Context) error
	}{
		{name: "typed_Read", run: func(ctx context.Context) error {
			return nativeOutputBudgetRead(ctx, healthClient, fixture.DB+"/records/s:health")
		}},
		{name: "short_Native", run: func(ctx context.Context) error { return nativeOutputBudgetNative(ctx, healthClient, healthNative) }},
	}
	for _, check := range checks {
		go func() {
			ctx, stop := context.WithTimeout(t.Context(), 3*time.Second)
			defer stop()
			before := time.Now()
			err := check.run(ctx)
			result := probeResult{method: check.name, duration: time.Since(before), err: err}
			probeResults <- result
		}()
	}
	for range checks {
		result := <-probeResults
		t.Logf("healthy probe method=%s latency=%s error=%v", result.method, result.duration, result.err)
		if result.err != nil {
			t.Errorf("healthy query failed while backend replies were retained by slow consumers: method=%s error=%v", result.method, result.err)
		}
	}
	after := nativeOutputBudgetSample(t, service, fmt.Sprintf("after_probes ready=%d elapsed=%s", ready.Load(), time.Since(started)))
	if slowContext.Err() != nil || testmetrics.Sum(after, "weir_watchdog_expirations_total") != testmetrics.Sum(initial, "weir_watchdog_expirations_total") {
		t.Error("healthy progress depended on slow-call expiry or watchdog release")
	}
	if testmetrics.Sum(after, "weir_execution_memory_budget_rejections_total") != testmetrics.Sum(initial, "weir_execution_memory_budget_rejections_total") {
		t.Error("the bounded 32-reply scenario unexpectedly rejected Native work")
	}
}

func nativeOutputBudgetRequest(database, id string) (*pb.NativeRequest, error) {
	filter := bson.D{{Key: "_id", Value: id}}
	command := bson.D{{Key: "find", Value: "records"}, {Key: "filter", Value: filter}, {Key: "limit", Value: 1}, {Key: "singleBatch", Value: true}}
	raw, err := bson.Marshal(command)
	if err != nil {
		return nil, err
	}
	document := &pb.Document{ContentType: "application/bson", Data: raw}
	request := &pb.NativeRequest{Resource: database, Request: document}
	return request, nil
}

func nativeOutputBudgetRead(ctx context.Context, client pb.StoreServiceClient, resource string) error {
	request := &weirclient.ReadRequest{Resource: resource}
	options := weirclient.ReadOneOptions{StoreName: "mongo", Request: request}
	result, err := weirclient.ReadOne(ctx, client, options)
	if err != nil {
		return err
	}
	if result == nil || result.Failure != nil || result.Document == nil || result.Document.ContentType != "application/bson" {
		return fmt.Errorf("healthy Read lacked a successful BSON document: %v", result)
	}
	return nativeOutputBudgetDocument(result.Document.Data)
}

func nativeOutputBudgetNative(ctx context.Context, client pb.StoreServiceClient, request *pb.NativeRequest) error {
	var body []byte
	consume := func(_ context.Context, _ *weirclient.NativeResponse, chunk []byte) error {
		if len(body)+len(chunk) > 64<<10 {
			return fmt.Errorf("healthy Native response exceeded expected finite bound")
		}
		body = append(body, chunk...)
		return nil
	}
	options := weirclient.NativeOptions{StoreName: "mongo", Request: request, Consume: consume}
	result, err := weirclient.Native(ctx, client, options)
	if err != nil {
		return err
	}
	if result == nil || result.Failure != nil || result.Completion != weirclient.NativeResponseComplete || result.Response == nil || result.Response.BodyContentType != "application/bson" {
		return fmt.Errorf("healthy Native lacked complete BSON evidence: %v", result)
	}
	var reply struct {
		OK     float64 `bson:"ok"`
		Cursor struct {
			Batch []bson.Raw `bson:"firstBatch"`
		} `bson:"cursor"`
	}
	if err := bson.Unmarshal(body, &reply); err != nil {
		return err
	}
	if reply.OK != 1 || len(reply.Cursor.Batch) != 1 {
		return fmt.Errorf("healthy Native Mongo response did not return its owned document")
	}
	return nativeOutputBudgetDocument(reply.Cursor.Batch[0])
}

func nativeOutputBudgetDocument(raw []byte) error {
	var document struct {
		N   int64  `bson:"n"`
		Pad string `bson:"pad"`
	}
	if err := bson.Unmarshal(raw, &document); err != nil {
		return err
	}
	if document.N != 42 || document.Pad != strings.Repeat("h", 2048) {
		return fmt.Errorf("healthy query changed its complete document content")
	}
	return nil
}

func nativeOutputBudgetSample(t *testing.T, service *memoryRecoveryCLI, phase string) map[string]*dto.MetricFamily {
	t.Helper()
	metrics := service.sample(t)
	nativeOutputBudgetLog(t, metrics, phase)
	return metrics
}

func nativeOutputBudgetLog(t *testing.T, metrics map[string]*dto.MetricFamily, phase string) {
	t.Helper()
	values := make(map[string]float64)
	for _, name := range []string{"process_resident_memory_bytes", "process_cpu_seconds_total", "go_memstats_heap_alloc_bytes", "go_memstats_heap_idle_bytes", "go_memstats_heap_released_bytes", "go_memstats_last_gc_time_seconds", "weir_memory_cgroup_working_set_bytes", "weir_memory_cgroup_limit_bytes", "weir_memory_latched", "weir_memory_unknown", "weir_memory_go_runtime_limit_bytes", "weir_store_overloaded", "weir_store_pending_entries", "weir_store_pending_reserved_bytes", "weir_store_active_executions", "weir_store_paused_native_outputs", "weir_store_paused_native_reserved_bytes", "weir_store_result_reserved_entries", "weir_store_result_reserved_bytes", "weir_store_retained_result_reserved_bytes", "weir_store_working_reserved_bytes", "weir_store_publishers", "weir_ingress_sessions", "weir_ingress_queued_response_bytes", "weir_watchdog_expirations_total", "weir_backend_connections_owned", "weir_execution_memory_budget_bytes", "weir_execution_memory_used_bytes", "weir_execution_memory_io_reserve_bytes", "weir_execution_memory_budget_known", "weir_execution_memory_leases", "weir_execution_memory_leases_limit", "weir_execution_memory_starting", "weir_execution_memory_settled", "weir_execution_memory_borrowed", "weir_execution_memory_resident_bytes", "weir_execution_memory_waiting", "weir_execution_memory_terminal", "weir_execution_memory_budget_rejections_total"} {
		values[name] = testmetrics.Sum(metrics, name)
	}
	gc := metrics["go_gc_duration_seconds"].GetMetric()
	if len(gc) > 0 {
		values["go_gc_cycles"] = float64(gc[0].GetSummary().GetSampleCount())
	}
	raw, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Native output budget phase=%s metrics=%s", phase, raw)
}
