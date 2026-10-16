//go:build integration

package app

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	dto "github.com/prometheus/client_model/go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// This opts into native Linux Docker and the fixed, independently checked local
// MongoDB fixture. The host driver never shares the service process's Go heap.
func TestCLIBusinessHeapRecovery(t *testing.T) {
	if os.Getenv("WEIR_MEMORY_RECOVERY_INTEGRATION") != "1" {
		t.Skip("requires explicit owned CLI memory-recovery qualification")
	}
	if runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" {
		t.Fatal("this qualification requires a native Darwin arm64 driver")
	}
	binary := os.Getenv("WEIR_MEMORY_RECOVERY_BINARY")
	if !filepath.IsAbs(binary) {
		t.Fatal("WEIR_MEMORY_RECOVERY_BINARY must identify this source's Linux CLI")
	}
	engine := strings.TrimSpace(packagedDocker(t, "info", "--format", "{{.OSType}}/{{.Architecture}}/{{.CgroupVersion}}"))
	if engine != "linux/aarch64/2" {
		t.Fatal("native Linux arm64 cgroup-v2 engine required", engine)
	}
	image := strings.TrimSpace(packagedDocker(t, "image", "inspect", "--format", "{{.Os}}/{{.Architecture}} {{.Id}}", "busybox:1.37.0"))
	if image != "linux/arm64 sha256:ad4f11f7c85f87c0a6d6c0fe492ad53f9cdcff6e8c88b881360c70eff701c67b" {
		t.Fatal("the previously cached pinned native fixture image is required", image)
	}
	t.Logf("driver Go=%s host=%s/%s engine=%s image=%s", runtime.Version(), runtime.GOOS, runtime.GOARCH,
		strings.TrimSpace(packagedDocker(t, "info", "--format", "{{.ServerVersion}} kernel={{.KernelVersion}} {{.OSType}}/{{.Architecture}} cgroup={{.CgroupVersion}}")), image)
	fixture := testmongo.Open(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	document := bson.D{{Key: "_id", Value: "large"}, {Key: "pad", Value: strings.Repeat("x", 12<<20)}}
	if _, err := fixture.Admin.Database(fixture.DB).Collection("records").InsertOne(ctx, document); err != nil {
		t.Fatal(err)
	}
	for _, capacity := range []int{256, 512} {
		for _, automatic := range []bool{true, false} {
			for round := range 3 {
				t.Run(fmt.Sprintf("%dMiB/automatic=%t/round=%d", capacity, automatic, round+1), func(t *testing.T) {
					opts := memoryRecoveryOptions{binary: binary, capacityMiB: capacity, automatic: automatic}
					service := startMemoryRecoveryCLI(t, opts)
					client := endpointProcessClient(t, service.address)
					initial := service.sample(t)
					managed := testmetrics.Sum(initial, "weir_memory_go_runtime_managed")
					if managed != 0 && !automatic || managed != 1 && automatic {
						t.Fatal("unexpected CLI runtime-memory policy", managed)
					}
					if automatic {
						limit := testmetrics.Sum(initial, "weir_memory_go_runtime_limit_bytes")
						want := uint64(capacity<<20) * 70 / 100 * 80 / 100
						if limit <= 0 || limit > float64(want) {
							t.Fatal("unexpected default soft limit", limit, want)
						}
						t.Logf("automatic Go soft limit=%g", limit)
					}
					resource := fixture.DB + "/records/s:health"
					memoryRecoverySteady(t, service, client, resource)
					memoryRecoveryBurst(t, service, fixture.DB+"/records/s:large")
					if testmetrics.Sum(service.sample(t), "weir_memory_latched") == 0 {
						memoryRecoverySteady(t, service, client, resource)
					}
				})
			}
		}
	}
}

type memoryRecoveryOptions struct {
	binary          string
	capacityMiB     int
	sharedMemoryMiB int
	automatic       bool
	config          *Config
}

type memoryRecoveryCLI struct {
	name, address, diagnostic string
	capacity                  float64
	automatic                 bool
	stores                    int
}

func startMemoryRecoveryCLI(t *testing.T, opts memoryRecoveryOptions) *memoryRecoveryCLI {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0755); err != nil {
		t.Fatal(err)
	}
	owner := fmt.Sprintf("weir-memory-recovery-%d-%d", os.Getpid(), time.Now().UnixNano())
	cfg := DefaultConfig()
	if opts.config != nil {
		cfg = *opts.config
	}
	cfg.Basic.Listeners.Application = "0.0.0.0:7447"
	cfg.Basic.Diagnostics.Address, cfg.Basic.Diagnostics.AllowIntranet = "0.0.0.0:7449", true
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	applicationAddress := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Basic.Discovery.Advertise = []string{applicationAddress}
	stores := 1
	if opts.capacityMiB == 512 {
		stores = 3
	}
	if opts.config == nil {
		for index := range stores {
			backend := &Mongo{URI: "mongodb://host.docker.internal:27028/?directConnection=true&serverMonitoringMode=poll"}
			connection := BackendConfig{MongoDB: backend}
			local := &Local{Backend: connection}
			definition := StoreConfig{Name: fmt.Sprintf("mongo%d", index), Local: local}
			cfg.Routing.Stores = append(cfg.Routing.Stores, definition)
		}
	} else {
		stores = len(cfg.Routing.Stores)
	}
	writeConfigFiles(t, filepath.Join(directory, "node.yaml"), cfg, 0644)
	args := []string{"create", "--name", owner, "--label", "weir.owner=" + owner, "--platform=linux/arm64", "--pull=never",
		"--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges", "--user=65532:65532",
		fmt.Sprintf("--memory=%dm", opts.capacityMiB), fmt.Sprintf("--memory-swap=%dm", opts.capacityMiB), "--cpus=2", "--pids-limit=96",
		"--publish", applicationAddress + ":7447", "--publish", "127.0.0.1::7449",
		"--mount", "type=bind,src=" + directory + ",dst=/fixture,readonly", "--mount", "type=bind,src=" + opts.binary + ",dst=/weir,readonly"}
	if !opts.automatic {
		args = append(args, "--env=GOMEMLIMIT=off")
	}
	if opts.sharedMemoryMiB > 0 {
		args = append(args, fmt.Sprintf("--shm-size=%dm", opts.sharedMemoryMiB))
	}
	args = append(args, "busybox:1.37.0", "/weir", "serve", "--config", "/fixture/node.yaml", "--routes", "/fixture/node-routing.yaml")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		check := exec.CommandContext(ctx, "docker", "inspect", "--type", "container", "--format", `{{index .Config.Labels "weir.owner"}}`, owner)
		raw, err := check.CombinedOutput()
		cancel()
		if err != nil {
			if strings.TrimSpace(string(raw)) == "Error response from daemon: No such container: "+owner {
				return
			}
			t.Error("cleanup could not verify candidate ownership; preserving evidence", owner, err, string(raw))
			return
		}
		label := strings.TrimSpace(string(raw))
		if label != owner {
			t.Error("refusing cleanup of a container without this test's owner label")
			return
		}
		ctx, cancel = context.WithTimeout(context.Background(), 12*time.Second)
		raw, err = exec.CommandContext(ctx, "docker", "stop", "--timeout=8", owner).CombinedOutput()
		cancel()
		if err != nil {
			t.Error("cleanup stop failed", err, string(raw))
		}
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		logs, logErr := exec.CommandContext(ctx, "docker", "logs", owner).CombinedOutput()
		cancel()
		if logErr != nil {
			t.Error("cleanup logs failed", logErr, string(logs))
		} else {
			t.Logf("CLI logs=%s", strings.TrimSpace(string(logs)))
		}
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		check = exec.CommandContext(ctx, "docker", "inspect", "--type", "container", "--format", `{{index .Config.Labels "weir.owner"}} {{.State.Running}} {{.State.OOMKilled}} {{.State.ExitCode}}`, owner)
		raw, err = check.CombinedOutput()
		cancel()
		state := strings.Fields(string(raw))
		if err != nil || len(state) != 4 || state[0] != owner || state[1] != "false" {
			t.Error("cleanup cannot confirm stopped owned container; preserving it", err, string(raw))
			return
		}
		if state[2] != "false" || state[3] != "0" {
			t.Error("CLI failed or required forced shutdown", state[2:])
		}
		t.Logf("CLI exit=%s %s", state[2], state[3])
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		raw, err = exec.CommandContext(ctx, "docker", "rm", owner).CombinedOutput()
		cancel()
		if err != nil {
			t.Error("cleanup removal failed", err, string(raw))
		}
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		check = exec.CommandContext(ctx, "docker", "inspect", "--type", "container", "--format", `{{index .Config.Labels "weir.owner"}}`, owner)
		raw, err = check.CombinedOutput()
		cancel()
		if err == nil || strings.TrimSpace(string(raw)) != "Error response from daemon: No such container: "+owner {
			t.Error("cleanup removal did not prove container absence", err, string(raw))
		}
	})
	packagedDocker(t, args...)
	packagedDocker(t, "start", owner)
	service := &memoryRecoveryCLI{name: owner, capacity: float64(opts.capacityMiB << 20), automatic: opts.automatic, stores: stores}
	service.address = strings.TrimSpace(packagedDocker(t, "port", owner, "7447/tcp"))
	if service.address != applicationAddress {
		t.Fatal("Docker published address differs from the real advertised endpoint", service.address, applicationAddress)
	}
	service.diagnostic = strings.TrimSpace(packagedDocker(t, "port", owner, "7449/tcp"))
	deadline := time.Now().Add(7 * time.Second)
	probe := &http.Client{Timeout: 300 * time.Millisecond}
	for {
		response, err := probe.Get("http://" + service.diagnostic + "/readyz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("CLI did not become ready", packagedDocker(t, "logs", owner))
		}
		time.Sleep(20 * time.Millisecond)
	}
	return service
}

func (service *memoryRecoveryCLI) sample(t *testing.T) map[string]*dto.MetricFamily {
	t.Helper()
	metrics := testmetrics.Scrape(t, service.diagnostic)
	if testmetrics.Sum(metrics, "weir_memory_cgroup_valid") != 1 || testmetrics.Sum(metrics, "weir_memory_cgroup_limit_bytes") != service.capacity || testmetrics.Sum(metrics, "weir_memory_unknown") != 0 {
		t.Fatal("missing or wrong native cgroup observation")
	}
	return metrics
}

func memoryRecoverySteady(t *testing.T, service *memoryRecoveryCLI, client pb.StoreServiceClient, resource string) {
	t.Helper()
	before := service.sample(t)
	latencies := make([]time.Duration, 0, 60)
	started := time.Now()
	for index := range 30 {
		deadline := started.Add(time.Duration(index) * 100 * time.Millisecond)
		if remaining := time.Until(deadline); remaining > 0 {
			time.Sleep(remaining)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		writeRaw := bson.D{{Key: "_id", Value: "health"}, {Key: "n", Value: index}, {Key: "pad", Value: strings.Repeat("h", 2048)}}
		data, err := bson.Marshal(writeRaw)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		document := &pb.Document{ContentType: "application/bson", Data: data}
		write := &weirclient.WriteRequest{Resource: resource, Document: document}
		writeOptions := weirclient.WriteOptions{StoreName: "mongo0", Request: write}
		callStarted := time.Now()
		result, err := weirclient.Put(ctx, client, writeOptions)
		latencies = append(latencies, time.Since(callStarted))
		if err != nil || result == nil || result.Failure != nil || result.Outcome != weirclient.MutationApplied {
			cancel()
			t.Fatal("steady write failed", result, err)
		}
		read := &weirclient.ReadRequest{Resource: resource}
		readOptions := weirclient.ReadOneOptions{StoreName: "mongo0", Request: read}
		callStarted = time.Now()
		readback, err := weirclient.ReadOne(ctx, client, readOptions)
		latencies = append(latencies, time.Since(callStarted))
		cancel()
		if err != nil || readback == nil || readback.Document == nil || readback.Failure != nil {
			t.Fatal("steady read failed", readback, err)
		}
		var actual struct{ N int }
		if err := bson.Unmarshal(readback.Document.Data, &actual); err != nil || actual.N != index {
			t.Fatal("acknowledged write was not read back", actual.N, index, err)
		}
	}
	if remaining := time.Until(started.Add(3 * time.Second)); remaining > 0 {
		time.Sleep(remaining)
	}
	after := service.sample(t)
	slices.Sort(latencies)
	gcBefore, gcAfter := before["go_gc_duration_seconds"].GetMetric()[0].GetSummary(), after["go_gc_duration_seconds"].GetMetric()[0].GetSummary()
	t.Logf("steady offered=20ops/s completed=%d throughput=%.2fops/s p50=%s p99=%s max=%s CPU=%.6fs GC_count=%d GC_pause=%.6fs RSS=%.0f working_set=%.0f heap=%.0f",
		len(latencies), float64(len(latencies))/time.Since(started).Seconds(), latencies[29], latencies[59], latencies[59],
		testmetrics.Sum(after, "process_cpu_seconds_total")-testmetrics.Sum(before, "process_cpu_seconds_total"), gcAfter.GetSampleCount()-gcBefore.GetSampleCount(), gcAfter.GetSampleSum()-gcBefore.GetSampleSum(),
		testmetrics.Sum(after, "process_resident_memory_bytes"), testmetrics.Sum(after, "weir_memory_cgroup_working_set_bytes"), testmetrics.Sum(after, "go_memstats_heap_alloc_bytes"))
}

// Typed Read responses exercise real business heap pressure independently of
// Native's separate working-memory protection. Static flow-control windows keep
// each complete 12 MiB document resident without retaining it in the host driver.
func memoryRecoveryBurst(t *testing.T, service *memoryRecoveryCLI, resource string) {
	t.Helper()
	parts := strings.SplitN(resource, "/", 2)
	if len(parts) != 2 {
		t.Fatal("owned typed Read resource must include its database")
	}
	dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithStaticStreamWindowSize(64 << 10), grpc.WithStaticConnWindowSize(4 << 20)}
	connection, err := grpc.NewClient("passthrough:///"+service.address, dialOptions...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer func() { cancel(); _ = connection.Close() }()
	blockedClient := pb.NewStoreServiceClient(connection)
	var streams []grpc.BidiStreamingClient[pb.ExecuteRequest, pb.ExecuteResponse]
	metrics := service.sample(t)
	count := 0
	for testmetrics.Sum(metrics, "weir_memory_latched") == 0 {
		working := testmetrics.Sum(metrics, "weir_memory_cgroup_working_set_bytes")
		if working+float64(32<<20) > service.capacity || count >= 40 {
			t.Fatal("safe business-pressure headroom exhausted before reaching overload", count, working, service.capacity)
		}
		before := testmetrics.Sum(metrics, "weir_ingress_queued_response_bytes")
		stream, err := blockedClient.Execute(ctx)
		if err != nil {
			t.Fatal(err)
		}
		read := &pb.ReadRequest{Resource: resource}
		operation := &pb.Command_Read{Read: read}
		command := &pb.Command{Operation: operation}
		request := &pb.ExecuteRequest{StoreName: fmt.Sprintf("mongo%d", count%service.stores), Index: 1, Command: command}
		if err := stream.Send(request); err != nil {
			t.Fatal(err)
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatal(err)
		}
		streams = append(streams, stream)
		deadline := time.Now().Add(3 * time.Second)
		for {
			metrics = service.sample(t)
			// A full encoded document is charged before DATA waits on flow
			// control. A small admission failure cannot satisfy this check.
			if testmetrics.Sum(metrics, "weir_ingress_queued_response_bytes") >= before+float64(12<<20) {
				count++
				break
			}
			if testmetrics.Sum(metrics, "weir_memory_latched") == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("typed business pressure did not retain its full response", count)
			}
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(120 * time.Millisecond)
		metrics = service.sample(t)
	}
	client := endpointProcessClient(t, service.address)
	read := &weirclient.ReadRequest{Resource: parts[0] + "/records/s:health"}
	readOptions := weirclient.ReadOneOptions{StoreName: "mongo0", Request: read}
	probe, stop := context.WithTimeout(t.Context(), 300*time.Millisecond)
	result, err := weirclient.ReadOne(probe, client, readOptions)
	stop()
	if status.Code(err) != codes.ResourceExhausted && (result == nil || result.Failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED) {
		t.Fatal("actual live memory pressure did not reject a new point query", result, err)
	}
	for range 10 {
		metrics = service.sample(t)
		if testmetrics.Sum(metrics, "weir_memory_latched") != 1 {
			t.Fatal("live typed Read replies manufactured memory recovery")
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("live pressure kind=typed_Read response_document=12MiB requests=%d RSS=%.0f working_set=%.0f heap=%.0f latch=1", count,
		testmetrics.Sum(metrics, "process_resident_memory_bytes"), testmetrics.Sum(metrics, "weir_memory_cgroup_working_set_bytes"), testmetrics.Sum(metrics, "go_memstats_heap_alloc_bytes"))
	released := time.Now()
	runtime.KeepAlive(streams)
	cancel()
	_ = connection.Close()
	rejections := 0
	for {
		metrics = service.sample(t)
		working := testmetrics.Sum(metrics, "weir_memory_cgroup_working_set_bytes")
		probe, stop := context.WithTimeout(t.Context(), 300*time.Millisecond)
		result, err := weirclient.ReadOne(probe, client, readOptions)
		stop()
		accepted := err == nil && result != nil && result.Failure == nil && result.Document != nil
		if !accepted {
			if status.Code(err) != codes.ResourceExhausted && (result == nil || result.Failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED) {
				t.Fatal("recovery point query failed without memory overload evidence", result, err)
			}
			rejections++
		}
		idle := true
		for _, name := range []string{"weir_store_active_executions", "weir_store_paused_native_outputs", "weir_store_paused_native_reserved_bytes", "weir_store_pending_entries", "weir_store_pending_reserved_bytes", "weir_store_result_reserved_entries", "weir_store_result_reserved_bytes", "weir_store_retained_result_reserved_bytes", "weir_store_working_reserved_bytes", "weir_store_publishers", "weir_ingress_sessions", "weir_ingress_queued_response_bytes"} {
			if metrics[name] == nil || testmetrics.Sum(metrics, name) != 0 {
				idle = false
			}
		}
		if accepted && idle && testmetrics.Sum(metrics, "weir_memory_latched") == 0 && working <= service.capacity*.70 {
			t.Logf("recovered=%s overload_rejections=%d RSS=%.0f working_set=%.0f heap=%.0f", time.Since(released), rejections, testmetrics.Sum(metrics, "process_resident_memory_bytes"), working, testmetrics.Sum(metrics, "go_memstats_heap_alloc_bytes"))
			return
		}
		if time.Since(released) >= 5*time.Second {
			if service.automatic {
				t.Fatal("released business heap failed the fixed 5s automatic recovery bound", working, testmetrics.Sum(metrics, "weir_memory_latched"))
			}
			t.Logf("explicit off control still latched after fixed 5s: RSS=%.0f working_set=%.0f heap=%.0f latch=%.0f", testmetrics.Sum(metrics, "process_resident_memory_bytes"), working, testmetrics.Sum(metrics, "go_memstats_heap_alloc_bytes"), testmetrics.Sum(metrics, "weir_memory_latched"))
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
