//go:build integration

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	dto "github.com/prometheus/client_model/go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestCLIBusinessHeapRecovery runs the Linux CLI in a memory-limited
// container against scripts/mongo-local.sh. The host driver never
// shares the service process's Go heap.
func TestCLIBusinessHeapRecovery(t *testing.T) {
	if os.Getenv("WEIR_MEMORY_RECOVERY_INTEGRATION") != "1" {
		t.Skip("requires WEIR_MEMORY_RECOVERY_INTEGRATION=1, Docker and the local Mongo fixture")
	}
	fixture := testmongo.Open(t)
	binary := buildLinuxCLI(t)
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

// containerMongoURI reaches scripts/mongo-local.sh from a CLI container
// through the host gateway. On Linux, mongod must also bind the Docker bridge
// gateway (WEIR_MONGO_BIND).
const containerMongoURI = "mongodb://host.docker.internal:27028/?directConnection=true&serverMonitoringMode=poll"

var fullContainerID = regexp.MustCompile(`^[0-9a-f]{64}$`)

// buildLinuxCLI cross-compiles the CLI for a Linux container on this host's
// architecture.
func buildLinuxCLI(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0755); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(directory, "weir")
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", binary, "./cmd/weir")
	command.Dir = testutil.Root(t)
	command.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatal("Linux CLI build failed", err, string(output))
	}
	return binary
}

// containerHostAddress rewrites a host listener address so a CLI container
// reaches it through the host gateway.
func containerHostAddress(t *testing.T, address string) string {
	t.Helper()
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	return net.JoinHostPort("host.docker.internal", port)
}

// searchContainerURL returns the bridge address of the scripts/search-local.sh
// container so a CLI container reaches Search without a host round trip.
func searchContainerURL(t *testing.T, backend *testsearch.Backend) string {
	t.Helper()
	name := "weir-local-elasticsearch"
	if backend.Product == "opensearch" {
		name = "weir-local-opensearch-3.3"
	}
	raw := packagedDocker(t, "inspect", "--type", "container", "--format", `{{index .Config.Labels "weir.owner"}} {{(index .NetworkSettings.Networks "bridge").IPAddress}} {{.State.Running}}`, name)
	fields := strings.Fields(raw)
	if len(fields) != 3 || fields[0] != "weir-search-local" || fields[2] != "true" {
		t.Fatal("local Search fixture container is not running", raw)
	}
	address, err := netip.ParseAddr(fields[1])
	if err != nil || !address.Is4() || !address.IsPrivate() {
		t.Fatal("local Search fixture requires a private bridge address", raw, err)
	}
	return "http://" + address.String() + ":9200"
}

func validateContainerOwnership(raw, id, owner string) error {
	fields := strings.Fields(raw)
	if !fullContainerID.MatchString(id) || len(fields) < 2 || fields[0] != id || fields[1] != owner {
		return fmt.Errorf("container ID/owner mismatch; refuse stop/remove: %q", raw)
	}
	return nil
}

// name is the full container ID; owner is the container name and its
// weir.owner label.
type memoryRecoveryCLI struct {
	name, owner, address, diagnostic string
	capacity                         float64
	automatic                        bool
	stores                           int
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
	t.Cleanup(func() { _ = reservation.Close() })
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
			backend := &Mongo{URI: containerMongoURI}
			connection := BackendConfig{MongoDB: backend}
			local := &Local{Backend: connection}
			definition := StoreConfig{Name: fmt.Sprintf("mongo%d", index), Local: local}
			cfg.Routing.Stores = append(cfg.Routing.Stores, definition)
		}
	} else {
		stores = len(cfg.Routing.Stores)
	}
	writeConfigFiles(t, filepath.Join(directory, "node.yaml"), cfg, 0644)
	args := []string{"create", "--name", owner, "--label", "weir.owner=" + owner, "--platform=linux/" + runtime.GOARCH, "--pull=never",
		"--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges", "--user=65532:65532",
		fmt.Sprintf("--memory=%dm", opts.capacityMiB), fmt.Sprintf("--memory-swap=%dm", opts.capacityMiB), "--cpus=2", "--pids-limit=96",
		"--publish", applicationAddress + ":7447", "--publish", "127.0.0.1::7449",
		"--mount", "type=bind,src=" + directory + ",dst=/fixture,readonly", "--mount", "type=bind,src=" + opts.binary + ",dst=/weir,readonly"}
	if runtime.GOOS == "linux" {
		// Docker Engine on Linux maps the host gateway only on request.
		args = append(args, "--add-host=host.docker.internal:host-gateway")
	}
	if !opts.automatic {
		args = append(args, "--env=GOMEMLIMIT=off")
	}
	if opts.sharedMemoryMiB > 0 {
		args = append(args, fmt.Sprintf("--shm-size=%dm", opts.sharedMemoryMiB))
	}
	args = append(args, "busybox:1.37.0", "/weir", "serve", "--config", "/fixture/node.yaml", "--routes", "/fixture/node-routing.yaml")
	// Install the guard before create; no post-create observation/log can bypass it.
	containerID := ""
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		check := exec.CommandContext(ctx, "docker", "inspect", "--type", "container", "--format", `{{.Id}} {{index .Config.Labels "weir.owner"}}`, owner)
		raw, err := check.CombinedOutput()
		cancel()
		if err != nil {
			if strings.TrimSpace(string(raw)) == "Error response from daemon: No such container: "+owner {
				return
			}
			t.Error("cleanup could not verify container ownership; leaving it in place", owner, err, string(raw))
			return
		}
		if err := validateContainerOwnership(string(raw), containerID, owner); err != nil {
			t.Error(err)
			return
		}
		ctx, cancel = context.WithTimeout(context.Background(), 12*time.Second)
		raw, err = exec.CommandContext(ctx, "docker", "stop", "--timeout=8", containerID).CombinedOutput()
		cancel()
		if err != nil {
			t.Error("cleanup stop failed", err, string(raw))
		}
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		logs, logErr := exec.CommandContext(ctx, "docker", "logs", containerID).CombinedOutput()
		cancel()
		if logErr != nil {
			t.Error("cleanup logs failed", logErr, string(logs))
		} else {
			t.Logf("CLI logs=%s", strings.TrimSpace(string(logs)))
		}
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		check = exec.CommandContext(ctx, "docker", "inspect", "--type", "container", "--format", `{{.Id}} {{index .Config.Labels "weir.owner"}} {{.State.Running}} {{.State.OOMKilled}} {{.State.ExitCode}}`, owner)
		raw, err = check.CombinedOutput()
		cancel()
		state := strings.Fields(string(raw))
		ownershipErr := validateContainerOwnership(string(raw), containerID, owner)
		if err != nil || ownershipErr != nil || len(state) != 5 || state[2] != "false" {
			t.Error("cleanup cannot confirm stopped owned container; preserving it", err, string(raw))
			return
		}
		if state[3] != "false" || state[4] != "0" {
			t.Error("CLI failed or required forced shutdown", state[3:])
		}
		t.Logf("CLI exit=%s %s", state[3], state[4])
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		raw, err = exec.CommandContext(ctx, "docker", "rm", containerID).CombinedOutput()
		cancel()
		if err != nil {
			t.Error("cleanup removal failed", err, string(raw))
		}
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		check = exec.CommandContext(ctx, "docker", "inspect", "--type", "container", "--format", `{{.Id}} {{index .Config.Labels "weir.owner"}}`, owner)
		raw, err = check.CombinedOutput()
		cancel()
		if err == nil || strings.TrimSpace(string(raw)) != "Error response from daemon: No such container: "+owner {
			t.Error("cleanup removal did not prove container absence", err, string(raw))
		}
	})
	containerID = strings.TrimSpace(packagedDocker(t, args...))
	if !fullContainerID.MatchString(containerID) {
		t.Fatal("Docker create did not return a full container ID", containerID)
	}
	created := packagedDocker(t, "inspect", "--type", "container", "--format", `{{.Id}} {{index .Config.Labels "weir.owner"}}`, owner)
	if err := validateContainerOwnership(created, containerID, owner); err != nil {
		t.Fatal(err)
	}
	packagedDocker(t, "start", containerID)
	service := &memoryRecoveryCLI{name: containerID, owner: owner, capacity: float64(opts.capacityMiB << 20), automatic: opts.automatic, stores: stores}
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
		required := float64((count + 1) * (12 << 20))
		maxWire := before
		memoryRecoveryWireDiagnostic(t, metrics, fmt.Sprintf("before_read count=%d before=%.0f required=%.0f maxwire=%.0f", count, before, required, maxWire), false)
		streamContext, stopStream := context.WithCancel(ctx)
		defer stopStream()
		stream, err := blockedClient.Execute(streamContext)
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
			wire := testmetrics.Sum(metrics, "weir_ingress_queued_response_bytes")
			if wire > maxWire {
				maxWire = wire
				memoryRecoveryWireDiagnostic(t, metrics, fmt.Sprintf("new_max count=%d before=%.0f required=%.0f maxwire=%.0f", count, before, required, maxWire), false)
			}
			// Every preceding large message is still blocked on this dedicated
			// static-window connection. Compare their cumulative ownership,
			// because unrelated healthy-response bytes can be freed after before.
			// Small failures and framing overhead cannot replace a 12 MiB reply.
			if wire >= required {
				memoryRecoveryWireDiagnostic(t, metrics, fmt.Sprintf("threshold_satisfied count=%d before=%.0f required=%.0f maxwire=%.0f", count, before, required, maxWire), false)
				count++
				break
			}
			if testmetrics.Sum(metrics, "weir_memory_latched") == 1 {
				memoryRecoveryWireDiagnostic(t, metrics, fmt.Sprintf("latched_before_threshold count=%d before=%.0f required=%.0f maxwire=%.0f", count, before, required, maxWire), false)
				break
			}
			if time.Now().After(deadline) {
				memoryRecoveryWireDiagnostic(t, metrics, fmt.Sprintf("threshold_timeout count=%d before=%.0f required=%.0f maxwire=%.0f", count, before, required, maxWire), true)
				memoryRecoveryWireFailureReceive(t, stream, stopStream)
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

// Record owned response bytes without changing the three-second pressure bound.
// Full metric families are emitted only after that assertion has failed.
func memoryRecoveryWireDiagnostic(t *testing.T, metrics map[string]*dto.MetricFamily, phase string, full bool) {
	t.Helper()
	names := []string{"weir_ingress_queued_response_bytes", "process_resident_memory_bytes", "weir_memory_cgroup_working_set_bytes", "weir_memory_cgroup_limit_bytes", "weir_memory_latched", "weir_memory_unknown", "weir_store_active_executions", "weir_store_working_reserved_bytes", "weir_store_pending_entries", "weir_store_result_reserved_entries", "weir_store_result_reserved_bytes", "weir_store_retained_result_reserved_bytes", "weir_store_publishers", "weir_ingress_sessions", "weir_backend_connections_owned", "go_memstats_heap_alloc_bytes", "go_memstats_last_gc_time_seconds", "weir_watchdog_expirations_total"}
	values := make(map[string]float64)
	for _, name := range names {
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
	t.Logf("wire_threshold_diagnostic phase=%s metrics=%s", phase, raw)
	if full {
		// Histogram +Inf buckets have no JSON encoding.
		t.Logf("wire_threshold_diagnostic timeout_all_metric_families=%v", metrics)
	}
}

func memoryRecoveryWireFailureReceive(t *testing.T, stream grpc.BidiStreamingClient[pb.ExecuteRequest, pb.ExecuteResponse], cancel context.CancelFunc) {
	t.Helper()
	type receivedResponse struct {
		response *pb.ExecuteResponse
		err      error
	}
	received := make(chan receivedResponse, 1)
	started := time.Now()
	go func() {
		response, err := stream.Recv()
		result := receivedResponse{response: response, err: err}
		received <- result
	}()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case result := <-received:
		response := result.response
		read := response.GetEvent().GetReadResult()
		t.Logf("wire_threshold_diagnostic failure_only_Recv elapsed=%s response_type=%T index=%d document_bytes=%d content_type=%s failure_code=%s transport_status=%s transport_error=%v", time.Since(started), response.GetEvent().GetValue(), response.GetIndex(), len(read.GetDocument().GetData()), read.GetDocument().GetContentType(), read.GetFailure().GetCode(), status.Code(result.err), result.err)
	case <-timer.C:
		cancel()
		t.Logf("wire_threshold_diagnostic failure_only_Recv elapsed=%s timed_out_1s=true stream_canceled=true", time.Since(started))
	}
}
