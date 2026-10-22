//go:build integration

package app

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The retained tmpfs allocation is outside Go's runtime budget but inside this
// owned CLI container's working set. No production memory hooks are installed.
func TestCLISharedMemoryRecoveryProbe(t *testing.T) {
	if os.Getenv("WEIR_SHARED_PRESSURE_INTEGRATION") != "1" {
		t.Skip("requires explicit owned shared-memory pressure fixture")
	}
	if runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" {
		t.Fatal("requires native Darwin arm64 driver and Linux arm64 CLI")
	}
	binary := os.Getenv("WEIR_SHARED_PRESSURE_BINARY")
	if !filepath.IsAbs(binary) {
		t.Fatal("absolute supplied Linux CLI binary required")
	}
	sharedMiB := 128
	if supplied := os.Getenv("WEIR_SHARED_PRESSURE_MIB"); supplied != "" {
		value, err := strconv.Atoi(supplied)
		if err != nil || value != 128 && value != 160 && value != 192 && value != 344 && value != 352 && value != 368 {
			t.Fatal("shared-pressure size must be the fixed owned 128/160/192/344/352/368 MiB fixture")
		}
		sharedMiB = value
	}
	fixture := testmongo.Open(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	large := bson.D{{Key: "_id", Value: "shared-large"}, {Key: "pad", Value: strings.Repeat("x", 12<<20)}}
	health := bson.D{{Key: "_id", Value: "health"}, {Key: "n", Value: 1}}
	documents := []any{large, health}
	if _, err := fixture.Admin.Database(fixture.DB).Collection("records").InsertMany(ctx, documents); err != nil {
		t.Fatal(err)
	}
	opts := memoryRecoveryOptions{binary: binary, capacityMiB: 512, automatic: true, sharedMemoryMiB: sharedMiB + 32}
	service := startMemoryRecoveryCLI(t, opts)
	t.Logf("supplied CLI identity=%s", strings.TrimSpace(packagedDocker(t, "exec", service.name, "/weir", "version")))
	pressureBytes := strconv.Itoa(sharedMiB << 20)
	packagedDocker(t, "exec", service.name, "sh", "-c", "head -c "+pressureBytes+" /dev/zero > /dev/shm/weir-owned-pressure")
	size := strings.TrimSpace(packagedDocker(t, "exec", service.name, "stat", "-c", "%s", "/dev/shm/weir-owned-pressure"))
	if size != pressureBytes {
		t.Fatal("owned shared pressure allocation size differs", size)
	}
	shared := sharedPressureStat(t, service, "shmem")
	if shared < uint64(sharedMiB<<20) {
		t.Fatal("tmpfs bytes were not charged as non-reclaimable shared memory", shared)
	}
	metrics := service.sample(t)
	for _, name := range []string{"weir_memory_recovery_allowance_bytes", "weir_memory_recovery_allowance_known"} {
		if metrics[name] == nil {
			t.Fatal("supplied binary is missing recovery allowance diagnostics", name)
		}
	}
	if managed := testmetrics.Sum(metrics, "weir_memory_go_runtime_managed"); managed != 1 {
		t.Fatal("owned automatic CLI did not manage its runtime budget", managed)
	}
	if limit := testmetrics.Sum(metrics, "weir_memory_go_runtime_limit_bytes"); limit < 16<<20 {
		t.Fatal("owned 512 MiB CLI installed a runtime limit below its automatic floor", limit)
	}
	cpuBefore := testmetrics.Sum(metrics, "process_cpu_seconds_total")
	started := time.Now()
	t.Cleanup(func() {
		metrics := service.sample(t)
		gc := testmetrics.Sample(metrics, "go_gc_duration_seconds", nil).GetSummary()
		idle := true
		for _, name := range []string{"weir_store_active_executions", "weir_store_paused_native_outputs", "weir_store_paused_native_reserved_bytes", "weir_store_pending_entries", "weir_store_pending_reserved_bytes", "weir_store_result_reserved_entries", "weir_store_result_reserved_bytes", "weir_store_retained_result_reserved_bytes", "weir_store_working_reserved_bytes", "weir_store_publishers", "weir_ingress_sessions", "weir_ingress_queued_response_bytes"} {
			idle = idle && metrics[name] != nil && testmetrics.Sum(metrics, name) == 0
		}
		if !idle {
			t.Error("owned CLI retained Store/RPC/wire credits after callers stopped")
		}
		t.Logf("shared pressure final elapsed=%s shmem=%d RSS=%.0f working_set=%.0f heap_live=%.0f heap_idle=%.0f heap_released=%.0f GoLimit=%.0f recovery_allowance=%.0f recovery_known=%.0f latch=%.0f idle=%t process_cpu_delta=%g GC_cycles=%d GC_pause_sum=%g GC_CPU=unavailable_in_supplied_binary", time.Since(started), sharedPressureStat(t, service, "shmem"), testmetrics.Sum(metrics, "process_resident_memory_bytes"), testmetrics.Sum(metrics, "weir_memory_cgroup_working_set_bytes"), testmetrics.Sum(metrics, "go_memstats_heap_alloc_bytes"), testmetrics.Sum(metrics, "go_memstats_heap_idle_bytes"), testmetrics.Sum(metrics, "go_memstats_heap_released_bytes"), testmetrics.Sum(metrics, "weir_memory_go_runtime_limit_bytes"), testmetrics.Sum(metrics, "weir_memory_recovery_allowance_bytes"), testmetrics.Sum(metrics, "weir_memory_recovery_allowance_known"), testmetrics.Sum(metrics, "weir_memory_latched"), idle, testmetrics.Sum(metrics, "process_cpu_seconds_total")-cpuBefore, gc.GetSampleCount(), gc.GetSampleSum())
	})
	client := endpointProcessClient(t, service.address)
	if sharedMiB >= 344 {
		read := &weirclient.ReadRequest{Resource: fixture.DB + "/records/s:health"}
		options := weirclient.ReadOneOptions{StoreName: "mongo0", Request: read}
		completed, rejected := 0, 0
		for time.Since(started) < 3*time.Second {
			ctx, stop := context.WithTimeout(t.Context(), time.Second)
			result, err := weirclient.ReadOne(ctx, client, options)
			stop()
			if err == nil && result != nil && result.Failure == nil && result.Document != nil {
				completed++
			} else {
				if status.Code(err) != codes.ResourceExhausted && (result == nil || result.Failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED) {
					t.Fatal("low allowance point query failed without memory-pressure evidence", result, err)
				}
				rejected++
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Logf("low positive/zero recovery allowance probe completed=%d rejected=%d", completed, rejected)
		return
	}
	memoryRecoveryBurst(t, service, fixture.DB+"/records/s:shared-large")
	probe := &http.Client{Timeout: time.Second}
	response, err := probe.Get("http://" + service.diagnostic + "/livez")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal("live process failed after shared-pressure recovery", response.StatusCode)
	}
}

func sharedPressureStat(t *testing.T, service *memoryRecoveryCLI, name string) uint64 {
	t.Helper()
	raw := packagedDocker(t, "exec", service.name, "cat", "/sys/fs/cgroup/memory.stat")
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == name {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
	t.Fatal("required cgroup accounting field absent", name)
	return 0
}
