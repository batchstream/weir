//go:build integration && darwin

package app

import (
	"context"
	"encoding/json"
	"github.com/batchstream/weir/internal/testutil"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmemory"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDarwinMemoryApplication(t *testing.T) {
	if os.Getenv("WEIR_M19_NATIVE") != "1" {
		t.Skip("explicit M19 native fixture required")
	}
	fixture := testmongo.OpenSecure(t)
	proxy := testmongo.StartProxy(t, &fixture.Fixture)
	proxy.DropCommand = "bulkWrite"
	cfg := packagedConfig(t, proxy.URI())
	cfg.Basic.Memory = ByteSize(testmemory.Budget)
	n := secureNode(t, cfg)
	client := endpointProcessClient(t, n.Addresses()[0])
	packagedCalls(t, client, fixture)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	read := &pb.ReadRequest{Resource: "weir://records/" + fixture.DB + "/records/s:artifact"}
	var producers sync.WaitGroup
	producers.Add(2)
	for range 2 {
		go func() {
			defer producers.Done()
			for ctx.Err() == nil {
				call, stop := context.WithTimeout(ctx, 40*time.Millisecond)
				_, _ = testutil.ExecuteRecord(call, client, testutil.RecordCommand(read))
				stop()
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}
	defer func() {
		cancel()
		producers.Wait()
	}()
	// Include diagnostics and continuous traffic in the measured baseline.
	darwinMetrics(t, n.DiagnosticAddress())
	time.Sleep(500 * time.Millisecond)
	runtime.GC()
	pressure := testmemory.Open(t)
	for _, step := range []struct {
		label   string
		percent uint64
		latched bool
	}{{"high", 85, true}, {"middle", 75, true}, {"low", 0, false}} {
		pressure.Set(t, step.percent)
		runtime.GC()
		started := time.Now()
		if step.label == "middle" {
			time.Sleep(300 * time.Millisecond)
		}
		for {
			s := n.guard.Snapshot()
			oracle := testmemory.Oracle(t)
			if !s.Unknown && s.Latched == step.latched && testmemory.Difference(s.Bytes, oracle) < testmemory.Tolerance {
				if step.label == "middle" && (s.Bytes <= testmemory.Budget*70/100 || s.Bytes >= testmemory.Budget*80/100) {
					t.Fatal("outside middle band", s)
				}
				t.Logf("app %s elapsed=%s footprint=%d SDK_oracle=%d latched=%v", step.label, time.Since(started), s.Bytes, oracle, s.Latched)
				break
			}
			if time.Since(started) > 2*time.Second {
				t.Fatal("fixed app convergence", s, oracle)
			}
			time.Sleep(20 * time.Millisecond)
		}
		darwinMetrics(t, n.DiagnosticAddress())
		if step.latched {
			request := budgetPut("weir://records/"+fixture.DB+"/records", "refused")
			routedResult94, err := testutil.ExecuteRecord(ctx, client, testutil.RecordCommand(request))
			result := routedResult94.GetMutation()
			if result != nil || status.Code(err) != codes.ResourceExhausted {
				t.Fatal("unsafe overload admission", result, err)
			}
		}
	}
	cancel()
	producers.Wait()
	filter := bson.D{{Key: "_id", Value: "refused"}}
	check, stop := context.WithTimeout(context.Background(), 3*time.Second)
	count, err := fixture.Admin.Database(fixture.DB).Collection("records").CountDocuments(check, filter)
	stop()
	if err != nil || count != 0 {
		t.Fatal("rejected mutation reached backend", count, err)
	}
	packagedCalls(t, client, fixture)
	darwinUnknown(t, client, fixture, proxy)
	began := time.Now()
	for range 3 {
		if err := n.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if time.Since(began) > 3*time.Second {
		t.Fatal("fixed repeated Close bound")
	}
	budgetWait(t, "app proxy sockets released", func() bool {
		n, _ := proxy.Sockets()
		return n == 0
	})
	t.Logf("continuous reads/cancellation, no rejected DB mutation; restored Read/Mutate/Bulk; repeated Close=%s sockets=0", time.Since(began))
}

func darwinMetrics(t *testing.T, address string) {
	t.Helper()
	metrics := testmetrics.Scrape(t, address)
	labels := map[string]string{"source": "darwin_phys_footprint"}
	footprint := testmetrics.Sample(metrics, "weir_memory_sample_bytes", labels).GetGauge().GetValue()
	if footprint <= 0 || testmetrics.Sum(metrics, "weir_memory_process_valid") != 1 || testmetrics.Sum(metrics, "weir_memory_unknown") != 0 {
		t.Fatal("Darwin memory metrics", footprint)
	}
	for _, source := range []string{"unobserved", "linux_rss", "go_sys_minus_released"} {
		labels := map[string]string{"source": source}
		if testmetrics.Sample(metrics, "weir_memory_sample_bytes", labels).GetGauge().GetValue() != 0 {
			t.Fatal("inactive source nonzero", source)
		}
	}
	labels = map[string]string{"state": "not_applicable"}
	if testmetrics.Sample(metrics, "weir_memory_cgroup_state", labels).GetGauge().GetValue() != 1 || testmetrics.Sum(metrics, "weir_memory_cgroup_valid") != 0 {
		t.Fatal("Darwin cgroup")
	}
	t.Logf("metrics darwin_phys_footprint=%g bytes; other sources=0, process_valid=1, unknown=0, cgroup=not_applicable", footprint)
}

func darwinUnknown(t *testing.T, client pb.StoreServiceClient, fixture *testmongo.SecureFixture, proxy *testmongo.Proxy) {
	t.Helper()
	before := len(proxy.Events())
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	request := budgetPut("weir://records/"+fixture.DB+"/records", "lost")
	proxy.DropRemaining.Store(1)
	routedResult155, err := testutil.ExecuteRecord(ctx, client, testutil.RecordCommand(request))
	result := routedResult155.GetMutation()
	if err != nil || result.GetOutcome() != pb.MutationOutcome_UNKNOWN {
		t.Fatal("lost ACK", result, err)
	}
	filter := bson.D{{Key: "_id", Value: "lost"}}
	count, err := fixture.Admin.Database(fixture.DB).Collection("records").CountDocuments(ctx, filter)
	if err != nil || count != 1 {
		t.Fatal("real DB effect", count, err)
	}
	time.Sleep(200 * time.Millisecond)
	updates := 0
	for _, e := range proxy.Events()[before:] {
		if e.Command == "bulkWrite" {
			updates++
			if !e.Acknowledged || !e.Dropped {
				t.Fatal("ACK loss evidence")
			}
		}
	}
	if updates != 1 || proxy.DropRemaining.Load() != 0 {
		t.Fatal("replay", updates)
	}
	t.Log("one acknowledged DB update; reply dropped; UNKNOWN; no replay")
}

func TestDarwinMemoryArtifact(t *testing.T) {
	if os.Getenv("WEIR_M19_ARTIFACT") != "1" {
		t.Skip("explicit exact M19 archive required")
	}
	binary := os.Getenv("WEIR_M19_BINARY")
	source := os.Getenv("WEIR_M19_SOURCE")
	if !filepath.IsAbs(binary) || len(source) != 40 {
		t.Fatal("artifact identity")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	output, err := exec.CommandContext(ctx, binary, "version").Output()
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	var version map[string]string
	if err := json.Unmarshal(output, &version); err != nil {
		t.Fatal(err)
	}
	if version["revision"] != source || version["state"] != "clean-commit" || version["target"] != "darwin/arm64" {
		t.Fatal(version)
	}
	t.Log("exact archive", string(output))
	fixture := testmongo.OpenSecure(t)
	proxy := testmongo.StartProxy(t, &fixture.Fixture)
	proxy.DropCommand = "bulkWrite"
	observation := &budgetObservation{}
	monitor := &event.CommandMonitor{Started: observation.start, Succeeded: observation.finish}
	proxy.Monitor = monitor
	cfg := packagedConfig(t, proxy.URI())
	cfg.Basic.Memory = 256 << 20
	p := startProcess(t, binary, cfg)
	t.Logf("owned artifact PID=%d", p.command.Process.Pid)
	client := endpointProcessClient(t, p.address)
	darwinMetrics(t, p.diagnostic)
	packagedCalls(t, client, fixture)
	darwinUnknown(t, client, fixture, proxy)
	// Hold a real in-flight request while the artifact begins SIGTERM drain.
	gate := make(chan struct{})
	observation.hold(gate, 0)
	var release sync.Once
	defer release.Do(func() {
		close(gate)
		observation.hold(nil, 0)
	})
	result := make(chan bool, 1)
	request := budgetPut("weir://records/"+fixture.DB+"/records", "inflight")
	go func() {
		call, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		result <- budgetLoadCall(call, client, request, 1)
	}()
	budgetWait(t, "artifact in-flight", func() bool {
		n, _, _, _ := observation.snapshot()
		return n == 1
	})
	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		p.stop(t)
	}()
	budgetWait(t, "artifact draining", func() bool { return healthProcessDarwin(t, p) == 503 })
	release.Do(func() {
		close(gate)
		observation.hold(nil, 0)
	})
	if !<-result {
		t.Fatal("admitted write lost in drain")
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("artifact close deadline")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("fixed in-flight close bound")
	}
	t.Logf("exact artifact in-flight SIGTERM/Wait=%s", time.Since(start))
	budgetWait(t, "artifact sockets released", func() bool {
		n, _ := proxy.Sockets()
		return n == 0
	})
	// Strong mmap hysteresis is confined to the opt-in app test above. The
	// archive has no allocation hook; verify a second normal start/stop here.
	fresh := startProcess(t, binary, cfg)
	freshClient := endpointProcessClient(t, fresh.address)
	packagedCalls(t, freshClient, fixture)
	darwinMetrics(t, fresh.diagnostic)
	start = time.Now()
	fresh.stop(t)
	if time.Since(start) > 3*time.Second {
		t.Fatal("normal SIGTERM bound")
	}
	t.Logf("exact artifact normal SIGTERM/Wait=%s", time.Since(start))
}

func healthProcessDarwin(t *testing.T, p *process) int {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + p.diagnostic + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}
