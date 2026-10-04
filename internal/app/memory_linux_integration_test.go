//go:build integration && linux

package app

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var memorySequence atomic.Uint64

// This child is test-only. It touches at most 416 MiB, uses a fixed 45s watchdog,
// and is additionally confined by the fixture's 512 MiB cgroup hard limit.
func TestMemoryPressureHelper(t *testing.T) {
	if os.Getenv("WEIR_MEMORY_HELPER") != "1" {
		t.Skip("owned memory helper only")
	}
	if os.Getenv("WEIR_MEMORY_NATIVE") != "1" || memoryFile(t, "/sys/fs/cgroup/memory.max") != 512<<20 {
		t.Fatal("helper requires owned 512 MiB cgroup")
	}
	watchdog := time.AfterFunc(45*time.Second, func() { os.Exit(2) })
	defer watchdog.Stop()
	var pages [][]byte
	defer func() {
		for _, page := range pages {
			_ = syscall.Munmap(page)
		}
	}()
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64), 64)
	for scanner.Scan() {
		count, err := strconv.Atoi(scanner.Text())
		if err != nil || count < 0 || count > 416 {
			t.Fatal("allocation ceiling")
		}
		for len(pages) > count {
			last := len(pages) - 1
			if err := syscall.Munmap(pages[last]); err != nil {
				t.Fatal(err)
			}
			pages = pages[:last]
		}
		for len(pages) < count {
			page, err := syscall.Mmap(-1, 0, 1<<20, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < len(page); i += os.Getpagesize() {
				page[i] = 1
			}
			pages = append(pages, page)
		}
		fmt.Printf("allocated=%d\n", count)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}

type memoryPressure struct {
	command *exec.Cmd
	input   io.WriteCloser
	output  *bufio.Scanner
	count   int
	done    chan error
	stopped bool
}

func startMemoryPressure(t *testing.T) *memoryPressure {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	t.Cleanup(cancel)
	command := exec.CommandContext(ctx, binary, "-test.run=^TestMemoryPressureHelper$")
	command.Env = append(os.Environ(), "WEIR_MEMORY_HELPER=1")
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	p := &memoryPressure{command: command, input: input, output: bufio.NewScanner(output), done: make(chan error, 1)}
	go func() { p.done <- command.Wait() }()
	t.Cleanup(func() { p.stop(t) })
	p.set(t, 0)
	return p
}

func (p *memoryPressure) set(t *testing.T, count int) {
	t.Helper()
	if _, err := fmt.Fprintln(p.input, count); err != nil {
		t.Fatal(err)
	}
	if !p.output.Scan() || p.output.Text() != fmt.Sprintf("allocated=%d", count) {
		t.Fatal("pressure child failed", p.output.Text(), p.output.Err())
	}
	p.count = count
}

func (p *memoryPressure) stop(t *testing.T) {
	t.Helper()
	if p.stopped {
		return
	}
	p.stopped = true
	_ = p.input.Close()
	select {
	case err := <-p.done:
		if err != nil {
			t.Error("pressure child", err)
		}
	case <-time.After(2 * time.Second):
		_ = p.command.Process.Kill()
		<-p.done
		t.Error("pressure child did not exit")
	}
	t.Logf("helper PID=%d released and Wait completed", p.command.Process.Pid)
}

func (p *memoryPressure) target(t *testing.T, percent uint64) {
	t.Helper()
	current := memoryFile(t, "/sys/fs/cgroup/memory.current")
	target := uint64(512<<20) * percent / 100
	base := int64(current) - int64(p.count<<20)
	count := int((int64(target) - base) / (1 << 20))
	if count < 0 || count > 416 {
		t.Fatal("fixture headroom insufficient", base, count)
	}
	p.set(t, count)
}

func memoryFile(t *testing.T, name string) uint64 {
	t.Helper()
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func memoryState(t *testing.T, p *process, label string, latched bool) {
	t.Helper()
	start := time.Now()
	for {
		metrics := testmetrics.Scrape(t, p.diagnostic)
		value := testmetrics.Sum(metrics, "weir_memory_latched")
		if value == 0 && !latched || value == 1 && latched {
			if testmetrics.Sum(metrics, "weir_memory_unknown") != 0 ||
				testmetrics.Sum(metrics, "weir_memory_cgroup_valid") != 1 ||
				testmetrics.Sum(metrics, "weir_memory_cgroup_limit_bytes") != 512<<20 {
				t.Fatal("invalid Linux profile")
			}
			current := memoryFile(t, "/sys/fs/cgroup/memory.current")
			rss := testmetrics.Sample(metrics, "weir_memory_sample_bytes", map[string]string{"source": "linux_rss"}).GetGauge().GetValue()
			observed := testmetrics.Sum(metrics, "weir_memory_cgroup_current_bytes")
			if rss >= 128<<20 {
				t.Fatal("unexpected Weir RSS; test must distinguish same-group pressure", rss)
			}
			delta := int64(current) - int64(observed)
			if delta < 0 {
				delta = -delta
			}
			independentRSS := memoryProcessRSS(t, p.command.Process.Pid)
			rssDelta := int64(independentRSS) - int64(rss)
			if rssDelta < 0 {
				rssDelta = -rssDelta
			}
			if delta > 16<<20 || rssDelta > 8<<20 {
				if time.Since(start) > 2*time.Second {
					t.Fatal("fixed 2s observation convergence deadline", delta, rssDelta)
				}
				t.Logf("%s transitional sample delta: cgroup=%d RSS=%d; awaiting next sample within same deadline", label, delta, rssDelta)
				time.Sleep(20 * time.Millisecond)
				continue
			}
			if label == "middle" && (observed <= float64(512<<20)*.70 || observed >= float64(512<<20)*.80) {
				t.Fatal("not in hysteresis band", observed)
			}
			t.Logf(
				"%s PID=%d elapsed=%s RSS=%g independent_RSS=%d RSS_delta=%d cgroup_sample=%g independent_current=%d limit=%d latch=%v",
				label,
				p.command.Process.Pid,
				time.Since(start),
				rss,
				independentRSS,
				rssDelta,
				observed,
				current,
				512<<20,
				latched,
			)
			return
		}
		if time.Since(start) > 2*time.Second {
			t.Fatal("fixed 2s overload/recovery deadline", label, value)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestLinuxMemoryCLI(t *testing.T) {
	if os.Getenv("WEIR_MEMORY_NATIVE") != "1" {
		t.Skip("requires owned scripts/test-memory-linux.py fixture")
	}
	if memoryFile(t, "/sys/fs/cgroup/memory.max") != 512<<20 {
		t.Fatal("requires 512 MiB owned cgroup")
	}
	t.Logf("native CLI suite Go=%s OS=%s arch=%s runner_PID=%d", runtime.Version(), runtime.GOOS, runtime.GOARCH, os.Getpid())
	// The script supplies its owned private-network DB or explicit Darwin loopback fixture.
	address := os.Getenv("WEIR_MEMORY_MONGO_ADDR")
	if address != "memory-mongo:27017" && !strings.HasPrefix(address, "host.docker.internal:") {
		t.Fatal("requires owned fixture address")
	}
	uri := "mongodb://" + address + "/?directConnection=true&serverMonitoringMode=poll"
	opts := options.Client().ApplyURI(uri).SetRetryReads(false).SetRetryWrites(false).SetMaxPoolSize(2).SetServerSelectionTimeout(time.Second)
	admin, err := mongo.Connect(opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	db := fmt.Sprintf("memory_%d_%d", os.Getpid(), memorySequence.Add(1))
	if err := admin.Database(db).CreateCollection(ctx, "records"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := admin.Database(db).Drop(cleanup); err != nil {
			t.Error(err)
		}
		if err := admin.Disconnect(cleanup); err != nil {
			t.Error(err)
		}
	})
	fixture := &testmongo.Fixture{Admin: admin, URI: uri, DB: db}
	proxy := testmongo.StartProxy(t, fixture)
	observer := &budgetObservation{}
	monitor := &event.CommandMonitor{Started: observer.start, Succeeded: observer.finish}
	proxy.Monitor = monitor
	backend := mongoFixtureConfig(t, proxy.URI())
	local := &Local{MongoDB: backend, MaxConcurrency: 2, MaxBatchOperations: 1}
	service := StoreConfig{Name: "records", Local: local}

	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	cfg.Basic.Listeners.Peer = "127.0.0.1:0"
	cfg.Basic.Diagnostics.Address = "127.0.0.1:0"
	cfg.Routing.Stores = []StoreConfig{service}

	// The declaration covers native buffers; cgroup pressure still uses its 512MiB limit.
	cfg.Basic.Memory = 2 << 30
	p := startProcess(t, "/fixture/weir", cfg)
	client := endpointProcessClient(t, p.address)
	root := db + "/records"
	request := budgetPut(root, "before")
	recordFixture := testutil.RecordRequest("records", request)
	for mode := 0; mode < 3; mode++ {
		success := budgetLoadCall(ctx, client, recordFixture, mode)
		if !success {
			t.Fatal("pre-pressure Read/Mutate/Bulk", mode)
		}
	}
	front := startProcess(t, "/fixture/weir", cfg)
	frontClient := endpointProcessClient(t, front.address)
	success := budgetLoadCall(ctx, frontClient, recordFixture, 1)
	if !success {
		t.Fatal("second executor before pressure")
	}
	helper := startMemoryPressure(t)
	memoryState(t, p, "low", false)
	// Hold one admitted batch mutation at the owned proxy before the real DB write.
	gate := make(chan struct{})
	observer.hold(gate, 0)
	released := false
	defer func() {
		if !released {
			close(gate)
		}
	}()
	admitted := budgetPut(root, "admitted")
	fixtureRequest := testutil.RecordRequest("records", admitted)
	batch := &pb.MutateBatchRequest{StoreName: "records", Requests: []*pb.MutateRequest{fixtureRequest.Operation.Mutate}}
	responses := make(chan *pb.MutateBatchResponse, 1)
	callErrors := make(chan error, 1)
	go func() {
		response, err := client.Mutate(ctx, batch)
		responses <- response
		callErrors <- err
	}()
	budgetWait(t, "admitted batch", func() bool {
		active, _, _, _ := observer.snapshot()
		return active == 1
	})
	helper.target(t, 84)
	memoryState(t, p, "high", true)
	memoryState(t, front, "high-second", true)
	read := &pb.ReadRequest{Resource: request.Resource}
	if _, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("records", read)); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("Read not rejected", err)
	}
	refused := budgetPut(root, "refused")
	if result, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("records", refused)); status.Code(err) != codes.ResourceExhausted || result != nil {
		t.Fatal("Mutate not safely rejected", result, err)
	}
	if _, err := testutil.ExecuteRecord(ctx, frontClient, testutil.RecordRequest("records", read)); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("second executor admission", err)
	}
	// Refusing fresh RPCs must not discard a response from an admitted RPC.
	close(gate)
	released = true
	observer.hold(nil, 0)
	response := <-responses
	callErr := <-callErrors
	if callErr != nil || len(response.GetResults()) != 1 || response.Results[0].GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("admitted batch result lost under overload", response, callErr)
	}
	if healthProcess(t, p, "/readyz") != 200 || healthProcess(t, p, "/livez") != 200 {
		t.Fatal("overload changed health")
	}
	helper.target(t, 75)
	time.Sleep(300 * time.Millisecond)
	memoryState(t, p, "middle", true)
	helper.set(t, 0)
	memoryState(t, p, "recovered", false)
	memoryState(t, front, "recovered-second", false)
	fresh := budgetPut(root, "after")
	freshFixture := testutil.RecordRequest("records", fresh)
	for mode := 0; mode < 3; mode++ {
		success := budgetLoadCall(ctx, client, freshFixture, mode)
		if !success {
			t.Fatal("fresh call failed to recover", mode)
		}
	}
	success = budgetLoadCall(ctx, frontClient, freshFixture, 1)
	if !success {
		t.Fatal("second executor recovery")
	}
	filter := bson.D{{Key: "_id", Value: "refused"}}
	if n, err := admin.Database(db).Collection("records").CountDocuments(ctx, filter); err != nil || n != 0 {
		t.Fatal("refused mutation executed", n, err)
	}
	// Wire counts distinguish independent calls from implicit write replays.
	updates := 0
	for _, e := range proxy.Events() {
		if e.Command == "bulkWrite" {
			updates++
		}
	}
	if updates != 7 {
		t.Fatal("unexpected replay/missing update", updates)
	}
	t.Log("real Mongo updates=7 (before one + two batch items, admitted one, after one + two batch items); refused=0; no replay")
	helper.target(t, 84)
	memoryState(t, p, "shutdown-high", true)
	start := time.Now()
	front.stop(t)
	p.stop(t)
	if time.Since(start) > 3*time.Second {
		t.Fatal("fixed 3s CLI shutdown bound")
	}
	budgetWait(t, "proxy sockets released", func() bool {
		current, _ := proxy.Sockets()
		return current == 0
	})
	helper.stop(t)
	for _, pid := range []int{p.command.Process.Pid, front.command.Process.Pid, helper.command.Process.Pid} {
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); !os.IsNotExist(err) {
			t.Fatal("owned child remains", pid, err)
		}
	}
	t.Logf("SIGTERM under high pressure: CLI Wait success in %s; proxy sockets=0; all 3 owned child PIDs absent", time.Since(start))
	events, err := os.ReadFile("/sys/fs/cgroup/memory.events")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cgroup memory.events: %s", strings.TrimSpace(string(events)))
	for _, line := range strings.Split(string(events), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && (fields[0] == "oom" || fields[0] == "oom_kill" || fields[0] == "max") && fields[1] != "0" {
			t.Fatal("hard-limit/OOM event", line)
		}
	}
}

func healthProcess(t *testing.T, p *process, path string) int {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + p.diagnostic + path)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}

func memoryProcessRSS(t *testing.T, pid int) uint64 {
	t.Helper()
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/smaps_rollup", pid))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) == 3 && f[0] == "Rss:" {
			n, err := strconv.ParseUint(f[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return n * 1024
		}
	}
	t.Fatal("missing process RSS")
	return 0
}
