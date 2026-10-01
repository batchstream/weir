package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/store"
)

type routeMemorySnapshot struct {
	At            time.Time
	HeapAlloc     uint64
	HeapInuse     uint64
	HeapObjects   uint64
	Goroutines    int
	RSS           uint64
	CPUSeconds    float64
	Route         RouteSnapshot
	Store         store.Snapshot
	RelaySlots    int
	PhysicalConns int
	Executed      int64
	PeakBatch     int64
}

type routeMemoryAddress struct {
	RPC, Probe string
	PID        int
}

// Helpers are separate router processes for attribution, not Lua workers.
// The explicit opt-in parent test owns their lifetime and loopback listeners.
func TestRouteMemoryProcess(t *testing.T) {
	role := os.Getenv("WEIR_ROUTE_MEMORY_HELPER")
	if role == "" {
		t.Skip("only the explicit Route memory acceptance parent starts this process")
	}
	limits := DefaultLimits()
	limits.Stall = 5 * time.Second
	opts := routeAcceptanceNodeOptions{limits: limits, hops: 4, peer: role != "entry"}
	if role == "executor" {
		adapter := newRouteAcceptanceAdapter(2 << 20)
		adapter.seen = nil
		opts.adapter = adapter
	} else {
		opts.target = os.Getenv("WEIR_ROUTE_MEMORY_TARGET")
		if opts.target == "" {
			t.Fatal("memory relay requires an owned downstream target")
		}
	}
	node := startRouteAcceptanceNode(t, opts)
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var once sync.Once
	mux := http.NewServeMux()
	mux.HandleFunc("GET /snapshot", func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("gc") == "1" {
			runtime.GC()
			debug.FreeOSMemory()
		}
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		snapshot := routeMemorySnapshot{At: time.Now(), HeapAlloc: memory.HeapAlloc, HeapInuse: memory.HeapInuse, HeapObjects: memory.HeapObjects, Goroutines: runtime.NumGoroutine(), Route: node.server.Snapshot()}
		if node.remote != nil {
			snapshot.RelaySlots = len(node.remote.slots)
		}
		node.server.connections.Range(func(_, _ any) bool { snapshot.PhysicalConns++; return true })
		if node.runtime != nil {
			snapshot.Store = node.runtime.Snapshot()
			snapshot.Executed = node.adapter.executed.Load()
			snapshot.PeakBatch = node.adapter.maxBatch.Load()
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(snapshot)
	})
	mux.HandleFunc("POST /stop", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
		once.Do(func() { close(stop) })
	})
	probeServer := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	probeDone := make(chan error, 1)
	go func() { probeDone <- probeServer.Serve(probe) }()
	t.Cleanup(func() {
		_ = probeServer.Close()
		_ = probe.Close()
		if err := <-probeDone; !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	})
	address := routeMemoryAddress{RPC: node.address, Probe: "http://" + probe.Addr().String(), PID: os.Getpid()}
	if err := json.NewEncoder(os.Stdout).Encode(address); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stop:
	case <-time.After(2 * time.Minute):
		t.Fatal("memory parent did not stop its router process")
	}
}

type routeMemoryProcess struct {
	role    string
	address routeMemoryAddress
	command *exec.Cmd
	joined  chan error
	output  *bytesLimitBuffer
}

// Test-process logs have a separate finite budget too; a broken helper cannot
// hide an unbounded buffer in the measurement harness.
type bytesLimitBuffer struct {
	mu   sync.Mutex
	raw  []byte
	lost int
}

func (b *bytesLimitBuffer) Write(raw []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	count := min(len(raw), (64<<10)-len(b.raw))
	b.raw = append(b.raw, raw[:count]...)
	b.lost += len(raw) - count
	return len(raw), nil
}

func (b *bytesLimitBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.raw) + fmt.Sprintf(" [discarded=%d]", b.lost)
}

func startRouteMemoryProcess(t *testing.T, role, target string) *routeMemoryProcess {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-test.run=^TestRouteMemoryProcess$", "-test.timeout=120s")
	command.Env = append(os.Environ(), "WEIR_ROUTE_MEMORY_HELPER="+role, "WEIR_ROUTE_MEMORY_TARGET="+target)
	output := &bytesLimitBuffer{}
	command.Stderr = output
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	p := &routeMemoryProcess{role: role, command: command, joined: make(chan error, 1), output: output}
	initialized := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if !scanner.Scan() {
			initialized <- fmt.Errorf("helper exited before address: %v", scanner.Err())
		} else {
			initialized <- json.Unmarshal(scanner.Bytes(), &p.address)
		}
		for scanner.Scan() {
			_, _ = output.Write(scanner.Bytes())
		}
		p.joined <- command.Wait()
	}()
	t.Cleanup(func() {
		if p.address.Probe != "" {
			client := &http.Client{Timeout: time.Second}
			reply, err := client.Post(p.address.Probe+"/stop", "text/plain", nil)
			if err == nil {
				_ = reply.Body.Close()
			}
		}
		select {
		case err := <-p.joined:
			if err != nil {
				t.Error("memory helper failed", role, err, output.String())
			}
		case <-time.After(5 * time.Second):
			_ = command.Process.Kill()
			<-p.joined
			t.Error("owned memory helper required forced termination", role, output.String())
		}
	})
	select {
	case err := <-initialized:
		if err != nil {
			t.Fatal(err, output.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("memory helper initialization timed out", role)
	}
	if p.address.PID != command.Process.Pid || p.address.Probe == "" || p.address.RPC == "" {
		t.Fatal("helper ownership or address mismatch", role)
	}
	return p
}

func routeMemorySample(p *routeMemoryProcess, forceGC bool) (routeMemorySnapshot, error) {
	url := p.address.Probe + "/snapshot"
	if forceGC {
		url += "?gc=1"
	}
	client := &http.Client{Timeout: 2 * time.Second}
	reply, err := client.Get(url)
	if err != nil {
		snapshot := routeMemorySnapshot{}
		return snapshot, err
	}
	defer reply.Body.Close()
	var snapshot routeMemorySnapshot
	err = json.NewDecoder(io.LimitReader(reply.Body, 16<<10)).Decode(&snapshot)
	return snapshot, err
}

type routeMemoryRun struct {
	Records        int
	RecordBytes    int
	ActiveRPCs     int
	ConsumedBytes  uint64
	ResponseFrames int
	Seconds        float64
	ThroughputMiB  float64
	Failures       int
	Baseline       map[string]routeMemorySnapshot
	Peak           map[string]routeMemorySnapshot
	AfterGC        map[string]routeMemorySnapshot
	Samples        int
}

func routeMemoryOSSample(processes []*routeMemoryProcess) (map[int][2]float64, error) {
	ids := make([]string, len(processes))
	for i, process := range processes {
		ids[i] = strconv.Itoa(process.address.PID)
	}
	command := exec.Command("ps", "-o", "pid=,rss=,time=", "-p", strings.Join(ids, ","))
	raw, err := command.Output()
	if err != nil {
		return nil, err
	}
	values := make(map[int][2]float64)
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return nil, errors.New("unexpected ps memory/CPU sample")
		}
		pid, pidErr := strconv.Atoi(fields[0])
		rss, rssErr := strconv.ParseFloat(fields[1], 64)
		parts := strings.Split(fields[2], ":")
		cpu := float64(0)
		for _, part := range parts {
			n, err := strconv.ParseFloat(part, 64)
			if err != nil {
				return nil, err
			}
			cpu = cpu*60 + n
		}
		if pidErr != nil || rssErr != nil {
			return nil, errors.New("invalid ps PID/RSS")
		}
		values[pid] = [2]float64{rss * 1024, cpu}
	}
	if len(values) != len(processes) {
		return nil, errors.New("missing owned process memory sample")
	}
	return values, nil
}

func routeMemoryCapture(processes []*routeMemoryProcess, forceGC bool) (map[string]routeMemorySnapshot, error) {
	result := make(map[string]routeMemorySnapshot)
	for _, process := range processes {
		snapshot, err := routeMemorySample(process, forceGC)
		if err != nil {
			return nil, err
		}
		result[process.role] = snapshot
	}
	osValues, err := routeMemoryOSSample(processes)
	if err != nil {
		return nil, err
	}
	for _, process := range processes {
		snapshot := result[process.role]
		values := osValues[process.address.PID]
		snapshot.RSS, snapshot.CPUSeconds = uint64(values[0]), values[1]
		result[process.role] = snapshot
	}
	return result, nil
}

func mergeRouteMemoryPeak(peak, sample map[string]routeMemorySnapshot) {
	for role, value := range sample {
		old := peak[role]
		old.HeapAlloc = max(old.HeapAlloc, value.HeapAlloc)
		old.HeapInuse = max(old.HeapInuse, value.HeapInuse)
		old.HeapObjects = max(old.HeapObjects, value.HeapObjects)
		old.Goroutines = max(old.Goroutines, value.Goroutines)
		old.RSS = max(old.RSS, value.RSS)
		old.CPUSeconds = max(old.CPUSeconds, value.CPUSeconds)
		old.Route.ActiveRPCs = max(old.Route.ActiveRPCs, value.Route.ActiveRPCs)
		old.Route.Outstanding = max(old.Route.Outstanding, value.Route.Outstanding)
		old.Route.OutstandingBytes = max(old.Route.OutstandingBytes, value.Route.OutstandingBytes)
		old.Route.PeakOutstanding = value.Route.PeakOutstanding
		old.Route.PeakOutstandingBytes = value.Route.PeakOutstandingBytes
		old.Store.Pending = max(old.Store.Pending, value.Store.Pending)
		old.Store.PendingBytes = max(old.Store.PendingBytes, value.Store.PendingBytes)
		old.Store.Active = max(old.Store.Active, value.Store.Active)
		old.Store.Retained = max(old.Store.Retained, value.Store.Retained)
		old.Store.ResultBytes = max(old.Store.ResultBytes, value.Store.ResultBytes)
		old.Store.WorkingBytes = max(old.Store.WorkingBytes, value.Store.WorkingBytes)
		old.Store.Publishers = max(old.Store.Publishers, value.Store.Publishers)
		old.RelaySlots = max(old.RelaySlots, value.RelaySlots)
		old.PhysicalConns = max(old.PhysicalConns, value.PhysicalConns)
		old.Executed = max(old.Executed, value.Executed)
		old.PeakBatch = max(old.PeakBatch, value.PeakBatch)
		peak[role] = old
	}
}

func TestRouteMemory200MiBTwoRelays(t *testing.T) {
	if os.Getenv("WEIR_ROUTE_MEMORY") != "1" {
		t.Skip("explicit resource acceptance: WEIR_ROUTE_MEMORY=1 go test ./internal/server -run '^TestRouteMemory200MiBTwoRelays$' -count=1 -v")
	}
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Fatal("explicit resource acceptance requires Darwin or Linux RSS/CPU sampling")
	}
	executor := startRouteMemoryProcess(t, "executor", "")
	relay := startRouteMemoryProcess(t, "relay", executor.address.RPC)
	entry := startRouteMemoryProcess(t, "entry", relay.address.RPC)
	processes := []*routeMemoryProcess{executor, relay, entry}
	client := routeAcceptanceClient(t, entry.address.RPC)
	runs := make([]routeMemoryRun, 0, 2)
	for _, count := range []int{100, 400} {
		baseline, err := routeMemoryCapture(processes, true)
		if err != nil {
			t.Fatal(err)
		}
		run := routeMemoryRun{Records: count, RecordBytes: 2 << 20, ActiveRPCs: 1, Baseline: baseline, Peak: make(map[string]routeMemorySnapshot)}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		stream, err := client.Route(ctx)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		sent := make(chan error, 1)
		go func() {
			for id := uint64(1); id <= uint64(count); id++ {
				request := routeAcceptanceRead(id, fmt.Sprintf("records/s:large%d", id))
				if err := stream.Send(request); err != nil {
					sent <- err
					return
				}
			}
			sent <- stream.CloseSend()
		}()
		started := time.Now()
		// This pause fills every real HTTP/2 window before consumption begins.
		time.Sleep(250 * time.Millisecond)
		paused, err := routeMemoryCapture(processes, false)
		if err != nil {
			cancel()
			<-sent
			t.Fatal(err)
		}
		mergeRouteMemoryPeak(run.Peak, paused)
		for role, snapshot := range paused {
			if snapshot.Route.Outstanding == 0 || snapshot.Route.Outstanding > 8 || snapshot.Route.OutstandingBytes > 16<<20 {
				cancel()
				<-sent
				t.Fatal("slow consumer did not exercise a bounded active backlog", role, snapshot.Route)
			}
		}
		if paused["executor"].Executed-baseline["executor"].Executed > 8 {
			cancel()
			<-sent
			t.Fatal("blocked output did not stop execution at the finite in-flight bound")
		}
		decoder := newRouteAcceptanceDecoder()
		consumed := make(chan error, 1)
		go func() {
			for {
				response, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					consumed <- nil
					return
				}
				if err != nil {
					consumed <- err
					return
				}
				event, err := decoder.consume(response)
				if err != nil {
					consumed <- err
					return
				}
				if event != nil {
					data := event.GetResult().GetRead().GetDocument().GetData()
					want := byte(1 + event.GetResult().Index%251)
					if len(data) != 2<<20 {
						consumed <- errors.New("memory run did not transfer a full legal 2 MiB record")
						return
					}
					for _, value := range data {
						if value != want {
							consumed <- errors.New("memory run response corrupted")
							return
						}
					}
				}
				// Fixed per-frame pacing makes 100/400 records comparable.
				time.Sleep(time.Millisecond)
			}
		}()
		ticker := time.NewTicker(50 * time.Millisecond)
		finished := false
		for !finished {
			select {
			case err := <-consumed:
				if err != nil {
					cancel()
					<-sent
					t.Fatal(err)
				}
				finished = true
			case <-ticker.C:
				sample, err := routeMemoryCapture(processes, false)
				if err != nil {
					cancel()
					<-sent
					<-consumed
					t.Fatal(err)
				}
				mergeRouteMemoryPeak(run.Peak, sample)
				run.Samples++
			}
		}
		ticker.Stop()
		if err := <-sent; err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		run.Seconds = time.Since(started).Seconds()
		run.ConsumedBytes, run.ResponseFrames = decoder.bytes, decoder.frames
		run.ThroughputMiB = float64(decoder.bytes) / (1 << 20) / run.Seconds
		if decoder.count != count || decoder.bytes != uint64(count)*(2<<20) || len(decoder.partial) != 0 || run.Samples < 10 {
			t.Fatal("resource test did not fully transfer and sample its legal workload", count, decoder.count, decoder.bytes, run.Samples)
		}
		time.Sleep(250 * time.Millisecond)
		run.AfterGC, err = routeMemoryCapture(processes, true)
		if err != nil {
			t.Fatal(err)
		}
		for role, peak := range run.Peak {
			if peak.Route.ActiveRPCs > 1 || peak.Route.Outstanding > 8 || peak.Route.OutstandingBytes > 16<<20 || peak.Route.PeakOutstanding > 8 || peak.Route.PeakOutstandingBytes > 16<<20 {
				t.Fatal("transport ledger exceeded configured memory/count bounds", role, peak.Route)
			}
			limits := store.DefaultLimits()
			if peak.Store.Pending > limits.PendingOperations || peak.Store.PendingBytes > limits.PendingBytes || peak.Store.Retained > limits.ResultOperations || peak.Store.ResultBytes > limits.ResultBytes || peak.Store.WorkingBytes > limits.WorkingBytes || peak.Store.Publishers > limits.ResultOperations {
				t.Fatal("executor queue, retained results or working set exceeded its configured budget", role, peak.Store)
			}
			after := run.AfterGC[role]
			if after.Route.ActiveRPCs != 0 || after.Route.Outstanding != 0 || after.Route.OutstandingBytes != 0 || after.RelaySlots != 0 || after.Store.Pending != 0 || after.Store.Active != 0 || after.Store.Retained != 0 || after.Store.ResultBytes != 0 || after.Store.WorkingBytes != 0 || after.Store.Publishers != 0 {
				t.Fatal("load left live RPCs/tasks/buffers", role, after)
			}
			if after.HeapAlloc > baseline[role].HeapAlloc+8<<20 || after.Goroutines > baseline[role].Goroutines+20 {
				t.Fatal("load did not release live heap or goroutines after GC", role, baseline[role], after)
			}
		}
		runs = append(runs, run)
		encoded, _ := json.Marshal(run)
		t.Log("ROUTE_MEMORY_RUN=" + string(encoded))
	}
	// Increasing response volume by 4x must not produce a comparable increase in
	// either relay's retained working set. RSS is observed separately, not used
	// as proof that every page must be returned immediately after GC.
	for _, role := range []string{"entry", "relay"} {
		first, second := runs[0].Peak[role], runs[1].Peak[role]
		if second.HeapAlloc > first.HeapAlloc+(16<<20) || second.HeapInuse > first.HeapInuse+(16<<20) || second.HeapObjects > first.HeapObjects+10000 {
			t.Fatal("relay working set grew with total batch volume", role, first, second)
		}
	}
	if artifact := os.Getenv("WEIR_ROUTE_MEMORY_REPORT"); artifact != "" {
		report := map[string]any{"platform": runtime.GOOS + "/" + runtime.GOARCH, "go": runtime.Version(), "two_forwarders": true, "pacing_per_frame_ms": 1, "runs": runs}
		raw, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(artifact, raw, 0644); err != nil {
			t.Fatal(err)
		}
	}
}
