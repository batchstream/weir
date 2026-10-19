//go:build integration

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testexecution"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testsearch"
)

type finiteCompletion struct {
	Schema        int      `json:"schema"`
	Stage         string   `json:"stage"`
	Profile       string   `json:"profile"`
	Revision      string   `json:"revision"`
	DriverPID     int      `json:"driver_pid"`
	Cases         []string `json:"cases"`
	Samples       int      `json:"samples"`
	Documents     int      `json:"documents"`
	Qualification bool     `json:"qualification"`
}

func finiteInputs(t *testing.T) *testexecution.ValidatedInputs {
	t.Helper()
	if os.Getenv("WEIR_FINITE_NATIVE") != "1" {
		t.Skip("requires explicit owned finite native controller; a skip cannot satisfy its completion contract")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("finite executor requires native Linux; no Desktop fallback")
	}
	inputs := loadProductionExecution(t, "functional")
	if inputs.Profile().ID != testexecution.LinuxFunctionalID {
		t.Fatal("finite profile required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(ctx, inputs.CLIPath(), "version").Output()
	if err != nil {
		t.Fatal("native CLI version", err)
	}
	if err := inputs.CheckVersion(raw); err != nil {
		t.Fatal(err)
	}
	return inputs
}

func finiteRecord(t *testing.T, record finiteCompletion) {
	t.Helper()
	if t.Failed() || t.Skipped() {
		return
	}
	path := os.Getenv("WEIR_FINITE_COMPLETION")
	if path == "" {
		t.Fatal("explicit task-owned completion path required")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	err = json.NewEncoder(f).Encode(record)
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		t.Fatal("completion write", err, closeErr)
	}
}

// The controller requires this selected test's structured record before any
// backend/CLI creation. An exit status with no selected tests is not evidence.
func TestProductionLinuxPreflight(t *testing.T) {
	inputs := finiteInputs(t)
	record := finiteCompletion{Schema: 1, Stage: "preflight", Profile: inputs.Profile().ID, Revision: inputs.HarnessSource().Revision, DriverPID: os.Getpid(), Cases: []string{"source", "cli", "kernel-driver"}}
	finiteRecord(t, record)
}

func finiteLoopback(address string) error {
	host, port, err := net.SplitHostPort(address)
	n, parseErr := strconv.Atoi(port)
	if err != nil || parseErr != nil || host != "127.0.0.1" || n < 1 || n > 65535 {
		return fmt.Errorf("finite endpoint must be explicit loopback")
	}
	return nil
}

func TestProductionLinuxFinite(t *testing.T) {
	inputs := finiteInputs(t)
	address, diagnostics := os.Getenv("WEIR_FINITE_APP"), os.Getenv("WEIR_FINITE_DIAGNOSTICS")
	if err := finiteLoopback(address); err != nil {
		t.Fatal(err)
	}
	if err := finiteLoopback(diagnostics); err != nil {
		t.Fatal(err)
	}
	record := finiteCompletion{Schema: 1, Stage: "finite", Profile: inputs.Profile().ID, Revision: inputs.HarnessSource().Revision, DriverPID: os.Getpid()}
	// Registered first: completion is written only after fixture cleanup succeeds.
	t.Cleanup(func() { finiteRecord(t, record) })
	opts := testsearch.OwnedEndpoint{URL: os.Getenv("WEIR_FINITE_ORACLE"), Owner: os.Getenv("WEIR_FINITE_OWNER")}
	backend := testsearch.OpenOwned(t, opts)
	pad := "finite-native-no-business-replay"
	expected := map[string]qualificationRecord{"hot-0": {N: 1, Pad: pad}, "lua-000": {N: 0, Pad: pad}}
	dataset := &qualificationDataset{store: "search", root: backend.Index, media: "application/json", search: backend, pads: []string{pad}, expected: expected}
	for id, document := range expected {
		raw, err := json.Marshal(document)
		if err != nil || len(raw) > testexecution.FunctionalDocumentBytes {
			t.Fatal("finite document bound", err)
		}
		status, _ := backend.Do(t, http.MethodPut, "/"+backend.Index+"/_doc/"+id+"?refresh=wait_for", string(raw))
		if status != 201 {
			t.Fatal("finite seed", status)
		}
	}
	request, err := http.NewRequest(http.MethodGet, "/"+backend.Index+"/_doc/hot-0", nil)
	if err != nil {
		t.Fatal(err)
	}
	native, err := weirclient.NewHTTPNativeRequest("", request)
	if err != nil {
		t.Fatal(err)
	}
	dataset.native = []*pb.NativeRequest{native}
	initial := testmetrics.Scrape(t, diagnostics)
	baseline := qualificationExecutionMemory(t, initial, 1<<30)["budget_rejections_total"]
	record.Samples++
	if baseline != 0 || initial["weir_memory_unknown"] == nil || testmetrics.Sum(initial, "weir_memory_go_runtime_managed") != 1 || testmetrics.Sum(initial, "weir_memory_cgroup_valid") != 1 || testmetrics.Sum(initial, "weir_memory_unknown") != 0 || testmetrics.Sum(initial, "weir_memory_cgroup_limit_bytes") != 1<<30 {
		t.Fatal("automatic cgroup memory policy missing")
	}
	ctx, cancel := context.WithTimeout(t.Context(), testexecution.FunctionalDeadline)
	defer cancel()
	openOptions := weirclient.OpenOptions{Seed: address, Stores: []string{"search"}}
	client, err := weirclient.Open(ctx, openOptions)
	if err != nil {
		t.Fatal("real SDK Resolve", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	record.Cases = append(record.Cases, "Resolve")
	// Reuse the real SDK operation/acknowledgement validators, not the formal
	// workload launcher, arrival policy, replay logic or qualification result.
	for _, kind := range []int{1, 0, 2, 3, 4} {
		job := qualificationJob{kind: kind, payload: 0, counter: 0, sequence: kind}
		if err := executeQualificationJob(ctx, client, dataset, job); err != nil {
			t.Fatal(qualificationMethod(kind), err)
		}
		rememberQualificationMutation(dataset, job)
		record.Cases = append(record.Cases, qualificationMethod(kind))
	}
	update := qualificationRecord{N: 2, Pad: pad}
	raw, err := json.Marshal(update)
	if err != nil || len(raw) > testexecution.FunctionalDocumentBytes {
		t.Fatal("finite update bound", err)
	}
	document := &pb.Document{ContentType: "application/json", Data: raw}
	write := &weirclient.WriteRequest{Resource: backend.Index + "/s:hot-0", Document: document}
	writeOptions := weirclient.WriteOptions{StoreName: "search", Request: write}
	updated, err := client.Put(ctx, writeOptions)
	if err := qualificationMutationEvidence(updated, err); err != nil {
		t.Fatal("Update", err)
	}
	expected["hot-0"] = update
	record.Cases = append(record.Cases, "Update")
	// Typed Delete supplies the D in CRUD, independently checked below.
	putJob := qualificationJob{sequence: 1}
	resource := backend.Index + "/s:" + qualificationWriteID(putJob, 0)
	deleteRequest := &weirclient.DeleteRequest{Resource: resource}
	deleteOptions := weirclient.DeleteOptions{StoreName: "search", Request: deleteRequest}
	result, err := client.Delete(ctx, deleteOptions)
	if err := qualificationMutationEvidence(result, err); err != nil {
		t.Fatal("Delete", err)
	}
	delete(expected, qualificationWriteID(putJob, 0))
	record.Cases = append(record.Cases, "Delete")
	if len(expected) > testexecution.FunctionalDocuments {
		t.Fatal("finite document bound")
	}
	verifyQualificationOracle(t, dataset)
	record.Cases = append(record.Cases, "oracle")
	record.Documents = len(expected)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	record.Cases = append(record.Cases, "SDK-close")
	deadline := time.Now().Add(5 * time.Second)
	for {
		metrics := testmetrics.Scrape(t, diagnostics)
		values := qualificationExecutionMemory(t, metrics, 1<<30)
		record.Samples++
		idle := values["budget_rejections_total"] == baseline && testmetrics.Sum(metrics, "weir_memory_latched") == 0 && metrics["weir_memory_cgroup_working_set_bytes"] != nil && testmetrics.Sum(metrics, "weir_memory_cgroup_working_set_bytes") <= .70*(1<<30)
		for _, name := range []string{"used_bytes", "leases", "native_leases", "resident_bytes", "input_reserved_bytes", "input_used_bytes", "wire_bytes", "wire_buffers", "starting", "settled", "borrowed", "waiting", "terminal", "ingress_bytes", "ingress_reserved_bytes", "ingress_buffers", "ingress_messages", "ingress_waiting", "ingress_connections", "ingress_connection_bytes"} {
			idle = idle && values[name] == 0
		}
		for _, name := range []string{"weir_store_pending_entries", "weir_store_pending_reserved_bytes", "weir_store_active_executions", "weir_store_paused_native_outputs", "weir_store_paused_native_reserved_bytes", "weir_store_result_reserved_entries", "weir_store_result_reserved_bytes", "weir_store_retained_result_reserved_bytes", "weir_store_working_reserved_bytes", "weir_store_publishers", "weir_ingress_sessions", "weir_ingress_queued_response_bytes"} {
			idle = idle && metrics[name] != nil && testmetrics.Sum(metrics, name) == 0
		}
		if idle {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("finite final ownership recovery exceeded 5s")
		}
		time.Sleep(25 * time.Millisecond)
	}
	record.Cases = append(record.Cases, "ownership-zero")
	if err := inputs.RecheckCLI(); err != nil {
		t.Fatal(err)
	}
	// Re-run merged source/actual-kernel-driver checks at completion.
	finiteInputs(t)
}

func TestExecutionProfileContractOwnedSearchEndpoint(t *testing.T) {
	owner := "weir-finite-0123456789abcdef0123456789abcdef"
	var creates, deletes atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "GET" && r.URL.Path == "/" {
			_, _ = fmt.Fprintf(w, `{"cluster_name":%q,"version":{"number":"8.19.22","build_flavor":"default","build_hash":"offline-contract"}}`, owner)
			return
		}
		if r.URL.Path != "/weir_finite_0123456789abcdef0123456789abcdef" {
			t.Error("non-owned index request", r.URL.Path)
			w.WriteHeader(400)
			return
		}
		switch r.Method {
		case "PUT":
			creates.Add(1)
		case "DELETE":
			deletes.Add(1)
		default:
			t.Error("unexpected mock oracle method", r.Method)
			w.WriteHeader(400)
			return
		}
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		if creates.Load() != 1 || deletes.Load() != 1 {
			t.Error("owned constructor did not create/clean exactly one index")
		}
	})
	opts := testsearch.OwnedEndpoint{URL: server.URL, Owner: owner}
	backend := testsearch.OpenOwned(t, opts)
	if backend.URL != server.URL || backend.Profile != "elasticsearch-8.19.22" || backend.Username != "" || backend.CAFile != "" {
		t.Fatal("owned endpoint lost its finite identity")
	}
}

func TestExecutionProfileContractFiniteEndpoints(t *testing.T) {
	for _, address := range []string{"localhost:1", "192.0.2.1:1", "127.0.0.1:0", "127.0.0.1:65536", "http://127.0.0.1:1"} {
		if finiteLoopback(address) == nil {
			t.Fatal("accepted remote/malformed endpoint", address)
		}
	}
	if err := finiteLoopback("127.0.0.1:19200"); err != nil {
		t.Fatal(err)
	}
}
