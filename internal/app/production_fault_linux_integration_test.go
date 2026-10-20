//go:build integration

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
)

// These three fixed messages belong to one fresh controller directory and driver.
// Docker ownership, actual controls, signal and wait remain controller-only.
type nativeFaultMessage struct {
	Owner      string `json:"owner"`
	DriverPID  int    `json:"driver_pid"`
	Mongo      string `json:"mongo"`
	Search     string `json:"search"`
	PID        int    `json:"pid"`
	StartTicks string `json:"start_ticks"`
	Exit       int    `json:"exit"`
	DeadlineNS int64  `json:"deadline_ns"`
}

func nativeFaultPublish(t *testing.T, name string, message nativeFaultMessage) {
	t.Helper()
	path := filepath.Join(os.Getenv("WEIR_NATIVE_FAULT_CONTROL"), name)
	raw, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".tmp", raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
}

func nativeFaultWait(t *testing.T, name string, deadline time.Time) nativeFaultMessage {
	t.Helper()
	path := filepath.Join(os.Getenv("WEIR_NATIVE_FAULT_CONTROL"), name)
	for time.Now().Before(deadline) {
		file, err := os.Open(path)
		if os.IsNotExist(err) {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		decoder := json.NewDecoder(io.LimitReader(file, 4097))
		decoder.DisallowUnknownFields()
		var message nativeFaultMessage
		err = decoder.Decode(&message)
		var extra any
		tail := decoder.Decode(&extra)
		closeErr := file.Close()
		if err != nil || tail != io.EOF || closeErr != nil || message.Owner != os.Getenv("WEIR_NATIVE_LOAD_OWNER") || message.DriverPID != os.Getpid() || time.Now().After(deadline) {
			t.Fatal("invalid/late owned fault handshake", err, tail, closeErr)
		}
		return message
	}
	t.Fatal("owned fault handshake deadline", name)
	var zero nativeFaultMessage
	return zero
}

func nativeFaultProxyURI(t *testing.T, proxy *testmongo.Proxy) string {
	t.Helper()
	parsed, err := url.Parse(proxy.URI())
	if err != nil {
		t.Fatal(err)
	}
	if err := finiteLoopback(parsed.Host); err != nil {
		t.Fatal(err)
	}
	// Upstream replica identity was independently checked. The single-member
	// fault route must stay direct, without an incompatible replica-set option.
	query := parsed.Query()
	query.Del("replicaSet")
	query.Set("directConnection", "true")
	query.Set("serverMonitoringMode", "poll")
	parsed.RawQuery = query.Encode()
	uri := parsed.String()
	return uri
}

func TestExecutionProfileContractNativeFaultProxyURI(t *testing.T) {
	// No database is started or contacted: StartProxy owns only its listener.
	fixture := &testmongo.Fixture{URI: "mongodb://127.0.0.1:1/?replicaSet=owned_set"}
	proxy := testmongo.StartProxy(t, fixture)
	uri := nativeFaultProxyURI(t, proxy)
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	original, err := url.Parse(proxy.URI())
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	if parsed.Host != original.Host || len(query) != 2 || query.Get("replicaSet") != "" || query.Get("directConnection") != "true" || query.Get("serverMonitoringMode") != "poll" {
		t.Fatal("fault route escaped its actual proxy or retained replica-set discovery", uri)
	}
	if err := mongodb.ValidateURI(uri); err != nil {
		t.Fatal("actual generated fault URI rejected by production parser", err)
	}
	connection := &Mongo{URI: uri}
	local := &Local{Backend: BackendConfig{MongoDB: connection}}
	store := StoreConfig{Name: "mongo", Local: local}
	cfg := DefaultConfig()
	cfg.Routing.Stores = []StoreConfig{store}
	cfg.Routing = cfg.Routing.resolved()
	if err := cfg.Routing.validate(); err != nil {
		t.Fatal("actual generated fault route rejected by startup configuration", err)
	}
}

func TestProductionLinuxFaultPreflight(t *testing.T) {
	inputs := nativeLoadInputs(t)
	if !inputs.Profile().NativePlainFault() {
		t.Fatal("exact native plain fault profile required")
	}
	record := nativeLoadBase(inputs, "preflight")
	record.Cases = []string{"source", "cli", "kernel-driver"}
	nativeLoadRecord(t, record)
}

func TestProductionLinuxFaultMongo(t *testing.T) {
	inputs := nativeLoadInputs(t)
	if !inputs.Profile().NativePlainFault() {
		t.Fatal("exact native plain fault profile required")
	}
	nativeLoadMongo(t, true)
	record := nativeLoadBase(inputs, "mongo")
	record.Cases = []string{"replica-init", "version", "advertised-loopback"}
	nativeLoadRecord(t, record)
}

func TestProductionLinuxFault(t *testing.T) {
	inputs := nativeLoadInputs(t)
	if !inputs.Profile().NativePlainFault() {
		t.Fatal("exact native plain fault profile required")
	}
	record := nativeLoadBase(inputs, "fault")
	// Completion follows proxy/socket, SDK and independent owned data cleanup.
	t.Cleanup(func() { nativeLoadRecord(t, record) })
	fixture := nativeLoadMongo(t, false)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := fixture.Admin.Database(fixture.DB).Drop(ctx); err != nil {
			t.Error(err)
		}
	})
	owned := testsearch.OwnedEndpoint{URL: os.Getenv("WEIR_NATIVE_LOAD_ORACLE"), Owner: os.Getenv("WEIR_NATIVE_LOAD_OWNER"), Product: "elasticsearch"}
	search := testsearch.OpenOwned(t, owned)
	mongoProxy := testmongo.StartProxy(t, fixture)
	searchProxy := startProcessFaultHTTPProxyOn(t, search.URL, "127.0.0.1:0")
	proxyURI := nativeFaultProxyURI(t, mongoProxy)
	message := nativeFaultMessage{Owner: owned.Owner, DriverPID: os.Getpid(), Mongo: proxyURI, Search: "http://" + searchProxy.listener.Addr().String()}
	nativeFaultPublish(t, "proxies.json", message)
	running := nativeFaultWait(t, "running.json", time.Now().Add(35*time.Second))
	for _, address := range []string{os.Getenv("WEIR_NATIVE_LOAD_APP"), os.Getenv("WEIR_NATIVE_LOAD_DIAGNOSTICS")} {
		if err := finiteLoopback(address); err != nil {
			t.Fatal(err)
		}
	}
	process := &memoryRecoveryCLI{address: os.Getenv("WEIR_NATIVE_LOAD_APP"), diagnostic: os.Getenv("WEIR_NATIVE_LOAD_DIAGNOSTICS"), capacity: 512 << 20, automatic: true, stores: 2}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	options := weirclient.OpenOptions{Seed: process.address, Stores: []string{"mongo", "search"}}
	client, err := weirclient.Open(ctx, options)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	pidCheck := func(t *testing.T) string {
		t.Helper()
		raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", running.PID))
		if err != nil {
			t.Fatal(err)
		}
		fields := strings.Fields(string(raw)[strings.LastIndexByte(string(raw), ')')+2:])
		if running.PID <= 0 || len(fields) < 20 || fields[19] != running.StartTicks {
			t.Fatal("actual CLI process identity changed")
		}
		return strconv.Itoa(running.PID)
	}
	prepareShutdown := func(t *testing.T) {
		t.Helper()
		nativeFaultPublish(t, "observe.json", message)
		nativeFaultWait(t, "observed.json", time.Now().Add(8*time.Second))
	}
	shutdown := func(t *testing.T, gate *processFaultGate) time.Time {
		t.Helper()
		requested := time.Now()
		nativeFaultPublish(t, "shutdown.json", message)
		signaled := nativeFaultWait(t, "signaled.json", requested.Add(8*time.Second))
		received := time.Now()
		remaining := time.Unix(0, signaled.DeadlineNS).Sub(received)
		// Project the controller's host-clock boundary onto this process's
		// monotonic clock once; later clock adjustments cannot renew the budget.
		deadline := received.Add(remaining)
		start := deadline.Add(-8 * time.Second)
		if start.Before(requested) || start.After(received) || remaining <= 0 {
			t.Fatal("invalid actual SIGTERM deadline")
		}
		gate.close()
		result := nativeFaultWait(t, "stopped.json", deadline)
		if result.Exit != 0 || result.DeadlineNS != signaled.DeadlineNS {
			t.Fatal("owned fault CLI exit/deadline mismatch", result.Exit)
		}
		return start
	}
	suite := processFaultSuite{process: process, client: client, pid: pidCheck(t), nativePID: pidCheck, nativePrepareShutdown: prepareShutdown, nativeShutdown: shutdown}
	backends := []processFaultBackend{
		{store: "mongo", root: fixture.DB + "/records", media: "application/bson", mongo: fixture, mongoProxy: mongoProxy},
		{store: "search", root: search.Index, media: "application/json", search: search, searchProxy: searchProxy},
	}
	for _, backend := range backends {
		if !t.Run(backend.store, func(t *testing.T) {
			processFaultHealthy(t, &suite, backend)
			processFaultLostAck(t, &suite, backend, false)
			processFaultLostAck(t, &suite, backend, true)
			processFaultCancelAfterAck(t, &suite, backend)
			processFaultSlowIO(t, &suite, backend)
			processFaultUnavailable(t, &suite, backend)
		}) {
			return
		}
	}
	processFaultShutdown(t, &suite, backends[0])
	record.Cases = []string{"Resolve", "healthy", "lost-ack-native", "lost-ack-typed", "cancel-after-ack", "slow-IO", "unavailable-recovery", "ownership-zero", "SIGTERM", "oracle", "no-replay"}
	record.Documents = 1
	if err := inputs.RecheckCLI(); err != nil {
		t.Fatal(err)
	}
}
