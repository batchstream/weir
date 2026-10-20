//go:build integration

package app

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testexecution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const legacyBusyboxIdentity = "linux/arm64 sha256:ad4f11f7c85f87c0a6d6c0fe492ad53f9cdcff6e8c88b881360c70eff701c67b"
const legacySearchImage = "docker.elastic.co/elasticsearch/elasticsearch@sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237"

var fullContainerID = regexp.MustCompile(`^[0-9a-f]{64}$`)

type legacyDesktopExecution struct {
	inputs        *testexecution.ValidatedInputs
	engine, image string
	searchID      string
	bridge        netip.Addr
}

// Called before fixture creation, seed, proxies, listeners, or Docker calls.
func loadProductionExecution(t *testing.T, suite string) *testexecution.ValidatedInputs {
	t.Helper()
	env := make(map[string]string)
	for _, key := range []string{"WEIR_INTEGRATION", "WEIR_SEARCH_INTEGRATION", "WEIR_MONGO_PROFILE", "WEIR_QUALIFICATION_BINARY", "WEIR_QUALIFICATION_SOURCE", "WEIR_QUALIFICATION_SHA256", "WEIR_PRODUCTION_QUALIFICATION_SMOKE", "WEIR_FAULT_EXPLORATORY"} {
		if value, ok := os.LookupEnv(key); ok {
			env[key] = value
		}
	}
	opts := testexecution.LoadOptions{ProfilePath: os.Getenv("WEIR_EXECUTION_PROFILE"), ProvenancePath: os.Getenv("WEIR_PROVENANCE_INPUTS"), Suite: suite, LegacyEnv: env}
	inputs, err := testexecution.Load(opts)
	if err != nil {
		t.Fatal("execution preflight:", err)
	}
	if err := inputs.RequireExecutable(); err != nil {
		t.Fatal(err)
	}
	if err := inputs.CheckHarnessRoot(testutil.Root(t)); err != nil {
		t.Fatal(err)
	}
	// Existing fixtures retain their own explicit opt-in and fixed addresses.
	// Missing old env is not a source of execution/provenance defaults.
	t.Setenv("WEIR_INTEGRATION", "1")
	profile := inputs.Profile()
	product := profile.Backends[len(profile.Backends)-1].Product
	t.Setenv("WEIR_SEARCH_INTEGRATION", product)
	mongoProfile := ""
	if profile.Backends[0].Transport == "tls" {
		mongoProfile = "tls"
	}
	t.Setenv("WEIR_MONGO_PROFILE", mongoProfile)
	return inputs
}
func inspectLegacyDesktop(t *testing.T, inputs *testexecution.ValidatedInputs) *legacyDesktopExecution {
	t.Helper()
	if err := inputs.RequireExecutable(); err != nil {
		t.Fatal(err)
	}
	adapter := &legacyDesktopExecution{inputs: inputs}
	adapter.engine = strings.TrimSpace(packagedDocker(t, "info", "--format", "{{.OSType}}/{{.Architecture}}/{{.CgroupVersion}} kernel={{.KernelVersion}}"))
	adapter.image = strings.TrimSpace(packagedDocker(t, "image", "inspect", "--format", "{{.Os}}/{{.Architecture}} {{.Id}}", "busybox:1.37.0"))
	if !strings.HasPrefix(adapter.engine, "linux/aarch64/2 kernel=") || strings.TrimPrefix(adapter.engine, "linux/aarch64/2 kernel=") == "" || adapter.image != legacyBusyboxIdentity {
		t.Fatal("legacy Desktop requires native Linux arm64 cgroup-v2 and pinned cached image", adapter.engine, adapter.image)
	}
	raw := packagedDocker(t, "inspect", "--type", "container", "--format", `{{.Id}} {{index .Config.Labels "weir.owner"}} {{.Config.Image}} {{(index .NetworkSettings.Networks "bridge").IPAddress}} {{.State.Running}}`, "weir-local-elasticsearch")
	id, bridge, err := validateLegacySearch(raw)
	if err != nil {
		t.Fatal(err)
	}
	adapter.searchID, adapter.bridge = id, bridge
	verifyLegacyMongoProcess(t, inputs.HarnessSource().Root)
	return adapter
}
func validateLegacySearch(raw string) (string, netip.Addr, error) {
	fields := strings.Fields(raw)
	var address netip.Addr
	if len(fields) != 5 || !fullContainerID.MatchString(fields[0]) || fields[1] != "weir-search-local" || fields[2] != legacySearchImage || fields[4] != "true" {
		return "", address, fmt.Errorf("owned pinned Elasticsearch full instance identity mismatch")
	}
	address, err := netip.ParseAddr(fields[3])
	if err != nil || !address.Is4() || !address.IsPrivate() {
		return "", address, fmt.Errorf("owned Elasticsearch requires a private native bridge")
	}
	return fields[0], address, nil
}
func (a *legacyDesktopExecution) mongoURL() string {
	return "mongodb://host.docker.internal:27028/?directConnection=true&serverMonitoringMode=poll"
}
func (a *legacyDesktopExecution) searchURL() string { return "http://" + a.bridge.String() + ":9200" }
func (a *legacyDesktopExecution) proxyHost(address string) string {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return ""
	}
	return net.JoinHostPort("host.docker.internal", port)
}
func (a *legacyDesktopExecution) proxyURL(proxy *processFaultHTTPProxy) string {
	return "http://" + a.proxyHost(proxy.listener.Addr().String())
}
func readLegacyMarker(t *testing.T, path string) string {
	t.Helper()
	actual, err := filepath.EvalSymlinks(path)
	if err != nil || actual != path {
		t.Fatal("legacy fixture marker symlink/missing", path, err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil || len(raw) > 4096 {
		t.Fatal("legacy fixture marker exceeds bound", path, err)
	}
	return strings.TrimSpace(string(raw))
}
func verifyLegacyMongoProcess(t *testing.T, root string) {
	t.Helper()
	data := filepath.Join(root, ".testdata", "mongo-local")
	if readLegacyMarker(t, filepath.Join(data, ".weir-owner")) != "weir-mongo-local mongodb-8.0.32 loopback-27028" {
		t.Fatal("MongoDB owner marker mismatch")
	}
	pid := readLegacyMarker(t, filepath.Join(data, "mongod.pid"))
	number, err := strconv.Atoi(pid)
	if err != nil || number <= 1 || strconv.Itoa(number) != pid {
		t.Fatal("MongoDB PID marker invalid")
	}
	args := []string{filepath.Join(root, ".tools", "mongodb-macos-aarch64--8.0.32", "bin", "mongod"), "--dbpath", data, "--bind_ip", "127.0.0.1", "--port", "27028", "--replSet", "weir_local", "--setParameter", "enableTestCommands=1", "--logpath", filepath.Join(data, "mongod.log"), "--pidfilepath", filepath.Join(data, "mongod.pid"), "--fork"}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(ctx, "ps", "-p", pid, "-o", "args=").Output()
	if err != nil || strings.TrimSpace(string(raw)) != strings.Join(args, " ") {
		t.Fatal("MongoDB actual PID/argv differs from owned loopback replica-set process", err)
	}
	t.Logf("legacy MongoDB owned PID=%s argv=%s", pid, strings.TrimSpace(string(raw)))
}
func verifyLegacyMongoVersion(t *testing.T, fixture *testmongo.Fixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	command := bson.D{{Key: "buildInfo", Value: 1}}
	var result struct {
		Version string `bson:"version"`
	}
	err := fixture.Admin.Database("admin").RunCommand(ctx, command).Decode(&result)
	if err != nil || result.Version != "8.0.32" {
		t.Fatal("owned MongoDB version mismatch", result.Version, err)
	}
	t.Log("actual backend MongoDB=8.0.32; owned replica identity independently checked by fixture hello")
}
func validateProductionCLIOptions(opts memoryRecoveryOptions) error {
	if opts.execution == nil {
		return fmt.Errorf("production CLI requires validated inputs")
	}
	if err := opts.execution.RequireExecutable(); err != nil {
		return err
	}
	profile := opts.execution.Profile()
	if opts.binary != opts.execution.CLIPath() || opts.capacityMiB != profile.CapacityMiB() || !opts.automatic || opts.sharedMemoryMiB != 0 || opts.config == nil {
		return fmt.Errorf("production CLI options conflict with fixed resource policy")
	}
	return opts.execution.RecheckCLI()
}
func validateContainerOwnership(raw, id, owner string) error {
	fields := strings.Fields(raw)
	if !fullContainerID.MatchString(id) || len(fields) < 2 || fields[0] != id || fields[1] != owner {
		return fmt.Errorf("container ID/owner mismatch; refuse stop/remove and preserve evidence: %q", raw)
	}
	return nil
}
