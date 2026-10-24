//go:build integration

package app

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	dto "github.com/prometheus/client_model/go"
)

// cliContainerOptions runs binary in a container limited to capacityMiB with
// config as its node configuration.
type cliContainerOptions struct {
	binary      string
	capacityMiB int
	config      Config
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
	raw := runDocker(t, "inspect", "--type", "container", "--format", `{{index .Config.Labels "weir.owner"}} {{(index .NetworkSettings.Networks "bridge").IPAddress}} {{.State.Running}}`, name)
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

// runDocker runs one bounded Docker CLI command and returns its combined
// output.
func runDocker(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("Docker %s failed: %v %s", args[0], err, output)
	}
	return string(output)
}

// cliContainer is a running CLI container. name is the full container ID;
// owner is the container name and its weir.owner label.
type cliContainer struct {
	name, owner, address, diagnostic string
	capacity                         float64
	stores                           int
}

// startCLIContainer starts the CLI in a memory- and CPU-limited busybox
// container, publishes its application listener on a loopback host address
// and waits for readiness. Cleanup stops and removes only the container
// it created.
func startCLIContainer(t *testing.T, opts cliContainerOptions) *cliContainer {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0755); err != nil {
		t.Fatal(err)
	}
	owner := fmt.Sprintf("weir-cli-%d-%d", os.Getpid(), time.Now().UnixNano())
	cfg := opts.config
	cfg.Listeners.Application = "0.0.0.0:7447"
	cfg.Listeners.Diagnostics = "0.0.0.0:7449"
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reservation.Close() })
	applicationAddress := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	writeConfigFile(t, filepath.Join(directory, "weir.yaml"), cfg, 0644)
	args := []string{"create", "--name", owner, "--label", "weir.owner=" + owner, "--platform=linux/" + runtime.GOARCH, "--pull=never",
		"--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges", "--user=65532:65532",
		fmt.Sprintf("--memory=%dm", opts.capacityMiB), fmt.Sprintf("--memory-swap=%dm", opts.capacityMiB), "--cpus=2", "--pids-limit=96",
		"--publish", applicationAddress + ":7447", "--publish", "127.0.0.1::7449",
		"--mount", "type=bind,src=" + directory + ",dst=/fixture,readonly", "--mount", "type=bind,src=" + opts.binary + ",dst=/weir,readonly"}
	if runtime.GOOS == "linux" {
		// Docker Engine on Linux maps the host gateway only on request.
		args = append(args, "--add-host=host.docker.internal:host-gateway")
	}
	args = append(args, "busybox:1.37.0", "/weir", "serve", "--config", "/fixture/weir.yaml")
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
	containerID = strings.TrimSpace(runDocker(t, args...))
	if !fullContainerID.MatchString(containerID) {
		t.Fatal("Docker create did not return a full container ID", containerID)
	}
	created := runDocker(t, "inspect", "--type", "container", "--format", `{{.Id}} {{index .Config.Labels "weir.owner"}}`, owner)
	if err := validateContainerOwnership(created, containerID, owner); err != nil {
		t.Fatal(err)
	}
	runDocker(t, "start", containerID)
	service := &cliContainer{name: containerID, owner: owner, capacity: float64(opts.capacityMiB << 20), stores: len(cfg.Stores)}
	service.address = strings.TrimSpace(runDocker(t, "port", owner, "7447/tcp"))
	if service.address != applicationAddress {
		t.Fatal("Docker published address differs from the reserved application address", service.address, applicationAddress)
	}
	service.diagnostic = strings.TrimSpace(runDocker(t, "port", owner, "7449/tcp"))
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
			t.Fatal("CLI did not become ready", runDocker(t, "logs", owner))
		}
		time.Sleep(20 * time.Millisecond)
	}
	return service
}

// sample scrapes the container's diagnostics and fails unless the CLI set its
// Go memory limit to 90% of the container's.
func (service *cliContainer) sample(t *testing.T) map[string]*dto.MetricFamily {
	t.Helper()
	metrics := testmetrics.Scrape(t, service.diagnostic)
	limit := float64(uint64(service.capacity) / 10 * 9)
	if testmetrics.Sum(metrics, "weir_memory_limit_bytes") != limit {
		t.Fatal("CLI did not derive its Go memory limit from the container")
	}
	return metrics
}

// storeActive counts running record batches and Native commands.
func storeActive(metrics map[string]*dto.MetricFamily) float64 {
	active := testmetrics.Sum(metrics, "weir_store_active_executions") + testmetrics.Sum(metrics, "weir_store_active_natives")
	return active
}
