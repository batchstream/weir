//go:build integration

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Explicit opt-in: the supplied binary is extracted from the delivered archive;
// the supplied image is loaded from the delivered OCI content, not rebuilt here.
func TestPackagedArtifacts(t *testing.T) {
	if os.Getenv("WEIR_M15_INTEGRATION") != "1" {
		t.Skip("requires explicit owned packaged-artifact fixture")
	}
	if runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" {
		t.Fatal("this fixture qualifies Darwin arm64 + native Linux arm64 only")
	}
	binary, image := os.Getenv("WEIR_M15_BINARY"), os.Getenv("WEIR_M15_IMAGE")
	source, helper := os.Getenv("WEIR_M15_SOURCE"), os.Getenv("WEIR_M15_HELPER")
	if !filepath.IsAbs(binary) || !filepath.IsAbs(helper) || !strings.HasPrefix(image, "sha256:") || len(source) != 40 {
		t.Fatal("explicit artifact identities required")
	}
	root := filepath.Join(testutil.Root(t), ".testdata", fmt.Sprintf("weir-m15-artifact-%d", os.Getpid()))
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	owner := filepath.Base(root)
	if err := os.WriteFile(filepath.Join(root, "owner"), []byte(owner), 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("owned artifact fixture", root)
	engine := packagedDocker(t, "info", "--format", "{{.OSType}}/{{.Architecture}}/{{.CgroupVersion}}")
	if strings.TrimSpace(engine) != "linux/aarch64/2" {
		t.Fatal("requires native arm64 cgroup-v2 engine", engine)
	}
	identity := packagedDocker(t, "image", "inspect", "--format", "{{.Os}}/{{.Architecture}} {{.Config.User}} {{json .Config.Entrypoint}}", image)
	if strings.TrimSpace(identity) != `linux/arm64 65532:65532 ["/weir"]` {
		t.Fatal("image identity", identity)
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
	if version["revision"] != source || version["state"] != "clean-commit" || version["dirty"] != "false" || version["target"] != "darwin/arm64" {
		t.Fatal(version)
	}
	t.Log("archive version", strings.TrimSpace(string(output)))
	// Cleanup of each subtest completes before the next starts; fixture Close owns Mongo.
	for _, platform := range []string{"darwin", "linux"} {
		rounds := 1
		if platform == "linux" {
			rounds = 3
		}
		for round := 0; round < rounds; round++ {
			t.Run(fmt.Sprintf("%s-%d", platform, round), func(t *testing.T) {
				fixture := testmongo.OpenSecure(t)
				config := packagedConfig(t, fixture.URI)
				address := ""
				var process *process
				container := ""
				if platform == "darwin" {
					process = startProcess(t, binary, config)
					address = process.address
				} else {
					container = fmt.Sprintf("%s-%s-%d", owner, platform, round)
					directory := filepath.Join(root, fmt.Sprintf("linux-%d", round))
					if err := os.Mkdir(directory, 0755); err != nil {
						t.Fatal(err)
					}
					packagedFiles(t, directory, fixture.URI)
					containerOpts := packagedContainerOptions{owner: owner, name: container, image: image, directory: directory, helper: helper}
					address = packagedContainer(t, containerOpts)
					probe := packagedDocker(t, "exec", "--env", "WEIR_M15_PROBE=1", container, "/app.test", "-test.run=^TestPackagedImageProbe$", "-test.v", "-test.timeout=8s")
					if !strings.Contains(probe, "--- PASS: TestPackagedImageProbe") {
						t.Fatal(probe)
					}
					t.Log(probe)
				}
				client := endpointProcessClient(t, address)
				packagedCalls(t, client, fixture)
				started := time.Now()
				if process != nil {
					process.stop(t)
				} else {
					packagedDocker(t, "kill", "--signal=TERM", container)
					exit := packagedDocker(t, "wait", container)
					if strings.TrimSpace(exit) != "0" {
						t.Fatal("image exit", exit)
					}
					state := packagedDocker(t, "inspect", "--format", "{{.State.OOMKilled}} {{.State.Running}} {{.State.ExitCode}}", container)
					if strings.TrimSpace(state) != "false false 0" {
						t.Fatal(state)
					}
				}
				if time.Since(started) > 3*time.Second {
					t.Fatal("fixed SIGTERM/Wait bound exceeded")
				}
				t.Logf("%s actual artifact Read/Mutate/Bulk, cancellation, SIGTERM/Wait=%s", platform, time.Since(started))
			})
		}
	}
	t.Run("linux-verified-tls-rejections", func(t *testing.T) {
		fixture := testmongo.OpenSecure(t)
		for _, negative := range []string{"ca", "hostname"} {
			t.Run(negative, func(t *testing.T) {
				directory := filepath.Join(root, "negative-"+negative)
				if err := os.Mkdir(directory, 0755); err != nil {
					t.Fatal(err)
				}
				uri := fixture.URI
				if negative == "ca" {
					uri = fixture.BadCAURI
				}
				packagedFiles(t, directory, uri)
				if negative == "hostname" {
					filename := filepath.Join(directory, "node-routing.yaml")
					raw, err := os.ReadFile(filename)
					if err != nil {
						t.Fatal(err)
					}
					raw = []byte(strings.ReplaceAll(string(raw), "host.docker.internal", "m15-wrong"))
					if err := os.WriteFile(filename, raw, 0644); err != nil {
						t.Fatal(err)
					}
				}
				name := owner + "-negative-" + negative
				opts := packagedContainerOptions{
					owner:     owner,
					name:      name,
					image:     image,
					directory: directory,
					helper:    helper,
					negative:  true,
				}
				packagedContainer(t, opts)
				exit := packagedDocker(t, "wait", name)
				if strings.TrimSpace(exit) != "1" {
					t.Fatal("bad TLS must fail startup", exit)
				}
				logs := packagedDocker(t, "logs", name)
				if strings.Contains(logs, "Weir listening") {
					t.Fatal("bad TLS reached serving")
				}
				t.Log("image rejected", negative, "before serving; exit=1")
			})
		}
	})
	t.Run("linux-unknown-no-replay", func(t *testing.T) {
		fixture := testmongo.OpenSecure(t)
		proxy := testmongo.StartProxy(t, &fixture.Fixture)
		proxy.DropCommand = "bulkWrite"
		directory := filepath.Join(root, "fault")
		if err := os.Mkdir(directory, 0755); err != nil {
			t.Fatal(err)
		}
		packagedFiles(t, directory, proxy.URI())
		opts := packagedContainerOptions{
			owner:     owner,
			name:      owner + "-fault",
			image:     image,
			directory: directory,
			helper:    helper,
		}
		address := packagedContainer(t, opts)
		client := endpointProcessClient(t, address)
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		request := budgetPut("weir://records/"+fixture.DB+"/records", "lost")
		proxy.DropRemaining.Store(1)
		routedResult196, err := testutil.ExecuteRecord(ctx, client, testutil.RecordCommand(request))
		result := routedResult196.GetMutation()
		if err != nil || result.GetOutcome() != pb.MutationOutcome_UNKNOWN {
			t.Fatal("dropped acknowledged reply must be UNKNOWN", result, err)
		}
		filter := bson.D{{Key: "_id", Value: "lost"}}
		count, err := fixture.Admin.Database(fixture.DB).Collection("records").CountDocuments(ctx, filter)
		if err != nil || count != 1 {
			t.Fatal("real backend effect", count, err)
		}
		time.Sleep(200 * time.Millisecond)
		updates := 0
		for _, event := range proxy.Events() {
			if event.Command == "bulkWrite" {
				updates++
				if !event.Dropped || !event.Acknowledged {
					t.Fatal("fault evidence", event.Command)
				}
			}
		}
		if updates != 1 || proxy.DropRemaining.Load() != 0 {
			t.Fatal("mutation replay", updates)
		}
		packagedDocker(t, "kill", "--signal=TERM", opts.name)
		if strings.TrimSpace(packagedDocker(t, "wait", opts.name)) != "0" {
			t.Fatal("fault process close")
		}
		budgetWait(t, "proxy closed", func() bool {
			current, _ := proxy.Sockets()
			return current == 0
		})
		t.Log("one acknowledged real backend update; reply dropped; UNKNOWN; no replay; proxy sockets=0")
	})
}

func packagedConfig(t *testing.T, uri string) Config {
	backend := mongoFixtureConfig(t, uri)
	local := &Local{MongoDB: backend, MaxConcurrency: 2, MaxBatchOperations: 1}
	service := StoreConfig{Name: "records", Local: local}

	config := DefaultConfig()
	config.Basic.Listeners.Application = "127.0.0.1:0"
	config.Basic.Diagnostics.Address = "127.0.0.1:0"
	config.Routing.Stores = []StoreConfig{service}

	return config
}

func packagedFiles(t *testing.T, directory, uri string) {
	t.Helper()
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatal("owned URI")
	}
	query := parsed.Query()
	// This reads only a CA generated by this very test, never existing user material.
	ca, err := os.ReadFile(query.Get("tlsCAFile"))
	if err != nil {
		t.Fatal("owned CA")
	}
	if err := os.WriteFile(filepath.Join(directory, "ca.crt"), ca, 0644); err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Host = net.JoinHostPort("host.docker.internal", port)
	query.Set("tlsCAFile", "/fixture/ca.crt")
	parsed.RawQuery = query.Encode()
	config := packagedConfig(t, parsed.String())
	config.Basic.Listeners.Application = "0.0.0.0:7447"
	config.Basic.Discovery.Advertise = []string{"127.0.0.1:7447"}
	config.Basic.Diagnostics.Address = "127.0.0.1:7449"
	writeConfigFiles(t, filepath.Join(directory, "node.yaml"), config, 0644)
}

type packagedContainerOptions struct {
	owner, name, image, directory, helper string
	negative                              bool
}

func packagedContainer(t *testing.T, opts packagedContainerOptions) string {
	t.Helper()
	// Register cleanup before create; partial daemon successes remain discoverable.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		check := exec.CommandContext(ctx, "docker", "inspect", "--format", `{{index .Config.Labels "weir.owner"}} {{.State.Running}}`, opts.name)
		raw, err := check.CombinedOutput()
		if err != nil {
			t.Errorf("cleanup inspect %s: %v; preserving materials", opts.name, err)
			return
		}
		if !strings.HasPrefix(string(raw), opts.owner+" ") {
			t.Error("cleanup owner mismatch")
			return
		}
		logs, logErr := exec.CommandContext(ctx, "docker", "logs", opts.name).CombinedOutput()
		if logErr != nil {
			t.Error("cleanup log failure", logErr)
		} else if err := os.WriteFile(opts.directory+".log", logs, 0600); err != nil {
			t.Error(err)
		}
		_, err = exec.CommandContext(ctx, "docker", "stop", "--timeout=5", opts.name).CombinedOutput()
		if err != nil {
			t.Error("cleanup stop", err)
		}
		raw, err = exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.State.Running}}", opts.name).Output()
		if err != nil || strings.TrimSpace(string(raw)) != "false" {
			t.Error("not confirmed stopped; preserving materials")
			return
		}
		_, err = exec.CommandContext(ctx, "docker", "rm", "-v", opts.name).CombinedOutput()
		if err != nil {
			t.Error("cleanup rm", err)
			return
		}
		if err := os.RemoveAll(opts.directory); err != nil {
			t.Error(err)
		}
		t.Log("owned container removed and temporary config/CA removed", opts.name)
	})
	args := []string{"create", "--name", opts.name, "--label", "weir.owner=" + opts.owner, "--platform=linux/arm64",
		"--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges", "--memory=512m", "--memory-swap=512m", "--cpus=2", "--pids-limit=96",
		"--add-host=m15-wrong:host-gateway", "--publish", "127.0.0.1::7447", "--mount", "type=bind,src=" + opts.directory + ",dst=/fixture,readonly",
		"--mount", "type=bind,src=" + opts.helper + ",dst=/app.test,readonly", opts.image, "serve", "--config", "/fixture/node.yaml", "--routes", "/fixture/node-routing.yaml"}
	packagedDocker(t, args...)
	packagedDocker(t, "start", opts.name)
	if opts.negative {
		return ""
	}
	until := time.Now().Add(7 * time.Second)
	for {
		logs := packagedDocker(t, "logs", opts.name)
		if strings.Contains(logs, "Diagnostics listening") {
			break
		}
		if time.Now().After(until) {
			t.Fatal("image startup failed; sanitized log retained")
		}
		time.Sleep(50 * time.Millisecond)
	}
	address := strings.TrimSpace(packagedDocker(t, "port", opts.name, "7447/tcp"))
	return address
}

func packagedDocker(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("Docker %s failed: %v %s", args[0], err, output)
	}
	return string(output)
}

func packagedCalls(t *testing.T, client pb.StoreServiceClient, fixture *testmongo.SecureFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	request := budgetPut("weir://records/"+fixture.DB+"/records", "artifact")
	for mode := 0; mode < 3; mode++ {
		if !budgetLoadCall(ctx, client, request, mode) {
			t.Fatal("packaged Read/Mutate/Bulk", mode)
		}
	}
	read := &pb.ReadRequest{Resource: request.Resource}
	routedResult363, err := testutil.ExecuteRecord(ctx, client, testutil.RecordCommand(read))
	result := routedResult363.GetRead()
	if err != nil || result.GetDocument() == nil {
		t.Fatal("packaged readback", err)
	}
	invalid := &pb.MutateRequest{Resource: "weir://missing/db/records/s:artifact", Action: request.Action}
	response, err := testutil.ExecuteRecord(ctx, client, testutil.RecordCommand(invalid))
	if status.Code(err) != codes.Unavailable || response != nil {
		t.Fatal("unhosted Store request was not rejected", response, err)
	}
	cancelled, stop := context.WithCancel(ctx)
	cancelRequest := budgetPut("weir://records/"+fixture.DB+"/records", "cancelled-artifact")
	fixtureRequest := testutil.RecordCommand(cancelRequest)
	batch := &pb.MutateBatchRequest{StoreName: "records", Requests: []*pb.MutateRequest{fixtureRequest.Operation.GetMutate()}}
	stop()
	cancelResponse, err := client.Mutate(cancelled, batch)
	if status.Code(err) != codes.Canceled || cancelResponse != nil {
		t.Fatal("batch cancellation", cancelResponse, err)
	}
	filter := bson.D{{Key: "_id", Value: "artifact"}}
	count, err := fixture.Admin.Database(fixture.DB).Collection("records").CountDocuments(ctx, filter)
	if err != nil || count != 1 {
		t.Fatal("independent backend read", count, err)
	}
	filter = bson.D{{Key: "_id", Value: "cancelled-artifact"}}
	count, err = fixture.Admin.Database(fixture.DB).Collection("records").CountDocuments(ctx, filter)
	if err != nil || count != 0 {
		t.Fatal("cancelled batch reached database", count, err)
	}
}

// Runs only via docker exec, as an independent bind-mounted helper. The image
// still contains only /weir and the pinned distroless runtime contents.
func TestPackagedImageProbe(t *testing.T) {
	if os.Getenv("WEIR_M15_PROBE") != "1" {
		t.Skip("owned image probe only")
	}
	if runtime.GOOS != "linux" || runtime.GOARCH != "arm64" || os.Geteuid() != 65532 {
		t.Fatal("not native nonroot Linux arm64")
	}
	status, err := os.ReadFile("/proc/1/status")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(status), "Uid:\t65532\t65532\t65532\t65532") ||
		!strings.Contains(string(status), "NoNewPrivs:\t1") ||
		!strings.Contains(string(status), "CapEff:\t0000000000000000") {
		t.Fatal("PID1 privileges")
	}
	cmd, err := os.ReadFile("/proc/1/cmdline")
	if err != nil || !strings.HasPrefix(string(cmd), "/weir\x00serve\x00--config\x00") {
		t.Fatal("PID1 is not exec Weir")
	}
	if err := os.WriteFile("/weir-m15-readonly-check", []byte("fixture"), 0600); err == nil {
		t.Fatal("root filesystem writable")
	}
	limit, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err != nil || strings.TrimSpace(string(limit)) != "536870912" {
		t.Fatal("finite cgroup", err)
	}
	metrics := testmetrics.Scrape(t, "127.0.0.1:7449")
	if testmetrics.Sum(metrics, "weir_memory_unknown") != 0 ||
		testmetrics.Sum(metrics, "weir_memory_cgroup_valid") != 1 ||
		testmetrics.Sum(metrics, "weir_memory_cgroup_limit_bytes") != 512<<20 ||
		testmetrics.Sum(metrics, "weir_memory_latched") != 0 {
		t.Fatal("nonroot readonly Linux memory observation")
	}
	rss := testmetrics.Sample(metrics, "weir_memory_sample_bytes", map[string]string{"source": "linux_rss"}).GetGauge().GetValue()
	if rss <= 0 {
		t.Fatal("RSS absent")
	}
	t.Logf(
		"PID1 /weir UID=65532 CapEff=0 NoNewPrivs=1 readonly; Linux RSS=%g cgroup current=%g limit=536870912 unknown=0",
		rss,
		testmetrics.Sum(metrics, "weir_memory_cgroup_current_bytes"),
	)
}
