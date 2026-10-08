//go:build integration

package app

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Reuses packaged fixture ownership and explicit archive/image identities.
// The independently mounted probe is never part of the product image.
func TestPackagedSearchArtifacts(t *testing.T) {
	if os.Getenv("WEIR_PACKAGED_SEARCH_INTEGRATION") != "1" {
		t.Skip("requires explicit owned packaged Search Search artifact fixture")
	}
	if runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" {
		t.Fatal("requires native Darwin arm64 and Linux arm64")
	}
	binary, image := os.Getenv("WEIR_PACKAGED_BINARY"), os.Getenv("WEIR_PACKAGED_IMAGE")
	source, helper := os.Getenv("WEIR_PACKAGED_SOURCE"), os.Getenv("WEIR_PACKAGED_HELPER")
	if !filepath.IsAbs(binary) || !filepath.IsAbs(helper) || !strings.HasPrefix(image, "sha256:") || len(source) != 40 {
		t.Fatal("explicit artifact identities required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	raw, err := exec.CommandContext(ctx, binary, "version").Output()
	cancel()
	var version map[string]string
	if err != nil || json.Unmarshal(raw, &version) != nil || version["revision"] != source || version["state"] != "clean-commit" || version["target"] != "darwin/arm64" {
		t.Fatal("archive identity mismatch")
	}
	identity := packagedDocker(
		t,
		"image",
		"inspect",
		"--format",
		`{{index .Config.Labels "org.opencontainers.image.revision"}} {{.Os}}/{{.Architecture}} {{.Config.User}} {{json .Config.Entrypoint}}`,
		image,
	)
	if strings.TrimSpace(identity) != source+` linux/arm64 65532:65532 ["/weir"]` {
		t.Fatal("image identity mismatch", identity)
	}
	engine := packagedDocker(t, "info", "--format", "{{.OSType}}/{{.Architecture}}/{{.CgroupVersion}}")
	if strings.TrimSpace(engine) != "linux/aarch64/2" {
		t.Fatal("requires native Linux arm64 cgroup-v2")
	}
	t.Log("exact source", source, "image", image, "archive", strings.TrimSpace(string(raw)))
	root, err := os.MkdirTemp(filepath.Join(testutil.Root(t), ".testdata"), "weir-local-artifact-")
	if err != nil {
		t.Fatal(err)
	}
	owner := filepath.Base(root)
	if err := os.WriteFile(filepath.Join(root, "owner"), []byte(owner), 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("artifact owner", root)
	fixture := testsearch.OpenSecure(t)
	for _, platform := range []string{"darwin", "linux"} {
		t.Run(platform, func(t *testing.T) {
			observation := &budgetObservation{}
			proxy := startSearchBudgetProxy(t, fixture, observation)
			cfg := packagedSearchConfig(fixture.Backend)
			cfg.Routing.Stores[0].Local.Backend.Search.URL = "https://" + proxy.listener.Addr().String()
			var process *process
			var address, container string
			if platform == "darwin" {
				process = startProcess(t, binary, cfg)
				address = process.address
			} else {
				directory := filepath.Join(root, platform)
				packagedSearchFiles(t, directory, cfg)
				container = owner + "-" + platform
				opts := packagedContainerOptions{owner: owner, name: container, image: image, directory: directory, helper: helper}
				address = packagedContainer(t, opts)
				probe := packagedDocker(t, "exec", "--env", "WEIR_PACKAGED_PROBE=1", container, "/app.test", "-test.run=^TestPackagedImageProbe$", "-test.v", "-test.timeout=8s")
				if !strings.Contains(probe, "--- PASS: TestPackagedImageProbe") {
					t.Fatal(probe)
				}
				t.Log(probe)
			}
			client := endpointProcessClient(t, address)
			searchClientOperations(t, client, fixture, platform)
			packagedSearchFaults(t, client, fixture, proxy)
			started := time.Now()
			if process != nil {
				process.stop(t)
			} else {
				packagedDocker(t, "kill", "--signal=TERM", container)
				if strings.TrimSpace(packagedDocker(t, "wait", container)) != "0" {
					t.Fatal("image exit")
				}
				state := packagedDocker(t, "inspect", "--format", "{{.State.OOMKilled}} {{.State.Running}} {{.State.ExitCode}}", container)
				if strings.TrimSpace(state) != "false false 0" {
					t.Fatal("image state", state)
				}
			}
			elapsed := time.Since(started)
			if elapsed > 3*time.Second {
				t.Fatal("original SIGTERM/Wait budget exceeded", elapsed)
			}
			budgetWait(t, "packaged Search sockets closed", func() bool {
				n, _ := proxy.sockets()
				return n == 0
			})
			t.Logf("%s exact artifact CRUD/Bulk/Native/Scan/expression/cancel/UNKNOWN; SIGTERM/Wait=%s; proxy=0", platform, elapsed)
		})
	}
	t.Run("linux-startup-rejections", func(t *testing.T) {
		handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
		unrelated := httptest.NewTLSServer(handler)
		defer unrelated.Close()
		for _, negative := range []string{"ca", "hostname", "credentials", "unsupported-product"} {
			t.Run(negative, func(t *testing.T) {
				cfg := packagedSearchConfig(fixture.Backend)
				var identityContacts atomic.Int32
				if negative == "unsupported-product" {
					identity := `{"version":{"number":"99.1.2","distribution":"unsupported-product"},"secret":"response-sentinel"}`
					handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						identityContacts.Add(1)
						if r.URL.Path != "/" {
							t.Error("identity rejection must happen before other server requests")
						}
						_, _ = io.WriteString(w, identity)
					})
					// Only this test fixture's own generated server materials are used.
					certificate, err := tls.LoadX509KeyPair(
						filepath.Join(fixture.Root, "materials", "server.pem"),
						filepath.Join(fixture.Root, "materials", "server.key"),
					)
					if err != nil {
						t.Fatal("owned identity server certificate unavailable")
					}
					identityServer := httptest.NewUnstartedServer(handler)
					identityServer.TLS = &tls.Config{
						Certificates: []tls.Certificate{certificate},
						MinVersion:   tls.VersionTLS12,
					}
					identityServer.StartTLS()
					t.Cleanup(identityServer.Close)
					cfg.Routing.Stores[0].Local.Backend.Search.URL = identityServer.URL
				}
				directory := filepath.Join(root, "negative-"+negative)
				packagedSearchFiles(t, directory, cfg)
				filename := filepath.Join(directory, "node-routing.yaml")
				raw, err := os.ReadFile(filename)
				if err != nil {
					t.Fatal(err)
				}
				switch negative {
				case "ca":
					block := &pem.Block{Type: "CERTIFICATE", Bytes: unrelated.Certificate().Raw}
					if err := os.WriteFile(filepath.Join(directory, "ca.crt"), pem.EncodeToMemory(block), 0644); err != nil {
						t.Fatal(err)
					}
				case "hostname":
					raw = []byte(strings.ReplaceAll(string(raw), "host.docker.internal", "packaged-wrong"))
				case "credentials":
					raw = []byte(strings.ReplaceAll(string(raw), fixture.Backend.Password, "wrong-owned-pair"))
				}
				if err := os.WriteFile(filename, raw, 0644); err != nil {
					t.Fatal(err)
				}
				opts := packagedContainerOptions{
					owner:     owner,
					name:      owner + "-" + negative,
					image:     image,
					directory: directory,
					helper:    helper,
					negative:  true,
				}
				packagedContainer(t, opts)
				if strings.TrimSpace(packagedDocker(t, "wait", opts.name)) != "1" {
					t.Fatal("invalid connection/server identity reached serving")
				}
				logs := packagedDocker(t, "logs", opts.name)
				if strings.Contains(logs, "Weir listening") || strings.Contains(logs, fixture.Backend.Password) || strings.Contains(logs, "sentinel") {
					t.Fatal("invalid startup served or disclosed credentials")
				}
				if negative == "unsupported-product" && identityContacts.Load() == 0 {
					t.Fatal("server identity rejection did not reach the owned identity response")
				}
				if negative == "unsupported-product" && !strings.Contains(logs, "unsupported Search server product") {
					t.Fatal("unsupported server identity must preserve its redacted rejection reason", logs)
				}
				t.Log("exact image rejected", negative, "before serving")
			})
		}
	})
}

func packagedSearchConfig(b *testsearch.Backend) Config {
	credentials := Credentials{Username: b.Username, Password: b.Password}
	connection := &credentials
	trust := &BackendTLS{CAFile: b.CAFile}
	backend := &Search{URL: b.URL}
	local := &Local{Backend: BackendConfig{Search: backend, Authentication: connection, TLS: trust}, Batching: BatchingConfig{MaxOperations: 1}}
	service := StoreConfig{Name: "search", Local: local}

	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application, cfg.Basic.Diagnostics.Address = "127.0.0.1:0", "127.0.0.1:0"
	cfg.Routing.Stores = []StoreConfig{service}
	return cfg
}

func packagedSearchFiles(t *testing.T, directory string, cfg Config) {
	t.Helper()
	if err := os.Mkdir(directory, 0755); err != nil {
		t.Fatal(err)
	}
	backend := *cfg.Routing.Stores[0].Local.Backend.Search
	connection := *cfg.Routing.Stores[0].Local.Backend.TLS
	// Only the CA generated by this running owner is consumed here.
	ca, err := os.ReadFile(connection.CAFile)
	if err != nil {
		t.Fatal("owned Search CA unavailable")
	}
	if err := os.WriteFile(filepath.Join(directory, "ca.crt"), ca, 0644); err != nil {
		t.Fatal(err)
	}
	endpoint, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	endpoint.Host = net.JoinHostPort("host.docker.internal", endpoint.Port())
	backend.URL, connection.CAFile = endpoint.String(), "/fixture/ca.crt"
	local := *cfg.Routing.Stores[0].Local
	local.Backend.Search = &backend
	local.Backend.TLS = &connection
	service := StoreConfig{Name: cfg.Routing.Stores[0].Name, Local: &local}
	cfg.Routing.Stores = []StoreConfig{service}
	cfg.Basic.Listeners.Application, cfg.Basic.Diagnostics.Address = "0.0.0.0:7447", "127.0.0.1:7449"
	cfg.Basic.Discovery.Advertise = []string{"127.0.0.1:7447"}
	writeConfigFiles(t, filepath.Join(directory, "node.yaml"), cfg, 0644)
}

func packagedSearchFaults(t *testing.T, client pb.StoreServiceClient, f *testsearch.SecureFixture, proxy *searchBudgetProxy) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	root := f.Backend.Index
	for _, mode := range []string{"ordinary", "native"} {
		id := fmt.Sprintf("lost-%s-%d", mode, time.Now().UnixNano())
		before, applied, dropped := proxy.mutations.Load(), proxy.applied.Load(), proxy.dropped.Load()
		proxy.dropNext.Store(true)
		if mode == "ordinary" {
			request := searchBudgetPut(root, id)
			recordResult, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("search", request))
			result := recordResult.GetMutationResult()
			if err != nil || result.GetOutcome() != pb.MutationOutcome_UNKNOWN {
				t.Fatal("acknowledged mutation response lost must be UNKNOWN", result, err)
			}
		} else {
			body := []byte("{\"index\":{\"_id\":\"" + id + "\"}}\n{\"n\":1}\n")
			httpRequest, err := http.NewRequest(http.MethodPost, "http://ignored.invalid/_bulk", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			httpRequest.Header.Set("Content-Type", "application/x-ndjson")
			nativeCall, err := weirclient.NewHTTPNativeRequest(root, httpRequest)
			if err != nil {
				t.Fatal(err)
			}
			nativeVariant := &pb.Command_Native{Native: nativeCall}
			call := &pb.Command{Operation: nativeVariant}
			stream, err := testutil.ExecuteEvents(ctx, client, "search", call)
			if err != nil {
				t.Fatal(err)
			}
			for {
				reply, err := stream.Recv()
				if err != nil {
					t.Fatal("missing Native terminal", err)
				}
				if end := reply.GetNativeEnd(); end != nil {
					if end.Completion != pb.NativeCompletion_RESPONSE_INCOMPLETE || end.Failure == nil {
						t.Fatal("lost native reply reported complete", end)
					}
					break
				}
			}
			if _, err := stream.Recv(); err != io.EOF {
				t.Fatal("Native EOF", err)
			}
		}
		code, raw := f.Admin.Do(t, "GET", "/"+f.Backend.Index+"/_doc/"+id, "")
		var found struct {
			Version int `json:"_version"`
		}
		if code != 200 || json.Unmarshal(raw, &found) != nil || found.Version != 1 {
			t.Fatal("independent backend effect must be exactly one", code)
		}
		time.Sleep(200 * time.Millisecond)
		if proxy.mutations.Load()-before != 1 || proxy.applied.Load()-applied != 1 || proxy.dropped.Load()-dropped != 1 {
			t.Fatal("dispatch/reply-loss accounting mismatch")
		}
		t.Log(mode, "dispatch=1 acknowledged=1 dropped=1 backend version=1; no replay")
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	scan := &pb.ScanRequest{Resource: root}
	variant := &pb.Command_Scan{Scan: scan}
	command := &pb.Command{Operation: variant}
	stream, err := testutil.ExecuteEvents(cancelled, client, "search", command)
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.Canceled {
		t.Fatal("packaged Execute cancellation", err)
	}
}
