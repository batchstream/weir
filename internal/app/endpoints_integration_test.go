//go:build integration

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testdns"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func buildEndpointProcess(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "weir")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, "./cmd/weir")
	cmd.Dir = testutil.Root(t)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatal(err, string(output))
	}
	return binary
}

func endpointProcessConfig(t *testing.T) Config {
	t.Helper()
	mongoFixture := testmongo.Open(t)
	database := mongoFixture.DB
	search := testsearch.Open(t)
	mongo := &Mongo{URI: mongoFixture.URI, Database: database, Collection: "records"}
	mongoLocal := &Local{Mongo: mongo}
	backend := &Search{URL: search.URL, Index: search.Index, Profile: search.Profile}
	searchLocal := &Local{Search: backend}
	m := Service{Name: "mongo", Local: mongoLocal}
	s := Service{Name: "search", Local: searchLocal}
	mr := Route{Store: "mongo", Service: "mongo"}
	sr := Route{Store: "search", Service: "search"}
	cfg := DefaultConfig()
	cfg.Peer = "127.0.0.1:0"
	cfg.Diagnostics = "127.0.0.1:0"
	cfg.Services = []Service{m, s}
	cfg.Routes = []Route{mr, sr}
	return cfg
}

func endpointProcessClient(t *testing.T, address string) pb.WeirClient {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0), grpc.WaitForReady(false)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewWeirClient(conn)
}

func TestEndpointIndependentProcessesDistributionReplacement(t *testing.T) {
	cfg := endpointProcessConfig(t)
	binary := buildEndpointProcess(t)
	var peers []*process
	var addresses []string
	for range 3 {
		p := startProcess(t, binary, cfg)
		peers = append(peers, p)
		addresses = append(addresses, p.address)
	}
	front := DefaultConfig()
	front.Application = "127.0.0.1:0"
	front.Diagnostics = "127.0.0.1:0"
	front.Routes = cfg.Routes
	for _, kind := range []string{"mongo", "search"} {
		remote := &Remote{Endpoints: addresses, Relays: 4}
		service := Service{Name: kind, Remote: remote}
		front.Services = append(front.Services, service)
	}
	p := startProcess(t, binary, front)
	client := endpointProcessClient(t, p.address)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := 0; i < 80; i++ {
		for _, kind := range []string{"mongo", "search"} {
			root := "weir://mongo/" + cfg.Services[0].Local.Mongo.Database + "/records"
			if kind == "search" {
				root = "weir://search/" + cfg.Services[1].Local.Search.Index
			}
			req := &pb.ReadRequest{Resource: fmt.Sprintf("%s/s:process%d", root, i)}
			result, err := client.Read(ctx, req)
			if err != nil || result.GetMissing() == nil {
				t.Fatal(result, err)
			}
		}
	}
	var total float64
	for _, peer := range peers {
		n := testmetrics.Sum(testmetrics.Scrape(t, peer.diagnostic), "weir_store_records_total")
		if n < 20 {
			t.Fatal("no process distribution", n)
		}
		total += n
		t.Logf("executor PID=%d records=%g", peer.command.Process.Pid, n)
	}
	if total != 160 {
		t.Fatal("duplicated execution", total)
	}
	peers[0].stop(t)
	req := &pb.ReadRequest{Resource: "weir://mongo/" + cfg.Services[0].Local.Mongo.Database + "/records/s:after-stop"}
	for range 20 {
		result, err := client.Read(ctx, req)
		if err != nil || result.GetMissing() == nil {
			t.Fatal("healthy endpoints unavailable after peer SIGTERM", result, err)
		}
	}
	cfg.Peer = addresses[0]
	replacement := startProcess(t, binary, cfg)
	until := time.Now().Add(5 * time.Second)
	for i := 0; ; i++ {
		req.Resource = fmt.Sprintf("weir://mongo/%s/records/s:replacement%d", cfg.Services[0].Local.Mongo.Database, i)
		result, err := client.Read(ctx, req)
		if err != nil || result.GetMissing() == nil {
			t.Fatal(result, err)
		}
		if testmetrics.Sum(testmetrics.Scrape(t, replacement.diagnostic), "weir_store_records_total") > 0 {
			break
		}
		if time.Now().After(until) {
			t.Fatal("recovered endpoint never rejoined selection")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("three independent executors share one owned Mongo DB and Search index; stopped PID=%d, replacement PID=%d received new RPC", peers[0].command.Process.Pid, replacement.command.Process.Pid)
}

// Only this test child accepts the injected standard resolver dependency. The
// shipped CLI has no DNS-server flag or alternate configuration model.
func TestEndpointDNSProcessChild(t *testing.T) {
	raw := os.Getenv("WEIR_M9_DNS_CHILD")
	if raw == "" {
		t.Skip("test subprocess entry")
	}
	var input struct{ Endpoint, DNS string }
	if err := json.Unmarshal([]byte(raw), &input); err != nil {
		t.Fatal(err)
	}
	dialer := net.Dialer{Timeout: time.Second}
	native := &net.Resolver{PreferGo: true, StrictErrors: true, Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, input.DNS)
	}}
	cfg := server.RemoteConfig{Endpoints: []string{input.Endpoint}, Relays: 4, Resolver: native}
	remote, err := server.NewRemote(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	service := server.Service{RemoteWeir: remote}
	limits := server.DefaultLimits()
	admission, err := server.NewAdmission(limits)
	if err != nil {
		t.Fatal(err)
	}
	options := server.Config{Routes: map[string]server.Service{"mongo": service, "search": service}, Limits: limits, Admission: admission, InitialForwards: 4}
	srv, err := server.New(options)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(listener) }()
	<-srv.Serving()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	fmt.Printf("Weir listening on [%s]\n", listener.Addr())
	<-ctx.Done()
	drain, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := srv.Shutdown(drain); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestEndpointDNSAcrossProcesses(t *testing.T) {
	cfg := endpointProcessConfig(t)
	binary := buildEndpointProcess(t)
	first := startProcess(t, binary, cfg)
	_, port, _ := net.SplitHostPort(first.address)
	cfg.Peer = net.JoinHostPort("::1", port)
	second := startProcess(t, binary, cfg)
	dns := testdns.Start(t)
	answer := testdns.Answer{Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}
	dns.Set("executor.weir.test", answer)
	input := struct{ Endpoint, DNS string }{Endpoint: "executor.weir.test:" + port, DNS: dns.Address}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestEndpointDNSProcessChild$")
	command.Env = append(os.Environ(), "WEIR_M9_DNS_CHILD="+string(raw))
	front := watchProcess(t, command, false)
	client := endpointProcessClient(t, front.address)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &pb.ReadRequest{Resource: "weir://mongo/" + cfg.Services[0].Local.Mongo.Database + "/records/s:dns"}
	if result, err := client.Read(ctx, req); err != nil || result.GetMissing() == nil {
		t.Fatal(result, err)
	}
	answer.Addresses = []netip.Addr{netip.MustParseAddr("::1")}
	dns.Set("executor.weir.test", answer)
	first.stop(t)
	var calls int
	for {
		calls++
		result, err := client.Read(ctx, req)
		if err == nil && result.GetMissing() != nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("DNS process replacement did not recover", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if testmetrics.Sum(testmetrics.Scrape(t, second.diagnostic), "weir_store_records_total") != 1 || dns.AAAA.Load() < 2 || dns.Other.Load() != 0 {
		t.Fatal("replacement not proven via DNS/second executor")
	}
	t.Logf("actual loopback DNS A to AAAA, forwarding PID=%d, executor PIDs=%d/%d, distinct new Read probes=%d; no system DNS changes", front.command.Process.Pid, first.command.Process.Pid, second.command.Process.Pid, calls)
}
