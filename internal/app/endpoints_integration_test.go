//go:build integration

package app

import (
	"context"
	"fmt"
	"github.com/batchstream/weir/routeclient"
	"net"
	"net/netip"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
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

func endpointProcessConfig(t *testing.T) (Config, map[string]string) {
	t.Helper()
	mongoFixture := testmongo.Open(t)
	database := mongoFixture.DB
	search := testsearch.Open(t)
	mongo := mongoFixtureConfig(t, mongoFixture.URI)
	mongoLocal := &Local{MongoDB: mongo}
	backend := &Search{URL: search.URL}
	searchLocal := &Local{Search: backend}
	m := StoreConfig{Name: "mongo", Local: mongoLocal}
	s := StoreConfig{Name: "search", Local: searchLocal}

	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application, cfg.Basic.Listeners.Peer = "127.0.0.1:0", "127.0.0.1:0"
	cfg.Basic.Diagnostics.Address = "127.0.0.1:0"
	cfg.Routing.Stores = []StoreConfig{m, s}

	roots := map[string]string{
		"mongo":  "weir://mongo/" + database + "/records",
		"search": "weir://search/" + search.Index,
	}
	return cfg, roots
}

func endpointProcessClient(t *testing.T, address string) pb.WeirClient {
	t.Helper()
	conn, err := grpc.NewClient(
		"passthrough:///"+address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithNoProxy(),
		grpc.WithDisableRetry(),
		grpc.WithDisableServiceConfig(),
		grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0), grpc.WaitForReady(false)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewWeirClient(conn)
}

func openDiscoveredClient(t *testing.T, seed string, stores []string) *routeclient.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	options := routeclient.OpenOptions{Seed: seed, Stores: stores, RefreshInterval: 100 * time.Millisecond}
	for {
		client, err := routeclient.Open(ctx, options)
		if err == nil {
			t.Cleanup(func() { _ = client.Close() })
			return client
		}
		if ctx.Err() != nil {
			t.Fatal("Store discovery did not converge", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestEndpointIndependentProcessesDistributionReplacement(t *testing.T) {
	cfg, roots := endpointProcessConfig(t)
	cfg.Basic.Discovery.Group = "executors"
	binary := buildEndpointProcess(t)
	var peers []*process
	for range 3 {
		p := startProcess(t, binary, cfg)
		peers = append(peers, p)
		cfg.Basic.Discovery.Seeds = []string{peers[0].addresses[1]}
	}
	front := emptyConfig(t)
	front.Basic.Listeners.Peer, front.Basic.Diagnostics.Address = "127.0.0.1:0", "127.0.0.1:0"
	for _, peer := range peers {
		front.Basic.Discovery.Seeds = append(front.Basic.Discovery.Seeds, peer.addresses[1])
	}
	seed := startProcess(t, binary, front)
	client := openDiscoveredClient(t, seed.address, []string{"mongo", "search"})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// Wait for a fresh Resolve to expose all same-group replicas before measuring distribution.
	raw := endpointProcessClient(t, seed.address)
	budgetWait(t, "all replicas discovered", func() bool {
		request := &pb.ResolveRequest{Store: "mongo"}
		response, err := raw.Resolve(ctx, request)
		return err == nil && len(response.Targets) == 3
	})
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < 80; i++ {
		for _, kind := range []string{"mongo", "search"} {
			request := &pb.ReadRequest{Resource: fmt.Sprintf("%s/s:process%d", roots[kind], i)}
			result, err := client.Record(ctx, testutil.RecordCall(request))
			if err != nil || result.GetRead().GetMissing() == nil {
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
	}
	if total != 160 {
		t.Fatal("duplicated execution", total)
	}
	peers[0].stop(t)
	budgetWait(t, "departed replica withdrawn", func() bool {
		request := &pb.ResolveRequest{Store: "mongo"}
		response, err := raw.Resolve(ctx, request)
		return err == nil && len(response.Targets) == 2
	})
	time.Sleep(200 * time.Millisecond)
	request := &pb.ReadRequest{Resource: roots["mongo"] + "/s:after-stop"}
	for range 20 {
		result, err := client.Record(ctx, testutil.RecordCall(request))
		if err != nil || result.GetRead().GetMissing() == nil {
			t.Fatal("healthy replicas unavailable", result, err)
		}
	}
	cfg.Basic.Discovery.Seeds = []string{peers[1].addresses[1]}
	cfg.Basic.Listeners.Application = peers[0].address
	replacement := startProcess(t, binary, cfg)
	budgetWait(t, "replacement advertised", func() bool {
		request := &pb.ResolveRequest{Store: "mongo"}
		response, err := raw.Resolve(ctx, request)
		return err == nil && len(response.Targets) == 3
	})
	time.Sleep(200 * time.Millisecond)
	until := time.Now().Add(5 * time.Second)
	for i := 0; ; i++ {
		request.Resource = fmt.Sprintf("%s/s:replacement%d", roots["mongo"], i)
		result, err := client.Record(ctx, testutil.RecordCall(request))
		if err != nil || result.GetRead().GetMissing() == nil {
			t.Fatal(result, err)
		}
		if testmetrics.Sum(testmetrics.Scrape(t, replacement.diagnostic), "weir_store_records_total") > 0 {
			break
		}
		if time.Now().After(until) {
			t.Fatal("replacement never rejoined selection")
		}
	}
	metrics := testmetrics.Scrape(t, seed.diagnostic)
	if metrics["weir_store_executions_total"] != nil || metrics["weir_relay_terminations_total"] != nil {
		t.Fatal("initialization node owns business execution")
	}
	t.Log("three independent same-group executors: direct client distribution, graceful withdrawal, restart and rejoin; seed owns only directory")
}

func TestEndpointDNSAcrossProcesses(t *testing.T) {
	cfg, roots := endpointProcessConfig(t)
	cfg.Basic.Discovery.Group = "dns-executors"
	cfg.Basic.Discovery.Advertise = []string{"executor.weir.test:7447"}
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	binary := buildEndpointProcess(t)
	first := startProcess(t, binary, cfg)
	_, port, _ := net.SplitHostPort(first.address)
	cfg.Basic.Discovery.Advertise = []string{"executor.weir.test:" + port}
	// Restart first with its actual dynamic business port advertised through DNS.
	first.stop(t)
	cfg.Basic.Listeners.Application = first.address
	first = startProcess(t, binary, cfg)
	cfg.Basic.Listeners.Application = net.JoinHostPort("::1", port)
	second := startProcess(t, binary, cfg)
	dns := testdns.Start(t)
	answer := testdns.Answer{Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}
	dns.Set("executor.weir.test", answer)
	dialer := net.Dialer{Timeout: time.Second}
	resolver := &net.Resolver{PreferGo: true, StrictErrors: true, Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, dns.Address)
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	options := routeclient.OpenOptions{Seed: first.address, Stores: []string{"mongo"}, Resolver: resolver, RefreshInterval: 100 * time.Millisecond}
	client, err := routeclient.Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	request := &pb.ReadRequest{Resource: roots["mongo"] + "/s:dns"}
	result, err := client.Record(ctx, testutil.RecordCall(request))
	if err != nil || result.GetRead().GetMissing() == nil {
		t.Fatal(result, err)
	}
	answer.Addresses = []netip.Addr{netip.MustParseAddr("::1")}
	dns.Set("executor.weir.test", answer)
	first.stop(t)
	for {
		result, err := client.Record(ctx, testutil.RecordCall(request))
		if err == nil && result.GetRead().GetMissing() != nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("client DNS refresh did not recover", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if testmetrics.Sum(testmetrics.Scrape(t, second.diagnostic), "weir_store_records_total") != 1 || dns.AAAA.Load() < 2 || dns.Other.Load() != 0 {
		t.Fatal("replacement not proven via DNS/second executor")
	}
	t.Log("client DNS refresh changed direct traffic from IPv4 process to IPv6 process without system DNS changes")
}
