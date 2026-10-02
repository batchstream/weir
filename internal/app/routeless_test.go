package app

import (
	"context"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/routeclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestLoadWithoutRoutingDoesNotDiscoverFiles(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	basic, err := yaml.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	basicFilename := filepath.Join(directory, "weir.yaml")
	if err := os.WriteFile(basicFilename, basic, 0600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(directory)
	for _, mode := range []string{"missing", "malformed", "missing-credential"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "malformed" {
				if err := os.WriteFile("routes.yaml", []byte("malformed-secret-sentinel: ["), 0600); err != nil {
					t.Fatal(err)
				}
			} else if mode == "missing-credential" {
				withBackend := credentialTestConfig(t, "search")
				fields := credentialFields(&withBackend)
				*fields.username, *fields.usernameFile = "", "missing-secret-sentinel.txt"
				routing, err := yaml.Marshal(withBackend.Routing)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile("routes.yaml", routing, 0600); err != nil {
					t.Fatal(err)
				}
			}
			loaded, err := Load("weir.yaml", "")
			if err != nil || !reflect.DeepEqual(loaded.Basic, cfg.Basic) || loaded.Routing.Stores != nil {
				t.Fatal("an omitted routing path must not discover files or resolve credentials", err)
			}
			if mode != "missing" {
				if _, err := Load("weir.yaml", "routes.yaml"); err == nil {
					t.Fatal("the explicitly selected routing file should still fail validation")
				}
			}
		})
	}
}

func TestEmptyRoutingDocuments(t *testing.T) {
	for _, document := range []string{"{}\n", "stores: []\n", "stores: null\n"} {
		cfg, err := DecodeRouting(strings.NewReader(document))
		if err != nil || len(cfg.Stores) != 0 {
			t.Fatal("empty Store config rejected", err)
		}
	}
}

func TestRoutelessNodeLifecycleAndUnknownStore(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	cfg.Basic.Listeners.Peer = "127.0.0.1:0"
	cfg.Basic.Diagnostics.Address = "127.0.0.1:0"
	node, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if len(node.runtimes) != 0 || len(node.targets) != 1 ||
		len(node.localNames) != 0 {
		t.Fatal("an empty graph must construct admission only, with no backend or remote state")
	}
	if node.ready() {
		t.Fatal("routeless node is ready before startup")
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if health(t, node, "/readyz") != 200 || health(t, node, "/livez") != 200 || health(t, node, "/metrics") != 200 {
		t.Fatal("routeless node did not serve diagnostics after startup")
	}
	addresses := node.Addresses()
	if len(addresses) != 2 {
		t.Fatal("both configured data-plane listeners must start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request := &pb.ReadRequest{Resource: "weir://missing/records/s:key"}
	for i, address := range addresses {
		connection, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry())
		if err != nil {
			t.Fatal(err)
		}
		client := pb.NewWeirClient(connection)
		requestContext := ctx
		routedResult139, readErr := routeclient.Record(requestContext, client, testutil.RecordCall(request))
		result := routedResult139.GetRead()
		closeErr := connection.Close()
		want := codes.Unavailable
		if i == 1 {
			want = codes.Unimplemented
		}
		if status.Code(readErr) != want || closeErr != nil || result != nil {
			t.Fatal("an unknown Store must return a routing status on either listener", readErr, closeErr)
		}
	}
	if len(node.runtimes) != 0 || health(t, node, "/readyz") != 200 {
		t.Fatal("unknown Store traffic must not create backends or alter readiness")
	}
	addresses = append(addresses, node.DiagnosticAddress())
	if err := node.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if node.ready() {
		t.Fatal("closed routeless node is ready")
	}
	for _, address := range addresses {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatal("routeless shutdown did not release a listener", err)
		}
		_ = listener.Close()
	}
}

func TestDiscoveryOnlyWildcardApplicationLearnsTargets(t *testing.T) {
	for _, application := range []string{"0.0.0.0:0", "[::]:0"} {
		t.Run(application, func(t *testing.T) {
			loopback := "127.0.0.1"
			if strings.HasPrefix(application, "[") {
				probe, err := net.Listen("tcp6", "[::1]:0")
				if err != nil {
					t.Skip("IPv6 loopback unavailable")
				}
				_ = probe.Close()
				loopback = "::1"
			}
			cfg := emptyConfig(t)
			cfg.Basic.Listeners.Application = application
			cfg.Basic.Listeners.Peer = "127.0.0.1:0"
			cfg.Basic.Discovery.Group = "discovery-only"
			node, err := Open(context.Background(), cfg)
			if err != nil {
				t.Fatal("discovery-only wildcard listener failed assembly", err)
			}
			t.Cleanup(func() { _ = node.Close(context.Background()) })
			if err := node.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			addresses := node.Addresses()
			peerConnection, err := grpc.NewClient("passthrough:///"+addresses[1], grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy())
			if err != nil {
				t.Fatal(err)
			}
			defer peerConnection.Close()
			advertisement := &pb.NodeAdvertisement{
				NodeId: strings.Repeat("1", 32), Sequence: 1,
				Group: "owners", Stores: []string{"records"}, Targets: []string{"business.example:7447"},
				RemainingLeaseMs: 10000,
			}
			exchangeRequest := &pb.ExchangeRequest{Nodes: []*pb.NodeAdvertisement{advertisement}}
			peer := pb.NewDirectoryClient(peerConnection)
			exchanged, err := peer.Exchange(ctx, exchangeRequest)
			if err != nil {
				t.Fatal("discovery-only node rejected learned Store", err)
			}
			localAdvertisement := false
			for _, entry := range exchanged.Nodes {
				if entry.Group == "discovery-only" {
					localAdvertisement = true
					if len(entry.Stores) != 0 || len(entry.Targets) != 0 {
						t.Fatal("discovery-only node advertised business targets", entry)
					}
				}
			}
			if !localAdvertisement {
				t.Fatal("local directory membership missing")
			}
			_, port, err := net.SplitHostPort(addresses[0])
			if err != nil {
				t.Fatal(err)
			}
			applicationAddress := net.JoinHostPort(loopback, port)
			connection, err := grpc.NewClient("passthrough:///"+applicationAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy())
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			client := pb.NewWeirClient(connection)
			resolveRequest := &pb.ResolveRequest{Store: "records"}
			resolved, err := client.Resolve(ctx, resolveRequest)
			if err != nil || resolved.Group != "owners" || len(resolved.Targets) != 1 || resolved.Targets[0] != "business.example:7447" {
				t.Fatal("wildcard initialization ingress did not Resolve learned targets", resolved, err)
			}
			if err := node.Close(ctx); err != nil {
				t.Fatal(err)
			}
			for _, address := range addresses {
				listener, err := net.Listen("tcp", address)
				if err != nil {
					t.Fatal("wildcard discovery-only shutdown retained a listener", err)
				}
				_ = listener.Close()
			}
		})
	}
}
