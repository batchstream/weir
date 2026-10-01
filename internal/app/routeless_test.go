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
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/server"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
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
			if err != nil || loaded.Basic != cfg.Basic || loaded.Routing.Services != nil || loaded.Routing.Routes != nil {
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
	for _, document := range []string{
		"{}\n",
		"services: []\nroutes: []\n",
		"services: null\nroutes: null\n",
		"services: []\n",
		"routes: []\n",
	} {
		cfg, err := DecodeRouting(strings.NewReader(document))
		if err != nil || len(cfg.Services) != 0 || len(cfg.Routes) != 0 {
			t.Fatal("both empty collections must form a valid empty graph", err)
		}
	}
	cfg := remoteConfig(t)
	for _, mode := range []string{"services-only", "routes-only"} {
		partial := cfg.Routing
		if mode == "services-only" {
			partial.Routes = nil
		} else {
			partial.Services = nil
		}
		raw, err := yaml.Marshal(partial)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeRouting(strings.NewReader(string(raw))); err == nil || err.Error() != "invalid static graph bounds" {
			t.Fatal("one nonempty collection must not form an empty graph", mode, err)
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
	if len(node.runtimes) != 0 || len(node.remotes) != 0 || len(node.targets) != 1 ||
		len(node.localNames) != 0 || len(node.remoteNames) != 0 {
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
		if i == 1 {
			requestContext = metadata.NewOutgoingContext(ctx, metadata.Pairs(server.HopMetadata, "0"))
		}
		routedResult139, readErr := routeclient.Record(requestContext, client, testutil.RecordCall(request))
		result := routedResult139.GetRead()
		closeErr := connection.Close()
		if status.Code(readErr) != codes.InvalidArgument || closeErr != nil || result != nil {
			t.Fatal("an unknown Store must return a routing status on either listener", readErr, closeErr)
		}
	}
	if len(node.runtimes) != 0 || len(node.remotes) != 0 || health(t, node, "/readyz") != 200 {
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
