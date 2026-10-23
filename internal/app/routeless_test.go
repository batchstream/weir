package app

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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
				fields := withBackend.Routing.Stores[0].Local.credentials()
				fields.Username, fields.UsernameFile = "", "missing-secret-sentinel.txt"
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
	if len(node.stores) != 0 || node.guard.Snapshot().Budget == 0 {
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request := &pb.ReadRequest{Resource: "records/s:key"}
	connection, err := grpc.NewClient("passthrough:///"+node.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	client := pb.NewStoreServiceClient(connection)
	recordResult, readErr := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("missing", request))
	result := recordResult.GetReadResult()
	closeErr := connection.Close()
	if status.Code(readErr) != codes.NotFound || closeErr != nil || result != nil {
		t.Fatal("an unknown Store must be reported as not found", readErr, closeErr)
	}
	if len(node.stores) != 0 || health(t, node, "/readyz") != 200 {
		t.Fatal("unknown Store traffic must not create backends or alter readiness")
	}
	addresses := []string{node.Address(), node.DiagnosticAddress()}
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
