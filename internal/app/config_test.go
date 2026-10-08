package app

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func emptyConfig(t *testing.T) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	return cfg
}

func TestConfigurationValidation(t *testing.T) {
	for _, mode := range []string{"duplicate-store", "missing-backend"} {
		t.Run(mode, func(t *testing.T) {
			cfg := credentialTestConfig(t, "search")
			switch mode {
			case "duplicate-store":
				cfg.Routing.Stores = append(cfg.Routing.Stores, cfg.Routing.Stores[0])
			case "missing-backend":
				cfg.Routing.Stores[0].Local = nil
			}
			if err := cfg.Validate(); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestMongoTLSProfileStaticValidationBeforeSideEffects(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	uri := "mongodb://unresolved.invalid:27017/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tlsCAFile=%2Fmissing%2Fca.pem"
	credentials := Credentials{
		Username: "user",
		Password: "password-sentinel",
	}
	mongo := &Mongo{URI: uri, Credentials: credentials}
	local := &Local{Backend: BackendConfig{MongoDB: mongo}}
	service := StoreConfig{Name: "records", Local: local}

	cfg.Routing.Stores = []StoreConfig{service}

	err := cfg.Validate()
	if err != nil {
		t.Fatal("valid URI must be accepted without accessing missing CA or DNS", err)
	}
	mongo.URI += "&tlsInsecure=true"
	node, err := Open(context.Background(), cfg)
	if node != nil {
		node.Close(context.Background())
	}
	if err == nil || strings.Contains(err.Error(), "password-sentinel") || strings.Contains(err.Error(), "missing/ca.pem") {
		t.Fatal("unsafe URI accepted or leaked configuration")
	}
}

func TestMongoStartupRedactsDriverConnectionFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	uri := "mongodb://" + address + "/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true"
	credentials := Credentials{
		Username: "user-sentinel",
		Password: "password-sentinel",
	}
	mongo := &Mongo{URI: uri, Credentials: credentials}
	local := &Local{Backend: BackendConfig{MongoDB: mongo}}
	service := StoreConfig{Name: "records", Local: local}

	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	cfg.Routing.Stores = []StoreConfig{service}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	node, err := Open(ctx, cfg)
	if node != nil ||
		err == nil ||
		!strings.Contains(err.Error(), "MongoDB replica-set qualification failed") ||
		strings.Contains(err.Error(), "sentinel") ||
		strings.Contains(err.Error(), address) ||
		strings.Contains(err.Error(), "mongodb://") {
		t.Fatal("startup exposed a driver connection error or lost its qualification reason", err)
	}
}

func TestAssemblyDirectoryOnlyPartialListenerAndConcurrentClose(t *testing.T) {
	cfg := emptyConfig(t)
	node, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(node.stores) != 0 || node.guard.Snapshot().Budget == 0 {
		t.Fatal("directory-only created database state")
	}
	node.Start(context.Background())
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if err := node.Close(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	node.Start(context.Background())
	for _, address := range node.Addresses() {
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			t.Fatal("listener remains live")
		}
	}
	// A valid graph with an occupied second listener unwinds the first listener
	// without a database adapter.
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	first, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Basic.Listeners.Application = first.Addr().String()
	_ = first.Close()
	cfg.Basic.Listeners.Peer = occupied.Addr().String()
	for range 3 {
		if node, err := Open(context.Background(), cfg); err == nil || node != nil {
			t.Fatal("partial startup succeeded")
		}
	}
	recovered, err := net.Listen("tcp", cfg.Basic.Listeners.Application)
	if err != nil {
		t.Fatal("partial listener leaked", err)
	}
	_ = recovered.Close()
}

func TestIntranetListenerConfiguration(t *testing.T) {
	for _, listener := range []string{"127.0.0.1:0", "[::1]:7447", "10.20.30.40:7447", "0.0.0.0:7447", "[::]:7447"} {
		cfg := emptyConfig(t)
		cfg.Basic.Listeners.Application = listener
		if err := cfg.Validate(); err != nil {
			t.Fatal(listener, err)
		}
		cfg.Basic.Listeners.Application, cfg.Basic.Listeners.Peer = "", listener
		if strings.HasPrefix(listener, "0.0.0.0:") || strings.HasPrefix(listener, "[::]:") {
			cfg.Basic.Discovery.PeerAddress = "peer.example:7448"
		}
		if err := cfg.Validate(); err != nil {
			t.Fatal(listener, err)
		}
	}
	for _, listener := range []string{
		":7447",
		"localhost:7447",
		"127.0.0.1:-1",
		"127.0.0.1:+1",
		"127.0.0.1:65536",
		"127.0.0.1:",
		"[invalid]:7447",
	} {
		cfg := emptyConfig(t)
		cfg.Basic.Listeners.Application = listener
		if err := cfg.Validate(); err == nil {
			t.Fatal("invalid listener accepted", listener)
		}
	}
	for _, listeners := range [][3]string{
		{"127.0.0.1:7447", "127.0.0.1:7447", ""},
		{"[::1]:7447", "[0:0:0:0:0:0:0:1]:7447", ""},
		{"127.0.0.1:7447", "", "127.0.0.1:07447"},
		{"0.0.0.0:7447", "127.0.0.1:7447", ""},
	} {
		cfg := emptyConfig(t)
		cfg.Basic.Listeners.Application, cfg.Basic.Listeners.Peer, cfg.Basic.Diagnostics.Address = listeners[0], listeners[1], listeners[2]
		if err := cfg.Validate(); err == nil {
			t.Fatal("duplicate/overlapping listeners accepted", listeners)
		}
	}
	cfg := emptyConfig(t)
	cfg.Basic.Listeners.Peer, cfg.Basic.Diagnostics.Address = cfg.Basic.Listeners.Application, cfg.Basic.Listeners.Application
	if err := cfg.Validate(); err != nil {
		t.Fatal("independent ephemeral ports rejected", err)
	}
}

func TestEphemeralListenersSeparateBusinessAndDirectory(t *testing.T) {
	cfg := emptyConfig(t)
	cfg.Basic.Listeners.Peer = cfg.Basic.Listeners.Application
	node, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	addresses := node.Addresses()
	if len(addresses) != 2 || addresses[0] == addresses[1] {
		t.Fatal("ephemeral listeners not distinct", addresses)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request := &pb.ReadRequest{Resource: "records/s:key"}
	for i, address := range addresses {
		conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		client := pb.NewStoreServiceClient(conn)
		result, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("missing", request))
		want := codes.Unavailable
		if i == 1 {
			want = codes.Unimplemented
		}
		if status.Code(err) != want || result != nil {
			t.Fatal("listener accepted unsupported business destination", i, result, err)
		}
	}
}

func TestListenerRolesMatchAddressesAndMetricLabels(t *testing.T) {
	for _, mode := range []string{"application", "peer", "both"} {
		t.Run(mode, func(t *testing.T) {
			cfg := emptyConfig(t)
			if mode != "application" {
				cfg.Basic.Listeners.Peer = "127.0.0.1:0"
			}
			if mode == "peer" {
				cfg.Basic.Listeners.Application = ""
			}
			node, err := Open(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = node.Close(context.Background()) })
			if err := node.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			roles := []string{mode}
			if mode == "both" {
				roles = []string{"application", "peer"}
			}
			addresses := node.Addresses()
			if len(addresses) != len(roles) {
				t.Fatal("listener count changed", addresses)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			for i, role := range roles {
				options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry()}
				connection, err := grpc.NewClient("passthrough:///"+addresses[i], options...)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = connection.Close() })
				client := pb.NewStoreServiceClient(connection)
				request := &pb.ResolveStoreRequest{StoreName: "missing"}
				_, err = client.ResolveStore(ctx, request)
				want := codes.Unavailable
				if role == "peer" {
					want = codes.Unimplemented
				}
				if status.Code(err) != want {
					t.Fatal("address serves the wrong listener role", role, err)
				}
				peer := peerpb.NewPeerDiscoveryServiceClient(connection)
				exchange := &peerpb.SyncDirectoryRequest{}
				_, err = peer.SyncDirectory(ctx, exchange)
				want = codes.Unimplemented
				if role == "peer" {
					want = codes.OK
				}
				if status.Code(err) != want {
					t.Fatal("directory service does not match listener role", role, err)
				}
			}
			families := testmetrics.Registry(t, node.registry)
			for _, role := range []string{"application", "peer"} {
				labels := map[string]string{"listener": role}
				metric := testmetrics.Sample(families, "weir_rpc_completions_total", labels)
				if (metric != nil) != (mode == "both" || mode == role) {
					t.Fatal("listener metrics use the wrong role", role)
				}
			}
		})
	}
}

func TestCurrentPeerExamples(t *testing.T) {
	for _, name := range []string{"peer-a.yaml", "peer-b.yaml"} {
		filename := filepath.Join(testutil.Root(t), "examples", name)
		routingFilename := strings.TrimSuffix(filename, ".yaml") + ".routes.yaml"
		if _, err := Load(filename, routingFilename); err != nil {
			t.Fatal(name, err)
		}
	}
}

func TestGenericDiscoveryConfiguration(t *testing.T) {
	cfg := emptyConfig(t)
	cfg.Basic.Listeners.Peer = "127.0.0.1:0"
	cfg.Basic.Discovery.Group = "catalog"
	cfg.Basic.Discovery.PeerAddress = "peer.example:7448"
	cfg.Basic.Discovery.Seeds = []string{"seed.example:7448", "[::1]:7448"}
	cfg.Basic.Discovery.Advertise = []string{"catalog.example:7447", "192.0.2.1:7447"}
	if err := cfg.Validate(); err != nil {
		t.Fatal("generic DNS/IP discovery rejected", err)
	}
	for _, targets := range [][]string{{"dns:///peer:7447"}, {"peer:0"}, {"peer:7447", "PEER.:07447"}} {
		cfg.Basic.Discovery.Advertise = targets
		if err := cfg.Validate(); err == nil {
			t.Fatal("invalid discovery addresses accepted", targets)
		}
	}
}
