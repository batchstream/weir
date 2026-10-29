package app

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func emptyConfig(t *testing.T) Config {
	t.Helper()
	cfg := Config{}
	cfg.Listeners.Application = "127.0.0.1:0"
	return cfg
}

func TestConfigurationValidation(t *testing.T) {
	for _, mode := range []string{"duplicate-store", "missing-backend"} {
		t.Run(mode, func(t *testing.T) {
			cfg := credentialTestConfig(t, "search")
			switch mode {
			case "duplicate-store":
				cfg.Stores = append(cfg.Stores, cfg.Stores[0])
			case "missing-backend":
				cfg.Stores[0].Search = nil
			}
			if err := cfg.Validate(); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestMongoTLSProfileStaticValidationBeforeSideEffects(t *testing.T) {
	cfg := emptyConfig(t)
	uri := "mongodb://unresolved.invalid:27017/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true"
	credentials := &Credentials{
		Username: "user",
		Password: "password-sentinel",
	}
	mongo := &MongoDB{URI: uri}
	trust := &TLS{CAFile: "/missing/ca.pem"}
	service := StoreConfig{Name: "records", MongoDB: mongo, Authentication: credentials, TLS: trust}
	cfg.Stores = []StoreConfig{service}

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
	credentials := &Credentials{
		Username: "user-sentinel",
		Password: "password-sentinel",
	}
	mongo := &MongoDB{URI: uri}
	service := StoreConfig{Name: "records", MongoDB: mongo, Authentication: credentials}

	cfg := emptyConfig(t)
	cfg.Stores = []StoreConfig{service}

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

func TestAssemblyWithoutStoresPartialStartupAndConcurrentClose(t *testing.T) {
	cfg := emptyConfig(t)
	node, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(node.stores) != 0 || node.guard.Snapshot().Live == 0 {
		t.Fatal("Store-less node created database state")
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
	conn, err := net.DialTimeout("tcp", node.Address(), 100*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		t.Fatal("listener remains live")
	}
	// An occupied diagnostics address fails startup after the application
	// listener is bound; the application listener must be released.
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	first, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Listeners.Application = first.Addr().String()
	_ = first.Close()
	cfg.Listeners.Diagnostics = occupied.Addr().String()
	for range 3 {
		if node, err := Open(context.Background(), cfg); err == nil || node != nil {
			t.Fatal("partial startup succeeded")
		}
	}
	recovered, err := net.Listen("tcp", cfg.Listeners.Application)
	if err != nil {
		t.Fatal("partial listener leaked", err)
	}
	_ = recovered.Close()
}

func TestListenerConfiguration(t *testing.T) {
	for _, listener := range []string{"127.0.0.1:0", "[::1]:7447", "10.20.30.40:7447", "0.0.0.0:7447", "[::]:7447"} {
		cfg := emptyConfig(t)
		cfg.Listeners.Application = listener
		if err := cfg.Validate(); err != nil {
			t.Fatal(listener, err)
		}
	}
	for _, listener := range []string{
		"",
		":7447",
		"localhost:7447",
		"127.0.0.1:-1",
		"127.0.0.1:+1",
		"127.0.0.1:65536",
		"127.0.0.1:",
		"[invalid]:7447",
	} {
		cfg := emptyConfig(t)
		cfg.Listeners.Application = listener
		if err := cfg.Validate(); err == nil {
			t.Fatal("invalid listener accepted", listener)
		}
	}
	for _, listeners := range [][2]string{
		{"127.0.0.1:7447", "127.0.0.1:07447"},
		{"[::1]:7447", "[0:0:0:0:0:0:0:1]:7447"},
		{"0.0.0.0:7447", "127.0.0.1:7447"},
	} {
		cfg := emptyConfig(t)
		cfg.Listeners.Application, cfg.Listeners.Diagnostics = listeners[0], listeners[1]
		if err := cfg.Validate(); err == nil {
			t.Fatal("duplicate/overlapping listeners accepted", listeners)
		}
	}
	cfg := emptyConfig(t)
	cfg.Listeners.Diagnostics = cfg.Listeners.Application
	if err := cfg.Validate(); err != nil {
		t.Fatal("independent ephemeral ports rejected", err)
	}
	for _, diagnostics := range []string{"localhost:7449", ":7449", "invalid"} {
		cfg.Listeners.Diagnostics = diagnostics
		if err := cfg.Validate(); err == nil {
			t.Fatal("invalid diagnostics listener accepted", diagnostics)
		}
	}
	cfg.Listeners.Diagnostics = "0.0.0.0:7449"
	if err := cfg.Validate(); err != nil {
		t.Fatal("non-loopback diagnostics listener rejected", err)
	}
}

func TestUnhostedStoreIsNotFound(t *testing.T) {
	cfg := emptyConfig(t)
	node, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())
	if err := node.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry()}
	conn, err := grpc.NewClient("passthrough:///"+node.Address(), options...)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	client := pb.NewStoreServiceClient(conn)
	request := &pb.ReadRequest{Resource: "records/s:key"}
	result, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("missing", request))
	if status.Code(err) != codes.NotFound || result != nil {
		t.Fatal("unhosted Store was not reported as not found", result, err)
	}
	// A method the server does not serve, such as an outdated client's
	// ResolveStore, is refused before gRPC reads its request.
	empty := &pb.Empty{}
	reply := &pb.Empty{}
	err = conn.Invoke(ctx, "/weir.v1.StoreService/ResolveStore", empty, reply)
	if status.Code(err) != codes.Unimplemented {
		t.Fatal("unserved method was not refused", err)
	}
	families := testmetrics.Registry(t, node.registry)
	labels := map[string]string{"method": "execute", "status": "non_ok"}
	if metric := testmetrics.Sample(families, "weir_rpc_completions_total", labels); metric.GetCounter().GetValue() != 1 {
		t.Fatal("rejected RPC was not counted", metric)
	}
	reason := map[string]string{"reason": "method"}
	if metric := testmetrics.Sample(families, "weir_admission_rejections_total", reason); metric.GetCounter().GetValue() != 1 {
		t.Fatal("unserved method was not counted as a method rejection", metric)
	}
}

func TestLargeConfiguration(t *testing.T) {
	cfg := credentialTestConfig(t, "search")
	base := cfg.Stores[0]
	cfg.Stores = nil
	for i := range 32 {
		service := base
		service.Name = fmt.Sprintf("store-%03d", i)
		cfg.Stores = append(cfg.Stores, service)
	}
	base.Authentication.Username = strings.Repeat("u", 8192)
	base.Authentication.Password = strings.Repeat("p", 16384)
	loaded, err := Load(writeCredentialTestDocument(t, cfg))
	if err != nil {
		t.Fatal("valid configuration retained an artificial scale threshold", err)
	}
	if len(loaded.Stores) != 32 || loaded.Stores[0].Authentication.Password != base.Authentication.Password {
		t.Fatal("large configuration was truncated")
	}
}
