package app

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func remoteConfig(t *testing.T) Config {
	t.Helper()
	remote := &Remote{Endpoints: []string{"127.0.0.1:1"}, MaxConcurrency: 2}
	service := Service{Name: "remote", Remote: remote}
	route := Route{Store: "records", Service: "remote"}
	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	cfg.Routing.Services = []Service{service}
	cfg.Routing.Routes = []Route{route}
	return cfg
}

func TestConfigurationValidation(t *testing.T) {
	for _, mode := range []string{"duplicate-store", "unknown-service", "duplicate-service", "overflow", "zero-session"} {
		t.Run(mode, func(t *testing.T) {
			cfg := remoteConfig(t)
			switch mode {
			case "duplicate-store":
				cfg.Routing.Routes = append(cfg.Routing.Routes, cfg.Routing.Routes[0])
			case "unknown-service":
				cfg.Routing.Routes[0].Service = "missing"
			case "duplicate-service":
				cfg.Routing.Services = append(cfg.Routing.Services, cfg.Routing.Services[0])
			case "overflow":
				cfg.Basic.Transport.Timeouts.Unary = Duration(1<<63 - 1)
			case "zero-session":
				cfg.Basic.Transport.MaxSessions = 0
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
	uri := "mongodb://user:password-sentinel@unresolved.invalid:27017/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tlsCAFile=%2Fmissing%2Fca.pem"
	mongo := &Mongo{URI: uri, Database: "catalog", Collection: "records"}
	local := &Local{MongoDB: mongo}
	service := Service{Name: "catalog", Local: local}
	route := Route{Store: "records", Service: service.Name}
	cfg.Routing.Services = []Service{service}
	cfg.Routing.Routes = []Route{route}
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
	uri := "mongodb://user-sentinel:password-sentinel@" + address + "/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true"
	mongo := &Mongo{URI: uri, Database: "catalog", Collection: "records"}
	local := &Local{MongoDB: mongo}
	service := Service{Name: "database", Local: local}
	route := Route{Store: "records", Service: service.Name}
	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	cfg.Routing.Services = []Service{service}
	cfg.Routing.Routes = []Route{route}
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

func TestAssemblyForwardOnlyPartialListenerAndConcurrentClose(t *testing.T) {
	cfg := remoteConfig(t)
	node, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(node.runtimes) != 0 || len(node.remotes) != 1 || len(node.targets) != 1 {
		t.Fatal("forward-only created database state")
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
	// and the already-created peer ClientConn, without a database adapter.
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
		cfg := remoteConfig(t)
		cfg.Basic.Listeners.Application = listener
		if err := cfg.Validate(); err != nil {
			t.Fatal(listener, err)
		}
		cfg.Basic.Listeners.Application, cfg.Basic.Listeners.Peer = "", listener
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
		cfg := remoteConfig(t)
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
		cfg := remoteConfig(t)
		cfg.Basic.Listeners.Application, cfg.Basic.Listeners.Peer, cfg.Basic.Diagnostics.Address = listeners[0], listeners[1], listeners[2]
		if err := cfg.Validate(); err == nil {
			t.Fatal("duplicate/overlapping listeners accepted", listeners)
		}
	}
	cfg := remoteConfig(t)
	cfg.Basic.Listeners.Peer, cfg.Basic.Diagnostics.Address = cfg.Basic.Listeners.Application, cfg.Basic.Listeners.Application
	if err := cfg.Validate(); err != nil {
		t.Fatal("independent ephemeral ports rejected", err)
	}
}

func TestRemovedAuthenticationFieldsAreUnknown(t *testing.T) {
	cfg := remoteConfig(t)
	cfg.Basic.Routing.File = "routing.json"
	basic, err := json.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"identity", "allow", "lua_worker"} {
		input := `{"` + field + `":null,` + string(basic[1:])
		if _, err := DecodeBasic(strings.NewReader(input)); err == nil {
			t.Fatal("legacy field accepted", field)
		}
	}
	routing, err := json.Marshal(cfg.Routing)
	if err != nil {
		t.Fatal(err)
	}
	input := strings.Replace(string(routing), `"remote":{`, `"remote":{"server_name":"obsolete",`, 1)
	if _, err := DecodeRouting(strings.NewReader(input)); err == nil {
		t.Fatal("legacy remote identity accepted")
	}
}

func TestEphemeralListenersKeepDistinctHopRules(t *testing.T) {
	cfg := remoteConfig(t)
	cfg.Basic.Listeners.Peer = cfg.Basic.Listeners.Application
	node, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close(context.Background())
	node.Start(context.Background())
	addresses := node.Addresses()
	if len(addresses) != 2 || addresses[0] == addresses[1] {
		t.Fatal("ephemeral listeners not distinct", addresses)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request := &pb.ReadRequest{Resource: "weir://missing/records/s:key"}
	for i, address := range addresses {
		conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		client := pb.NewWeirClient(conn)
		result, err := client.Read(ctx, request)
		if i == 0 {
			if err != nil || result.GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT {
				t.Fatal("application requires no peer metadata", result, err)
			}
		} else if status.Code(err) != codes.InvalidArgument {
			t.Fatal("peer accepted missing hop", result, err)
		}
		peerCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(server.HopMetadata, "0"))
		result, err = client.Read(peerCtx, request)
		if i == 0 {
			if status.Code(err) != codes.InvalidArgument {
				t.Fatal("application accepted client hop", result, err)
			}
		} else if err != nil || result.GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT {
			t.Fatal("peer accepted unknown Store or rejected canonical hop", result, err)
		}
	}
}

func TestCurrentPeerExamples(t *testing.T) {
	for _, name := range []string{"peer-a.json", "peer-b.json"} {
		filename := filepath.Join(testutil.Root(t), "examples", name)
		if _, err := Load(filename); err != nil {
			t.Fatal(name, err)
		}
	}
}

func TestRemoteEndpointListConfiguration(t *testing.T) {
	cfg := remoteConfig(t)
	raw, err := json.Marshal(cfg.Routing)
	if err != nil {
		t.Fatal(err)
	}
	input := strings.Replace(string(raw), `"endpoints":["127.0.0.1:1"]`, `"endpoint":"127.0.0.1:1"`, 1)
	if _, err := DecodeRouting(strings.NewReader(input)); err == nil {
		t.Fatal("legacy endpoint accepted", err)
	}
	for _, endpoints := range [][]string{
		nil,
		{},
		{"peer:1", "PEER.:01"},
		{"dns:///peer:1"},
		{"a:1", "b:1", "c:1", "d:1", "e:1", "f:1", "g:1", "h:1", "i:1"},
	} {
		cfg.Routing.Services[0].Remote.Endpoints = endpoints
		if err := cfg.Validate(); err == nil {
			t.Fatal("invalid endpoint list accepted", endpoints)
		}
	}
	cfg.Routing.Services[0].Remote.Endpoints = []string{"peer.example:1", "[::1]:1", "127.0.0.1:1"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}
