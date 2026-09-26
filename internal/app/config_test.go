package app

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testpeer"
)

func remoteConfig(t *testing.T) Config {
	t.Helper()
	ca := testpeer.NewCA(t)
	_, certificate, key := ca.Identity(t, "node.weir.test")
	dir := t.TempDir()
	certPath, keyPath, caPath := filepath.Join(dir, "identity.crt"), filepath.Join(dir, "identity.private"), filepath.Join(dir, "ca.crt")
	for name, data := range map[string][]byte{certPath: certificate, keyPath: key, caPath: ca.PEM} {
		if err := os.WriteFile(name, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	identity := &Identity{Certificate: certPath, PrivateKey: keyPath, CA: caPath}
	remote := &Remote{Endpoint: "127.0.0.1:1", ServerName: "node.weir.test", Relays: 2}
	service := Service{Name: "remote", Remote: remote}
	route := Route{Store: "records", Service: "remote"}
	cfg := DefaultConfig()
	cfg.Application = "127.0.0.1:0"
	cfg.Identity = identity
	cfg.Services = []Service{service}
	cfg.Routes = []Route{route}
	return cfg
}
func TestStrictConfiguration(t *testing.T) {
	cfg := remoteConfig(t)
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(strings.NewReader(string(raw))); err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{`"unknown":1,`, `"memory_mib":1,`, `"memory_mib":512,`, `"MEMORY_MIB":512,`, `"initial_forwards":9,`} {
		if _, err := Decode(strings.NewReader("{" + fragment + string(raw[1:]))); err == nil {
			t.Fatal("accepted unknown/duplicate", fragment)
		}
	}
	for _, mode := range []string{"duplicate-store", "unknown-service", "duplicate-service", "wildcard", "public", "missing-tls", "bad-grant", "duplicate-grant", "overflow", "zero-session", "extra-data"} {
		t.Run(mode, func(t *testing.T) {
			altered, err := Decode(strings.NewReader(string(raw)))
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "duplicate-store":
				altered.Routes = append(altered.Routes, altered.Routes[0])
			case "unknown-service":
				altered.Routes[0].Service = "missing"
			case "duplicate-service":
				altered.Services = append(altered.Services, altered.Services[0])
			case "wildcard":
				altered.Services[0].Remote.ServerName = "*.weir.test"
			case "public":
				altered.Application = "0.0.0.0:7447"
			case "missing-tls":
				altered.Identity = nil
			case "bad-grant":
				altered.Peer = "127.0.0.1:0"
				grant := Grant{Identity: "a.weir.test", Store: "missing", Operations: []string{"read"}}
				altered.Allow = []Grant{grant}
			case "duplicate-grant":
				altered.Peer = "127.0.0.1:0"
				grant := Grant{Identity: "a.weir.test", Store: "records", Operations: []string{"read", "read"}}
				altered.Allow = []Grant{grant}
			case "overflow":
				altered.Limits.UnaryMS = int(^uint(0) >> 1)
			case "zero-session":
				altered.Limits.Sessions = 0
			case "extra-data":
				if _, err := Decode(strings.NewReader(string(raw) + ` {}`)); err == nil {
					t.Fatal("trailing accepted")
				}
				return
			}
			if err := altered.Validate(); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
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
	node.Start()
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
	node.Start()
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
	cfg.Application = first.Addr().String()
	_ = first.Close()
	cfg.Peer = occupied.Addr().String()
	grant := Grant{Identity: "a.weir.test", Store: "records", Operations: []string{"read"}}
	cfg.Allow = []Grant{grant}
	for range 3 {
		if node, err := Open(context.Background(), cfg); err == nil || node != nil {
			t.Fatal("partial startup succeeded")
		}
	}
	recovered, err := net.Listen("tcp", cfg.Application)
	if err != nil {
		t.Fatal("partial listener leaked", err)
	}
	_ = recovered.Close()
}
