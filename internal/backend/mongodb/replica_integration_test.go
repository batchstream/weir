//go:build integration

package mongodb

import (
	"context"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil/testmongo"
)

func TestMongoReplicaDiscoveryOwnership(t *testing.T) {
	if os.Getenv("WEIR_MONGO_REPLICA_INTEGRATION") != "1" {
		t.Skip("explicit owned replica integration opt-in")
	}
	for _, secure := range []bool{false, true} {
		name := "plain"
		if secure {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			opts := testmongo.ReplicaOptions{TLS: secure}
			fixture := testmongo.OpenReplica(t, opts)
			probe, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			primary, _, err := fixture.WaitUsable(probe, -1)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			// A single secondary seed must discover and select the actual primary.
			seeds := []string{fixture.Addresses()[(primary+1)%3]}
			cfg := Config{Store: "mongo", URI: fixture.SeedURI(seeds), Username: fixture.Username, Password: fixture.Password, CAFile: fixture.CAFile}
			ctx, stop := context.WithTimeout(t.Context(), 3*time.Second)
			adapter, err := Open(ctx, cfg)
			stop()
			if err != nil {
				t.Fatal("production secondary seed discovery", err)
			}
			t.Cleanup(func() { _ = adapter.Close() })
			ownerMutation(t, adapter, fixture.DB, "prepared")
			snapshot := adapter.dialer.snapshot()
			if snapshot.Owned < 2 {
				t.Fatal("missing monitor/data ownership", snapshot)
			}
			bad := cfg
			bad.URI = strings.Replace(cfg.URI, fixture.SetName, "wrong_set", 1)
			replicaRejected(t, bad)
			if secure {
				bad = cfg
				bad.CAFile = fixture.BadCAFile
				replicaRejected(t, bad)
				// Member zero's leaf is localhost-only. The alternate address reaches the
				// same owned process but must fail verification before topology discovery.
				_, port, _ := net.SplitHostPort(fixture.Addresses()[0])
				seeds = []string{net.JoinHostPort("127.0.0.1", port)}
				bad = cfg
				bad.URI = fixture.SeedURI(seeds)
				replicaRejected(t, bad)
			}
			// Remove quorum without creating another fixture or stopping foreign ports.
			fixture.LosePrimary(t, primary)
			fixture.LosePrimary(t, (primary+1)%3)
			cancelled, release := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- adapter.client.Ping(cancelled, nil) }()
			time.Sleep(50 * time.Millisecond)
			start := time.Now()
			release()
			select {
			case err := <-done:
				if err == nil || time.Since(start) > 3*time.Second {
					t.Fatal("unavailable selection cancellation", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("selection cancellation exceeded 3s")
			}
			var closers sync.WaitGroup
			for range 3 {
				closers.Go(func() {
					if err := adapter.Close(); err != nil {
						t.Error(err)
					}
				})
			}
			closers.Wait()
			if time.Since(start) > 5*time.Second {
				t.Fatal("final ownership exceeded 5s")
			}
			ownerZero(t, adapter.dialer)
			t.Logf("three-member %s production discovery, wrong profiles, cancellation and final owner zero; peak=%d acquired=%d", name, adapter.dialer.snapshot().Peak, adapter.dialer.snapshot().Acquired)
		})
	}
}

func replicaRejected(t *testing.T, cfg Config) {
	t.Helper()
	for range 2 {
		ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
		start := time.Now()
		adapter, err := Open(ctx, cfg)
		cancel()
		if adapter != nil {
			_ = adapter.Close()
			t.Fatal("invalid external profile accepted")
		}
		if err == nil || time.Since(start) > 3*time.Second || strings.Contains(err.Error(), cfg.URI) || cfg.Password != "" && strings.Contains(err.Error(), cfg.Password) {
			t.Fatal("failed Open was unbounded or exposed configuration")
		}
	}
}
