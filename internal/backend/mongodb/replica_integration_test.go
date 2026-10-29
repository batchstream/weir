//go:build integration

package mongodb

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoReplicaDiscovery(t *testing.T) {
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
			t.Cleanup(func() { _ = adapter.Close(context.Background()) })
			putPlan(t, adapter, fixture.DB, "prepared")
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
					if err := adapter.Close(context.Background()); err != nil {
						t.Error(err)
					}
				})
			}
			closers.Wait()
			if time.Since(start) > 5*time.Second {
				t.Fatal("close exceeded 5s")
			}
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
			_ = adapter.Close(context.Background())
			t.Fatal("invalid external profile accepted")
		}
		if err == nil || time.Since(start) > 3*time.Second || strings.Contains(err.Error(), cfg.URI) || cfg.Password != "" && strings.Contains(err.Error(), cfg.Password) {
			t.Fatal("failed Open was unbounded or exposed configuration")
		}
	}
}

func putPlan(t *testing.T, a *Adapter, database, id string) *execution.Plan {
	t.Helper()
	doc := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: 1}}
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	body := &pb.Document{ContentType: "application/bson", Data: raw}
	put := &pb.MutateRequest_Put{Put: body}
	req := &pb.MutateRequest{Resource: database + "/records/s:" + id, Action: put}
	opOperation := &pb.Command_Mutate{Mutate: req}
	opCommand := &pb.Command{Operation: opOperation}
	op := &pb.ExecuteRequest{Index: 1, Command: opCommand}
	plan, failure := prepareTestRecord(a, op)
	if failure != nil {
		t.Fatal(failure)
	}
	return plan
}
