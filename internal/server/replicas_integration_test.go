//go:build integration

package server

import (
	"context"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/mongostore"
	"github.com/batchstream/weir/internal/searchstore"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testmetrics"
	"github.com/batchstream/weir/internal/testmongo"
)

// The second adapter/runtime owns a separate driver and scheduler but accesses
// the same task-owned database/index. endpoint optionally shares the fault proxy.
func replicaRuntime(t *testing.T, f scanFixture, endpoint string) *store.Runtime {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var adapter execution.Adapter
	var err error
	if f.backend == nil {
		if endpoint == "" {
			endpoint = testmongo.URI
		}
		cfg := mongostore.Config{URI: endpoint, Store: "mongo", Database: f.db, Collection: "records", Pool: 1}
		adapter, err = mongostore.Open(ctx, cfg)
	} else {
		if endpoint == "" {
			endpoint = f.backend.URL
		}
		cfg := searchstore.Config{URL: endpoint, Store: "search", Index: f.backend.Index, Profile: f.backend.Profile, Pool: 1}
		adapter, err = searchstore.Open(ctx, cfg)
	}
	if err != nil {
		t.Fatal(err)
	}
	limits := store.DefaultLimits()
	limits.Concurrency = 1
	limits.Collect = 0
	r, err := store.New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return r
}

func forwardReplicaFixture(t *testing.T, f scanFixture, hops int) scanFixture {
	t.Helper()
	name, _, _ := protocolResource(f.root)
	replica := replicaRuntime(t, f, "")
	f.replicas = append(f.replicas, replica)
	var addresses []string
	for _, runtime := range append([]*store.Runtime{f.runtime}, f.replicas...) {
		service := Service{LocalStore: runtime}
		opts := peerServerOptions{routes: map[string]Service{name: service}, peer: true, limits: f.server.limits}
		_, address := startPeerServer(t, opts)
		addresses = append(addresses, address)
	}
	for i := 0; i < hops; i++ {
		remote := multipleRemote(t, addresses)
		f.metricsRemotes = append(f.metricsRemotes, remote)
		route := Service{RemoteWeir: remote}
		opts := peerServerOptions{routes: map[string]Service{name: route}, limits: f.server.limits, budget: 4, peer: i < hops-1}
		f.server, f.address = startPeerServer(t, opts)
		addresses = []string{f.address}
	}
	f.conn, f.client = peerClient(t, f.address)
	return f
}

func sumLocalMetric(t *testing.T, f scanFixture, name string) float64 {
	t.Helper()
	var total float64
	for _, r := range append([]*store.Runtime{f.runtime}, f.replicas...) {
		total += testmetrics.Sum(testmetrics.Gather(t, r), name)
	}
	return total
}

func sampleLocalMetric(t *testing.T, f scanFixture, name string, labels map[string]string) float64 {
	t.Helper()
	var total float64
	for _, r := range append([]*store.Runtime{f.runtime}, f.replicas...) {
		metric := testmetrics.Sample(testmetrics.Gather(t, r), name, labels)
		total += metric.GetCounter().GetValue() + metric.GetGauge().GetValue()
	}
	return total
}
