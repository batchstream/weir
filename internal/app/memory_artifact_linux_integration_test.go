//go:build integration && linux

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Reuses the owned M14 fixture, but observes the exact archive without starting
// its 416MiB cgroup pressure helper. M19's extra allocation cap stays 256MiB.
func TestLinuxMemoryArtifactObservation(t *testing.T) {
	if os.Getenv("WEIR_M14_NATIVE") != "1" {
		t.Skip("owned native Linux fixture required")
	}
	address := os.Getenv("WEIR_M14_MONGO_ADDR")
	if !strings.HasPrefix(address, "host.docker.internal:") {
		t.Fatal("explicit mixed Darwin backend required")
	}
	source := os.Getenv("WEIR_M19_SOURCE")
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "/fixture/weir", "version").Output()
	if err != nil {
		t.Fatal(err)
	}
	var version map[string]string
	if err := json.Unmarshal(output, &version); err != nil {
		t.Fatal(err)
	}
	if len(source) != 40 || version["revision"] != source || version["target"] != "linux/arm64" || version["state"] != "clean-commit" {
		t.Fatal(version)
	}
	uri := "mongodb://" + address + "/?directConnection=true&serverMonitoringMode=poll"
	opts := options.Client().ApplyURI(uri).SetRetryReads(false).SetRetryWrites(false).SetMaxPoolSize(2).SetServerSelectionTimeout(time.Second)
	admin, err := mongo.Connect(opts)
	if err != nil {
		t.Fatal(err)
	}
	db := fmt.Sprintf("m19_%d", os.Getpid())
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := admin.Database(db).Drop(cleanup); err != nil {
			t.Error(err)
		}
		if err := admin.Disconnect(cleanup); err != nil {
			t.Error(err)
		}
	})
	if err := admin.Database(db).CreateCollection(ctx, "records"); err != nil {
		t.Fatal(err)
	}
	cfg := packagedConfig(t, uri)
	cfg.Basic.Memory = 128 << 20
	p := startProcess(t, "/fixture/weir", cfg)
	client := endpointProcessClient(t, p.address)
	request := budgetPut("weir://records/"+db+"/records", "observed")
	for mode := 0; mode < 3; mode++ {
		if !budgetLoadCall(ctx, client, request, mode) {
			t.Fatal("exact Linux app Read/Mutate/Bulk", mode)
		}
	}
	time.Sleep(200 * time.Millisecond)
	metrics := testmetrics.Scrape(t, p.diagnostic)
	labels := map[string]string{"source": "linux_rss"}
	rss := uint64(testmetrics.Sample(metrics, "weir_memory_sample_bytes", labels).GetGauge().GetValue())
	oracle := memoryProcessRSS(t, p.command.Process.Pid)
	difference := rss
	if rss > oracle {
		difference = rss - oracle
	} else {
		difference = oracle - rss
	}
	if rss == 0 ||
		difference > 8<<20 ||
		testmetrics.Sum(metrics, "weir_memory_unknown") != 0 ||
		testmetrics.Sum(metrics, "weir_memory_cgroup_valid") != 1 ||
		testmetrics.Sum(metrics, "weir_memory_cgroup_limit_bytes") != 512<<20 {
		t.Fatal("Linux memory observation", rss, oracle)
	}
	labels = map[string]string{"source": "darwin_phys_footprint"}
	if testmetrics.Sample(metrics, "weir_memory_sample_bytes", labels).GetGauge().GetValue() != 0 {
		t.Fatal("Darwin source active on Linux")
	}
	start := time.Now()
	p.stop(t)
	if time.Since(start) > 3*time.Second {
		t.Fatal("exact Linux close bound")
	}
	t.Logf(
		"exact source=%s PID=%d RSS=%d smaps=%d cgroup=512MiB; Read/Mutate/Bulk and SIGTERM=%s; mixed owned Darwin Mongo",
		source,
		p.command.Process.Pid,
		rss,
		oracle,
		time.Since(start),
	)
}
