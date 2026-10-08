//go:build integration

package app

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
)

func TestDiagnosticsMaximumStaticSeries(t *testing.T) {
	backend := testmongo.Open(t)
	cfg := emptyConfig(t)
	first, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Basic.Listeners.Application = first.Addr().String()
	_ = first.Close()
	cfg.Basic.Listeners.Peer = "127.0.0.1:0"
	cfg.Basic.Diagnostics.Address = "127.0.0.1:0"
	cfg.Routing.Stores = nil

	for i := 0; i < 16; i++ {
		name := fmt.Sprintf("local%d", i)
		mongo := mongoFixtureConfig(t, backend.URI)
		local := &Local{Backend: BackendConfig{MongoDB: mongo}}
		service := StoreConfig{Name: name, Local: local}
		cfg.Routing.Stores = append(cfg.Routing.Stores, service)

	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	n, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close(context.Background())
	n.Start(context.Background())
	families := testmetrics.Scrape(t, n.DiagnosticAddress())
	// The fixed vocabulary belongs to Weir. Go and process collectors vary with
	// the toolchain and operating system, so they do not enter this budget.
	for name := range families {
		if !strings.HasPrefix(name, "weir_") {
			delete(families, name)
		}
	}
	// 16 Mongo Stores at 59 each + 2 listener transports at 30 each +
	// 13 admission + 2 directory + 7 diagnostics + 36 Node lifecycle/memory.
	const maximumSeries = 1062
	if got := testmetrics.Series(families); got != maximumSeries {
		t.Fatal("maximum static series changed", got)
	}
	if testmetrics.Sum(families, "weir_store_executions_total") != 0 {
		t.Fatal("diagnostics executed database work")
	}
	for _, runtime := range n.stores {
		runtime.SetOverloaded(true)
	}
	if health(t, n, "/readyz") != 200 {
		t.Fatal("Store overload changed readiness")
	}
	after := testmetrics.Scrape(t, n.DiagnosticAddress())
	for name := range after {
		if !strings.HasPrefix(name, "weir_") {
			delete(after, name)
		}
	}
	if testmetrics.Series(after) != maximumSeries {
		t.Fatal("state added series")
	}
	t.Logf("maximum local Store configuration: 16 LocalStores, business and peer directory listeners, exactly %d Weir Prometheus series; no synthetic executions", maximumSeries)
}
