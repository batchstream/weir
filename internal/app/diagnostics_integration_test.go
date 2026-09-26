//go:build integration

package app

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testmetrics"
	"github.com/batchstream/weir/internal/testmongo"
)

func TestDiagnosticsMaximumStaticSeries(t *testing.T) {
	_, database := testmongo.Open(t)
	cfg := remoteConfig(t)
	first, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Application = first.Addr().String()
	_ = first.Close()
	cfg.Peer = "127.0.0.1:0"
	cfg.Diagnostics = "127.0.0.1:0"
	cfg.Services = nil
	cfg.Routes = nil
	for i := 0; i < 16; i++ {
		name := fmt.Sprintf("local%d", i)
		mongo := &Mongo{URI: testmongo.URI, Database: database, Collection: "records"}
		local := &Local{Mongo: mongo}
		service := Service{Name: name, Local: local}
		cfg.Services = append(cfg.Services, service)
		route := Route{Store: name, Service: name}
		cfg.Routes = append(cfg.Routes, route)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	n, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close(context.Background())
	n.Start()
	families := testmetrics.Scrape(t, n.DiagnosticAddress())
	if got := testmetrics.Series(families); got != 1931 {
		t.Fatal("maximum static series changed", got)
	}
	if testmetrics.Sum(families, "weir_store_executions_total") != 0 {
		t.Fatal("diagnostics executed database work")
	}
	for _, runtime := range n.runtimes {
		runtime.SetOverloaded(true)
	}
	if health(t, n, "/readyz") != 200 {
		t.Fatal("Store overload changed readiness")
	}
	if testmetrics.Series(testmetrics.Scrape(t, n.DiagnosticAddress())) != 1931 {
		t.Fatal("state added series")
	}
	t.Log("maximum legal graph: 16 LocalStores, 2 data listeners, exactly 1931 standard Prometheus series; no synthetic executions")
}
