package server

import (
	"context"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRPCMetricsReportInvalidStreamWithoutUnusedTransportFailures(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	stream, err := client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first := readFrame(1, testRequest())
	if err := stream.Send(first); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	invalid := readFrame(3, testRequest())
	if err := stream.Send(invalid); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatal("invalid ordinal did not terminate the native RPC", err)
	}
	waitPeerIdle(t, server)
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(server); err != nil {
		t.Fatal(err)
	}
	metrics := testmetrics.Registry(t, registry)
	labels := map[string]string{"method": "execute", "status": "non_ok"}
	if testmetrics.Sample(metrics, "weir_rpc_completions_total", labels).GetCounter().GetValue() != 1 {
		t.Fatal("native RPC failure missing from completion status metrics")
	}
	if metrics["weir_transport_failures_total"] != nil {
		t.Fatal("collector exposes an unobserved transport failure counter")
	}
	for _, name := range []string{"weir_watchdog_expirations_total", "weir_transport_forced_closes_total"} {
		if metrics[name] == nil {
			t.Fatal("collector lost observed transport metrics", name)
		}
	}
}
