package app

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestEmptyStoreLists(t *testing.T) {
	for _, stores := range []string{"", "stores: []\n", "stores: null\n"} {
		cfg, err := Decode(strings.NewReader("listeners:\n  application: 127.0.0.1:0\n" + stores))
		if err != nil || len(cfg.Stores) != 0 {
			t.Fatal("empty Store list rejected", err)
		}
	}
}

func TestStorelessNodeLifecycleAndUnknownStore(t *testing.T) {
	cfg := emptyConfig(t)
	cfg.Listeners.Diagnostics = "127.0.0.1:0"
	node, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if len(node.stores) != 0 || node.guard.Snapshot().Live == 0 {
		t.Fatal("an empty graph must construct admission only, with no backend or remote state")
	}
	if node.ready() {
		t.Fatal("Store-less node is ready before startup")
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if health(t, node, "/readyz") != 200 || health(t, node, "/livez") != 200 || health(t, node, "/metrics") != 200 {
		t.Fatal("Store-less node did not serve diagnostics after startup")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request := &pb.ReadRequest{Resource: "records/s:key"}
	connection, err := grpc.NewClient("passthrough:///"+node.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	client := pb.NewStoreServiceClient(connection)
	recordResult, readErr := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("missing", request))
	result := recordResult.GetReadResult()
	closeErr := connection.Close()
	if status.Code(readErr) != codes.NotFound || closeErr != nil || result != nil {
		t.Fatal("an unknown Store must be reported as not found", readErr, closeErr)
	}
	if len(node.stores) != 0 || health(t, node, "/readyz") != 200 {
		t.Fatal("unknown Store traffic must not create backends or alter readiness")
	}
	addresses := []string{node.Address(), node.DiagnosticAddress()}
	if err := node.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if node.ready() {
		t.Fatal("closed Store-less node is ready")
	}
	for _, address := range addresses {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatal("Store-less shutdown did not release a listener", err)
		}
		_ = listener.Close()
	}
}
