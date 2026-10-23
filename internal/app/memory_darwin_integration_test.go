//go:build integration && darwin

package app

import (
	"context"
	"os"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDarwinAutomaticMemoryApplication(t *testing.T) {
	if os.Getenv("WEIR_MEMORY_DARWIN_NATIVE") != "1" {
		t.Skip("explicit Darwin memory native fixture required")
	}
	fixture := testmongo.OpenSecure(t)
	cfg := recordsStoreConfig(t, fixture.URI)
	n := secureNode(t, cfg)
	client := endpointProcessClient(t, n.Addresses()[0])
	checkRecordCalls(t, client, fixture)
	snapshot := n.guard.Snapshot()
	if snapshot.Budget == 0 || snapshot.Unknown || snapshot.Latched || !snapshot.ProcessValid {
		t.Fatal("automatic memory capacity or observation unavailable", snapshot)
	}
	darwinMetrics(t, n.DiagnosticAddress())
}

func darwinMetrics(t *testing.T, address string) {
	t.Helper()
	metrics := testmetrics.Scrape(t, address)
	labels := map[string]string{"source": "darwin_phys_footprint"}
	footprint := testmetrics.Sample(metrics, "weir_memory_sample_bytes", labels).GetGauge().GetValue()
	if footprint <= 0 || testmetrics.Sum(metrics, "weir_memory_process_valid") != 1 || testmetrics.Sum(metrics, "weir_memory_unknown") != 0 {
		t.Fatal("Darwin memory metrics", footprint)
	}
	for _, source := range []string{"unobserved", "linux_rss", "go_sys_minus_released"} {
		labels := map[string]string{"source": source}
		if testmetrics.Sample(metrics, "weir_memory_sample_bytes", labels).GetGauge().GetValue() != 0 {
			t.Fatal("inactive source nonzero", source)
		}
	}
	labels = map[string]string{"state": "not_applicable"}
	if testmetrics.Sample(metrics, "weir_memory_cgroup_state", labels).GetGauge().GetValue() != 1 || testmetrics.Sum(metrics, "weir_memory_cgroup_valid") != 0 {
		t.Fatal("Darwin cgroup")
	}
	t.Logf("darwin_phys_footprint=%g bytes", footprint)
}

// checkRecordCalls exercises Read, Mutate and Bulk, readback and a cancelled
// batch against fixture.
func checkRecordCalls(t *testing.T, client pb.StoreServiceClient, fixture *testmongo.SecureFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	request := budgetPut(fixture.DB+"/records", "record")
	for mode := 0; mode < 3; mode++ {
		success := budgetLoadCall(ctx, client, testutil.RecordRequest("records", request), mode)
		if !success {
			t.Fatal("Read/Mutate/Bulk", mode)
		}
	}
	read := &pb.ReadRequest{Resource: request.Resource}
	recordResult2, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("records", read))
	result := recordResult2.GetReadResult()
	if err != nil || result.GetDocument() == nil {
		t.Fatal("readback", err)
	}
	cancelled, stop := context.WithCancel(ctx)
	cancelRequest := budgetPut(fixture.DB+"/records", "cancelled-record")
	batch := []*pb.MutateRequest{cancelRequest}
	stop()
	cancelResponse, err := testutil.MutateRecords(cancelled, client, "records", batch)
	if status.Code(err) != codes.Canceled || cancelResponse != nil {
		t.Fatal("batch cancellation", cancelResponse, err)
	}
	filter := bson.D{{Key: "_id", Value: "record"}}
	count, err := fixture.Admin.Database(fixture.DB).Collection("records").CountDocuments(ctx, filter)
	if err != nil || count != 1 {
		t.Fatal("independent backend read", count, err)
	}
	filter = bson.D{{Key: "_id", Value: "cancelled-record"}}
	count, err = fixture.Admin.Database(fixture.DB).Collection("records").CountDocuments(ctx, filter)
	if err != nil || count != 0 {
		t.Fatal("cancelled batch reached database", count, err)
	}
}
