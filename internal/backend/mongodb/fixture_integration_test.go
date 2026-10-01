//go:build integration

package mongodb

import (
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/event"
)

type adapterTestOptions struct {
	fixture *testmongo.Fixture
	uri     string
	monitor *event.CommandMonitor
}

func testAdapter(t *testing.T, o adapterTestOptions) *Adapter {
	t.Helper()
	if o.uri == "" {
		o.uri = o.fixture.URI
	}
	if o.monitor != nil {
		proxy := testmongo.StartProxy(t, o.fixture)
		proxy.Monitor = o.monitor
		o.uri = proxy.URI()
	}
	cfg := Config{URI: o.uri, Store: "mongo", Pool: 4}
	cfg = mongoFixtureConfig(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	})
	return a
}
func prepareCounter(t *testing.T, a *Adapter, resource string) *execution.Plan {
	t.Helper()
	r := &pb.ReadRequest{Resource: resource}
	v := &pb.Operation_Read{Read: r}
	op := &pb.Operation{Operation: v}
	p, f := a.prepareRecord(op)
	if f != nil {
		t.Fatal(f)
	}
	return p
}
