package mongodb

import (
	"context"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testrecords"
)

func prepareTestRecord(adapter *Adapter, operation *pb.ExecuteRequest) (*execution.Plan, *pb.Failure) {
	return testrecords.Prepare(context.Background(), adapter, adapter.config.Store, operation)
}

// Mock constructors use the same immutable lifetime rail as Open.
func initializeTestLifetime(adapter *Adapter) {
	adapter.lifetime, adapter.cancelLifetime = context.WithCancel(context.Background())
}
