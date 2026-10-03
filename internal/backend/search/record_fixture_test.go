package search

import (
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testrecords"
)

func prepareTestRecord(adapter *Adapter, operation *pb.Operation) (*execution.Plan, *pb.Failure) {
	return testrecords.Prepare(adapter, adapter.config.Store, operation)
}
