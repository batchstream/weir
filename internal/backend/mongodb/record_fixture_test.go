package mongodb

import (
	"context"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testrecords"
)

func prepareTestRecord(adapter *Adapter, operation *pb.ExecuteRequest) (*execution.Plan, *pb.Failure) {
	work, failure := testrecords.Prepare(context.Background(), adapter, adapter.config.Store, operation)
	if failure == nil {
		failure = adapter.validateRecord(context.Background(), work)
	}
	if failure != nil {
		return nil, failure
	}
	return work, nil
}
