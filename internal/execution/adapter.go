// Package execution defines the shared, encoding-independent record execution boundary.
package execution

import (
	"context"

	pb "github.com/batchstream/weir/api/weir/v1"
)

type Plan struct {
	Operation          *pb.BulkOperation
	Key, Token         string
	Bytes, ResultBytes int
	Batchable          bool
	// Backend is private to the adapter that prepared this plan.
	Backend any
}

type Feedback uint8

const (
	Neutral Feedback = iota
	Healthy
	Congested
)

type Adapter interface {
	Prepare(*pb.BulkOperation) (*Plan, *pb.Failure)
	Execute(context.Context, []*Plan) ([]*pb.BulkResult, Feedback)
	Close() error
}
