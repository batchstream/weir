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
	// Scan reserves a separate native page/decoder budget for its entire session.
	Scan      bool
	PageBytes int
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
	PrepareScan(*pb.ScanRequest) (*Plan, *pb.Failure)
	FetchScan(context.Context, *Plan) (*ScanPage, Feedback)
	CloseScan(context.Context, *Plan) *pb.Failure
	Close() error
}

// A page is completely validated before publication. Only positive native
// exhaustion evidence permits Exhausted=true. Failure pages contain no documents.
type ScanPage struct {
	Documents []*pb.Document
	Exhausted bool
	Failure   *pb.Failure
}
