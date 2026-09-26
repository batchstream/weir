// Package execution defines the shared, encoding-independent record execution boundary.
package execution

import (
	"context"
	"io"

	pb "github.com/batchstream/weir/api/weir/v1"
)

type Plan struct {
	Operation          *pb.BulkOperation
	Key, Token         string
	Bytes, ResultBytes int
	Batchable          bool
	// Scan and Native reserve their bounded page/exchange working set for the
	// lifetime of the one shared live-session slot.
	Scan      bool
	Native    bool
	Exchange  *NativeExchange
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
	PrepareNative(*pb.NativeOpen) (*Plan, *pb.Failure)
	ExecuteNative(context.Context, *Plan, *NativeExchange) (*pb.NativeEnd, Feedback)
	Close() error
}

// The source and sink are bounded transport dependencies, not backend semantics.
// Close interrupts a blocked input read without canceling a completed response.
type NativeExchange struct {
	Source io.ReadCloser
	Sink   NativeSink
}
type NativeSink interface {
	Interrupt()
	Head(*pb.NativeHead) error
	Chunk([]byte) error
}

// A page is completely validated before publication. Only positive native
// exhaustion evidence permits Exhausted=true. Failure pages contain no documents.
type ScanPage struct {
	Documents []*pb.Document
	Exhausted bool
	Failure   *pb.Failure
}
