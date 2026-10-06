// Package execution defines the adapter boundary shared by record batches and streaming commands.
package execution

import (
	"context"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// DefaultMaxReadSize bounds ordinary Record Read sources when no limit is set.
const DefaultMaxReadSize = 16 << 10

type Plan struct {
	ID                               uint64
	Command                          *pb.Command
	Key, BatchKey                    string
	Bytes, ResultBytes, WorkingBytes int
	Context                          context.Context
	BackendTimeout                   time.Duration
	Backend                          any
}

// Emit borrows an event until it returns. Callers must not mutate its contents.
// One caller's canceled emission does not cancel other members of a shared batch.
type Emit func(*Plan, *pb.Event) error

type Adapter interface {
	PrepareCommand(uint64, *pb.Command) (*Plan, *pb.Failure)
	PrepareRecord(*Record) (*Plan, *pb.Failure)
	// Execute returns true only for a nonterminal singleton Scan step.
	Execute(context.Context, []*Plan, Emit) bool
	ClosePlan(context.Context, *Plan) *pb.Failure
	Close() error
}

type ScanPage struct {
	Documents             []*pb.Document
	Exhausted             bool
	Complete              bool
	NextContinuationToken []byte
	Failure               *pb.Failure
}
