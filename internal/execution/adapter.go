// Package execution defines the adapter boundary shared by record batches and streaming commands.
package execution

import (
	"context"
	"io"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// DefaultMaxReadSize bounds ordinary Record Read sources when no limit is set.
const DefaultMaxReadSize = 16 << 10

type Plan struct {
	ID                                   uint64
	Command                              *pb.Command
	Operation                            *Operation
	Key, BatchKey                        string
	Bytes, ResultBytes, WorkingBytes     int
	Continue, CleanupRequired, Streaming bool
	Singleton                            bool
	Context                              context.Context
	BackendTimeout                       time.Duration
	Backend                              any
	Results                              *ResultBudget
}

type Feedback uint8

const (
	Neutral Feedback = iota
	Healthy
	Congested
	// Completed records a complete Native transport exchange. Its opaque body
	// makes no assertion about business or write effects.
	Completed
)

// Emit borrows an event until it returns. Callers must not mutate its contents.
// One caller's canceled emission does not cancel other members of a shared batch.
type Output struct {
	Result *Result
	Event  *pb.Event
}

type Emit func(*Plan, *Output) error

type Adapter interface {
	PrepareCommand(uint64, *pb.Command) (*Plan, *pb.Failure)
	PrepareRecord(*Record) (*Plan, *pb.Failure)
	Execute(context.Context, []*Plan, Emit) Feedback
	ClosePlan(context.Context, *Plan) *pb.Failure
	Close() error
}

// These private adapter dependencies model one bounded native exchange.
type NativeExchange struct {
	Source io.ReadCloser
	Sink   NativeSink
}
type NativeSink interface {
	Interrupt()
	Head(*pb.NativeHead) error
	Chunk([]byte) error
}

type ScanPage struct {
	Documents             []*pb.Document
	Exhausted             bool
	Complete              bool
	NextContinuationToken []byte
	Failure               *pb.Failure
}
