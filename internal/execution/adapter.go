// Package execution defines the adapter boundary shared by record batches and streaming commands.
package execution

import (
	"context"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

type Plan struct {
	ID                 uint64
	Command            *pb.Command
	Key, BatchKey      string
	Bytes, ResultBytes int
	// Native adapters update WorkingBytes before each serial Emit to describe
	// the response working set they still retain. Emit synchronizes accounting.
	WorkingBytes int
	Context      context.Context
	Backend      any
}

// Emit borrows an event until it returns. Callers must not mutate its contents.
// One caller's canceled emission does not cancel other members of a shared batch.
// Native emissions are serial. NativeEnd is the final emission; the adapter
// performs no more backend I/O after it and returns from Execute.
type Emit func(*Plan, *pb.Event) error

type Adapter interface {
	PrepareCommand(context.Context, uint64, *pb.Command) (*Plan, *pb.Failure)
	PrepareRecord(context.Context, *Record) (*Plan, *pb.Failure)
	// Record batches contain either reads or mutations with the same BatchKey.
	// Native commands execute individually and stream through Emit.
	Execute(context.Context, []*Plan, Emit)
	Close() error
}
