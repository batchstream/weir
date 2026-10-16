// Package execution defines the adapter boundary shared by record batches and streaming commands.
package execution

import (
	"context"
	"errors"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

var ErrExecutionMemory = errors.New("execution working memory capacity exhausted")

// Memory reserves adapter allocations before use. SetWorking describes the
// entire shared batch, not one record. ReserveResult describes this record's
// retained allocation, excluding the wire publication reserved by the runtime.
// Both may shrink after the corresponding references have actually been cleared.
type Memory interface {
	SetWorking(int) error
	ReserveResult(int) error
}

// NativeIO is the runtime's serial Native lifecycle boundary. GrowIO reserves
// total working bytes before allocation. EndIO transfers that reservation to
// held working bytes and an encoded event window, releasing the backend slot.
// BeginIO reacquires FIFO execution credit and a transient allocation window.
type NativeIO interface {
	BeginIO(context.Context, int) error
	EndIO(int, int) error
	GrowIO(int) error
}

type Plan struct {
	ID                 uint64
	Command            *pb.Command
	Key, BatchKey      string
	Bytes, ResultBytes int
	// Native adapters update WorkingBytes before each serial Emit to describe
	// the response working set they still retain. Emit synchronizes accounting.
	WorkingBytes int
	IOReserve    int
	NativeIO     NativeIO
	Memory       Memory
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
