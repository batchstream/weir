// Package execution defines the adapter boundary shared by record batches and Native commands.
package execution

import (
	"context"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// Plan is one prepared operation. The Store runtime sets Context before
// execution; adapters use it to observe this caller's cancellation and deadline
// inside a shared batch.
type Plan struct {
	Command *pb.Command
	// Key orders mutations of the same resource from one stream.
	Key string
	// BatchKey groups compatible records into one adapter invocation.
	BatchKey string
	// Bytes is the request size used for queue and batch bounds.
	Bytes int
	// Index is the record's one-based ordinal in its stream.
	Index   uint64
	Context context.Context
	Backend any
}

// Emit sends one NativeHead or NativeChunk event to the caller and returns
// after it was handed to the transport.
type Emit func(*pb.Event) error

type Adapter interface {
	PrepareRecord(context.Context, *Record) (*Plan, *pb.Failure)
	PrepareNative(context.Context, *pb.Command) (*Plan, *pb.Failure)
	// ExecuteRecords runs reads or mutations that share one BatchKey and
	// returns exactly one result event per plan, in order.
	ExecuteRecords(context.Context, []*Plan) []*pb.Event
	// ExecuteNative streams the response head and chunks through Emit and
	// returns the terminal NativeEnd. It performs no backend I/O afterwards.
	ExecuteNative(context.Context, *Plan, Emit) *pb.NativeEnd
	// Close retires owned I/O within the context and reports genuine failures.
	Close(context.Context) error
}
