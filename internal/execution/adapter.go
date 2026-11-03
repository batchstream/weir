// Package execution defines the adapter boundary shared by record batches and Native commands.
package execution

import (
	"context"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

// Plan is one prepared record.
type Plan struct {
	Command *pb.Command
	// Key orders mutations of the same resource from one stream.
	Key string
	// BatchKey groups compatible records into one adapter invocation.
	BatchKey string
	// Bytes is the request size used for queue and batch bounds.
	Bytes int
	// Index is the record's one-based ordinal in its stream.
	Index uint64
	// Waits marks a record whose execution also waits for something other
	// than the backend's work on it, such as a scheduled refresh. Its time
	// does not show how loaded the backend is.
	Waits bool
	// Context carries this caller's cancellation and deadline inside a shared
	// batch. The Store runtime replaces the one PrepareRecord set.
	Context context.Context
	Backend any
}

// Ended reports why a record must stop: its caller left or its batch ended.
func (p *Plan) Ended(batch context.Context) *pb.Failure {
	if p.Context.Err() != nil {
		return protocol.ContextFailure(p.Context)
	}
	if batch.Err() != nil {
		return protocol.ContextFailure(batch)
	}
	return nil
}

// Plan returns the record's plan with an adapter's own state.
func (r *Record) Plan(ctx context.Context, backend any) *Plan {
	plan := &Plan{Command: r.Command, Key: r.Key, Bytes: r.Bytes, Index: r.Index, Context: ctx, Backend: backend}
	return plan
}

// Emit sends one NativeHead or NativeChunk event to the caller and returns
// after it was handed to the transport.
type Emit func(*pb.Event) error

type Adapter interface {
	// PrepareRecord validates a record without backend I/O.
	PrepareRecord(context.Context, *Record) (*Plan, *pb.Failure)
	// ExecuteRecords runs one batch and returns exactly one result event per
	// plan, in order. Every plan of a batch shares its BatchKey and is either a
	// read or a mutation, and mutations of a batch name distinct resources.
	ExecuteRecords(context.Context, []*Plan) []*pb.Event
	// ExecuteNative streams the response head and chunks through Emit and
	// returns the terminal NativeEnd. It performs no backend I/O afterwards.
	ExecuteNative(context.Context, *pb.Command, Emit) *pb.NativeEnd
	// Close retires owned I/O within the context and reports genuine failures.
	Close(context.Context) error
}
