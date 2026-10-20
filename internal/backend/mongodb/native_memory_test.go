package mongodb

import (
	"bytes"
	"context"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
)

type nativeMemoryProbe struct {
	limit, reserved, held, ends, reads int
}

func (p *nativeMemoryProbe) BeginIO(context.Context, int) error {
	p.reads++
	return nil
}

func (p *nativeMemoryProbe) EndIO(working, _ int) error {
	p.held = working
	p.ends++
	return nil
}

func (p *nativeMemoryProbe) GrowIO(working int) error {
	if p.limit != 0 && working > p.limit {
		return execution.ErrExecutionMemory
	}
	p.reserved = max(p.reserved, working)
	return nil
}

func TestMongoNativeNegotiatedWireReservationPrecedesAttempt(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(map[bool]string{false: "response", true: "budget_full"}[full], func(t *testing.T) {
			response := bson.D{{Key: "ok", Value: 1}, {Key: "pad", Value: bytes.Repeat([]byte("x"), 8<<20)}}
			deployment := drivertest.NewMockDeployment(response)
			calls := 0
			monitor := &event.CommandMonitor{Started: func(context.Context, *event.CommandStartedEvent) { calls++ }}
			// Preparation deliberately has stale, smaller hello capacity. The
			// selected connection's real 48,000,000 bound must be reserved before
			// the driver encodes or writes the opaque business command.
			adapter := &Adapter{deployment: deployment, commandMonitor: monitor, maxMessageBytes: 1 << 20}
			initializeTestLifetime(adapter)
			command := bson.D{{Key: "count", Value: "records"}}
			raw, err := bson.Marshal(command)
			if err != nil {
				t.Fatal(err)
			}
			document := &pb.Document{ContentType: "application/bson", Data: raw}
			request := &pb.NativeRequest{Resource: "database", Request: document}
			work, failure := adapter.prepareNative(request)
			if failure != nil {
				t.Fatal(failure)
			}
			if work.WorkingBytes >= nativeWorkingBytes {
				t.Fatal("test did not create stale preparation evidence", work.WorkingBytes)
			}
			probe := &nativeMemoryProbe{}
			if full {
				probe.limit = 2 << 20
			}
			work.Command = testutil.NativeCommand(request)
			work.NativeIO = probe
			capture := &nativeCapture{}
			emit := func(plan *execution.Plan, event *pb.Event) error {
				if probe.ends != 1 || probe.reads != 0 {
					t.Fatal("Mongo output owned backend IO or reacquired it", probe)
				}
				return capture.Emit(plan, event)
			}
			end := adapter.executeNative(t.Context(), work, emit)
			if full {
				if calls != 0 || end.Completion != pb.NativeCompletion_NATIVE_NOT_STARTED || end.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
					t.Fatal("wire capacity failure occurred after the write attempt", calls, end)
				}
				return
			}
			if calls != 1 || end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || probe.reserved < nativeWorkingBytes+2*len(raw)+8192 || work.IOReserve != nativeWorkingBytes+2*protocol.NativeChunk+8192 {
				t.Fatal("negotiated wire upper bound was not reserved exactly before one attempt", calls, end, probe, work.IOReserve)
			}
			if probe.held < len(capture.body.Bytes()) || probe.held >= probe.reserved || probe.held != work.WorkingBytes {
				t.Fatal("retained response lost its actual backing capacity", probe, work.WorkingBytes, capture.body.Len())
			}
		})
	}
}
