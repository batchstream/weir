package server

import (
	"runtime"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestResponseCodecReservesActualMinimumCapacityBeforeAllocation(t *testing.T) {
	fixture := newWireLifetimeFixture(t)
	read := protocol.Missing()
	value := &pb.Event_ReadResult{ReadResult: read}
	event := &pb.Event{Value: value}
	response := &pb.ExecuteResponse{Index: 1, Event: event}
	codec := &responseCodec{admission: fixture.server.admission}
	encoded, err := codec.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := fixture.server.admission.memory.Snapshot()
	if snapshot.WireCount != 1 || snapshot.WireBytes != execution.MinimumWireBytes || snapshot.UsedBytes < snapshot.WireBytes || snapshot.Count != 1 {
		t.Fatal("small protobuf escaped its actual 1025-byte allocation", snapshot)
	}
	encoded.Free()
	if snapshot := fixture.server.admission.memory.Snapshot(); snapshot.UsedBytes != 0 || snapshot.Count != 0 || snapshot.WireBytes != 0 || snapshot.WireCount != 0 || snapshot.InputUsedBytes != 0 {
		t.Fatal("native Free failed to retire a bounded unadmitted response", snapshot)
	}
}

func TestResponseCodecRefusesLargeEncodeBeforeAllocatingWithoutMemory(t *testing.T) {
	fixture := newWireLifetimeFixture(t)
	sample := overload.Snapshot{RecoveryAllowanceKnown: true}
	fixture.server.admission.memory.ObserveMemory(sample)
	value := &pb.Event_Chunk{Chunk: make([]byte, 8<<20)}
	event := &pb.Event{Value: value}
	response := &pb.ExecuteResponse{Index: 1, Event: event}
	codec := &responseCodec{admission: fixture.server.admission}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	encoded, err := codec.Marshal(response)
	runtime.ReadMemStats(&after)
	if status.Code(err) != codes.ResourceExhausted || len(encoded) != 0 || after.TotalAlloc-before.TotalAlloc >= 1<<20 {
		t.Fatal("codec allocated the unreserved full response before its capacity check", err, after.TotalAlloc-before.TotalAlloc)
	}
	if snapshot := fixture.server.admission.memory.Snapshot(); snapshot.UsedBytes != 0 || snapshot.Count != 0 || snapshot.WireBytes != 0 || snapshot.WireCount != 0 {
		t.Fatal("failed codec admission created unbounded output ownership", snapshot)
	}
}
