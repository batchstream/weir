package execution

import (
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/protobuf/proto"
	"runtime"
	"strings"
	"testing"
)

func TestRecordPreservesBorrowedRequestAndDecodedPath(t *testing.T) {
	read := &pb.ReadRequest{Resource: "db/records/s:a%2Fb%20%E4%B8%AD"}
	command := &pb.Command{Operation: &pb.Command_Read{Read: read}}
	before := proto.Clone(command)
	record, err := NewRecord("mongo", 73, command)
	if err != nil {
		t.Fatal(err)
	}
	parts := record.Segments()
	if !proto.Equal(before, command) || record.Command() != command || record.Index() != 73 || record.StoreName() != "mongo" || record.Key() != read.Resource || len(parts) != 3 || parts[2] != "s:a/b 中" {
		t.Fatal("record lost input identity or decoded resource", record)
	}
	if record.Bytes() <= proto.Size(command)+2*len(read.Resource)+16*len(parts) {
		t.Fatal("metadata omitted from input charge", record.Bytes())
	}
}

func TestRecordUnsupportedMillionSegmentPathDoesNotAllocateSegmentTree(t *testing.T) {
	read := &pb.ReadRequest{Resource: strings.Repeat("segment/", 1_000_000) + "last"}
	value := &pb.Command_Read{Read: read}
	command := &pb.Command{Operation: value}
	allocations := testing.AllocsPerRun(5, func() {
		record, err := NewRecord("records", 1, command)
		if err != nil || len(record.Segments()) != 4 || record.Command() != command {
			t.Fatal("unsupported arity lost its original request or bounded marker", record, err)
		}
	})
	if allocations > 4 {
		t.Fatal("unsupported resource built a pre-admission segment tree", allocations)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	record, err := NewRecord("records", 1, command)
	runtime.ReadMemStats(&after)
	if err != nil || after.TotalAlloc-before.TotalAlloc >= 64<<10 {
		t.Fatal("unsupported resource allocated a large segment backing array", err, after.TotalAlloc-before.TotalAlloc)
	}
	runtime.KeepAlive(record)
	t.Logf("raw_resource_bytes=%d segments=1000001 preparation_allocations=%.0f allocated_bytes=%d", len(read.Resource), allocations, after.TotalAlloc-before.TotalAlloc)
}
func TestMutationRecordBorrowsDocumentWithoutCopy(t *testing.T) {
	data := []byte(`{"id":"a"}`)
	document := &pb.Document{ContentType: "application/json", Data: data}
	action := &pb.MutateRequest_Put{Put: document}
	mutate := &pb.MutateRequest{Resource: "records/s:a", Action: action}
	command := &pb.Command{Operation: &pb.Command_Mutate{Mutate: mutate}}
	record, err := NewRecord("search", 2, command)
	if err != nil {
		t.Fatal(err)
	}
	if record.Command().GetMutate() != mutate || &record.Command().GetMutate().GetPut().Data[0] != &data[0] {
		t.Fatal("record copied borrowed document")
	}
}
func TestRecordDeepUnsupportedResourceChargesOnlyItsBoundedSegmentHeaders(t *testing.T) {
	read := &pb.ReadRequest{Resource: strings.Repeat("a/", 1500) + "a"}
	command := &pb.Command{Operation: &pb.Command_Read{Read: read}}
	record, err := NewRecord("mongo", 1, command)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Segments()) != 4 || record.Bytes() < proto.Size(command)+2*len(read.Resource)+16*4 {
		t.Fatal("decoded path metadata was not charged", len(record.Segments()), record.Bytes())
	}
}
