package execution

import (
	"bytes"
	"runtime"
	"strings"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/protobuf/proto"
)

func TestReadRecordsPreserveBorrowedRequestAndDecodedPath(t *testing.T) {
	first := &pb.ReadRequest{Resource: "db/records/s:a%2Fb%20%E4%B8%AD"}
	second := &pb.ReadRequest{Resource: first.Resource}
	request := &pb.ReadBatchRequest{StoreName: "mongo", Requests: []*pb.ReadRequest{first, second}}
	before := proto.Clone(request)
	records, err := NewReadRecords(request, protocol.MaxBatchRequestBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(before, request) || len(records) != 2 {
		t.Fatal("constructor changed the public request")
	}
	for i, record := range records {
		parts := record.Segments()
		if len(parts) != 3 || parts[0] != "db" || parts[1] != "records" || parts[2] != "s:a/b 中" || record.StoreName() != "mongo" || record.Key() != first.Resource || record.Operation().Index != uint64(i+1) || record.Operation().Read != request.Requests[i] {
			t.Fatal("record lost decoded path, ordinal or immutable request ownership", record)
		}
		if record.Bytes() <= record.Operation().RequestBytes()+2*len(record.Key()) {
			t.Fatal("record omitted retained metadata from its memory charge", record.Bytes())
		}
	}
}

func TestMutationRecordsBorrowDocumentAndMaintainInputOrder(t *testing.T) {
	data := []byte(`{"id":"a"}`)
	document := &pb.Document{MediaType: "application/json", Data: data}
	put := &pb.MutateRequest_Put{Put: document}
	first := &pb.MutateRequest{Resource: "records/s:a", Action: put}
	empty := &pb.Empty{}
	remove := &pb.MutateRequest_Delete{Delete: empty}
	second := &pb.MutateRequest{Resource: first.Resource, Action: remove}
	request := &pb.MutateBatchRequest{StoreName: "search", Requests: []*pb.MutateRequest{first, second}}
	before := proto.Clone(request)
	records, err := NewMutationRecords(request, protocol.MaxBatchRequestBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(before, request) || records[0].Operation().Mutate != first || records[1].Operation().Mutate != second || records[0].Operation().Mutate.GetPut() != document || &records[0].Operation().Mutate.GetPut().Data[0] != &data[0] || records[0].Key() != records[1].Key() || records[0].Operation().Index != 1 || records[1].Operation().Index != 2 {
		t.Fatal("constructor copied document data or changed mutation identity/order")
	}
}

func TestRecordConstructorsRejectEntireLateInvalidBatch(t *testing.T) {
	for _, resource := range []string{"", "/records/s:b", "weir://search/records/s:b", "records/%73:b", "records/s:%2f", "records/..", "records/s:%00", "records/s:%FF"} {
		first := &pb.ReadRequest{Resource: "records/s:a"}
		last := &pb.ReadRequest{Resource: resource}
		request := &pb.ReadBatchRequest{StoreName: "search", Requests: []*pb.ReadRequest{first, last}}
		if records, err := NewReadRecords(request, protocol.MaxBatchRequestBytes); err == nil || records != nil {
			t.Fatal("late invalid request produced usable records", resource, records, err)
		}
	}
	empty := &pb.Empty{}
	remove := &pb.MutateRequest_Delete{Delete: empty}
	first := &pb.MutateRequest{Resource: "records/s:a", Action: remove}
	last := &pb.MutateRequest{Resource: "records/s:b"}
	request := &pb.MutateBatchRequest{StoreName: "search", Requests: []*pb.MutateRequest{first, last}}
	if records, err := NewMutationRecords(request, protocol.MaxBatchRequestBytes); err == nil || records != nil {
		t.Fatal("late missing mutation action produced usable records", records, err)
	}
}

func TestRecordConstructorRejectsNestedUnknownFields(t *testing.T) {
	for _, target := range []string{"batch", "request", "document", "action"} {
		t.Run(target, func(t *testing.T) {
			document := &pb.Document{MediaType: "application/json", Data: []byte(`{}`)}
			action := &pb.MutateRequest_Put{Put: document}
			first := &pb.MutateRequest{Resource: "records/s:a", Action: action}
			empty := &pb.Empty{}
			remove := &pb.MutateRequest_Delete{Delete: empty}
			last := &pb.MutateRequest{Resource: "records/s:b", Action: remove}
			request := &pb.MutateBatchRequest{StoreName: "search", Requests: []*pb.MutateRequest{first, last}}
			message := proto.Message(request)
			switch target {
			case "request":
				message = last
			case "document":
				message = document
			case "action":
				message = empty
			}
			message.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
			if records, err := NewMutationRecords(request, protocol.MaxBatchRequestBytes); err == nil || records != nil {
				t.Fatal("unknown field passed the sole protocol boundary", records, err)
			}
		})
	}
}

func TestReadRecordRelativePathAndBatchByteBounds(t *testing.T) {
	store := strings.Repeat("s", 63)
	resource := strings.Repeat("a", protocol.MaxResourceBytes)
	read := &pb.ReadRequest{Resource: resource}
	request := &pb.ReadBatchRequest{StoreName: store, Requests: []*pb.ReadRequest{read}}
	if _, err := NewReadRecords(request, protocol.MaxBatchRequestBytes); err != nil {
		t.Fatal("legal relative resource boundary rejected", err)
	}
	read.Resource += "a"
	if records, err := NewReadRecords(request, protocol.MaxBatchRequestBytes); err == nil || records != nil {
		t.Fatal("relative resource length exceeded its bound", records, err)
	}
	options := &pb.Document{MediaType: "application/json", Data: bytes.Repeat([]byte("x"), protocol.MaxDocument)}
	read = &pb.ReadRequest{Resource: "records/s:a", AdapterOptions: options}
	items := make([]*pb.ReadRequest, 16)
	for i := range items {
		items[i] = read
	}
	request = &pb.ReadBatchRequest{StoreName: "search", Requests: items}
	if proto.Size(request) <= protocol.MaxBatchRequestBytes {
		t.Fatal("invalid oversized batch fixture")
	}
	if records, err := NewReadRecords(request, protocol.MaxBatchRequestBytes); err == nil || records != nil {
		t.Fatal("batch encoded byte bound was lost", records, err)
	}
}

func TestReadRecordsRejectDecodedPathMetadataBeforeAllocation(t *testing.T) {
	read := &pb.ReadRequest{Resource: strings.Repeat("a/", 1500) + "a"}
	items := make([]*pb.ReadRequest, 1200)
	for i := range items {
		items[i] = read
	}
	request := &pb.ReadBatchRequest{StoreName: "mongo", Requests: items}
	if err := protocol.ValidateReadBatchRequest(request); err != nil {
		t.Fatal("metadata fixture must fit the public wire contract", err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	records, failure := NewReadRecords(request, protocol.MaxBatchRequestBytes)
	runtime.ReadMemStats(&after)
	if failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || records != nil {
		t.Fatal("retained decoded path metadata exceeded preparation budget", records, failure)
	}
	// Retaining all 1.8 million decoded segment headers would allocate over
	// 27 MiB. Validation and the size preflight must avoid that allocation.
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 8<<20 {
		t.Fatal("metadata budget was checked after allocating decoded paths", allocated)
	}
	read = &pb.ReadRequest{Resource: "db/records/s:a"}
	request = &pb.ReadBatchRequest{StoreName: "mongo", Requests: []*pb.ReadRequest{read}}
	if records, failure := NewReadRecords(request, 1); records != nil || failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("constructor ignored the actual Store pending-byte bound", records, failure)
	}
}

func BenchmarkReadRecords32(b *testing.B) {
	items := make([]*pb.ReadRequest, 32)
	for i := range items {
		items[i] = &pb.ReadRequest{Resource: "db/records/s:item"}
	}
	request := &pb.ReadBatchRequest{StoreName: "mongo", Requests: items}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := NewReadRecords(request, protocol.MaxBatchRequestBytes); err != nil {
			b.Fatal(err)
		}
	}
}
