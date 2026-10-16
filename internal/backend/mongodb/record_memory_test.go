package mongodb

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/value"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

type recordMemoryProbe struct{ limit, resultLimit, peak, result, calls int }

func (p *recordMemoryProbe) SetWorking(bytes int) error {
	if p.limit != 0 && bytes > p.limit {
		return execution.ErrExecutionMemory
	}
	p.peak = max(p.peak, bytes)
	return nil
}
func (p *recordMemoryProbe) ReserveResult(bytes int) error {
	p.calls++
	if p.resultLimit != 0 && bytes > p.resultLimit {
		return execution.ErrExecutionMemory
	}
	p.result = bytes
	return nil
}

func TestMongoSmallReadBatchReservesOneReplyAndActualDocuments(t *testing.T) {
	documents := make(bson.A, 128)
	for i := range documents {
		documents[i] = bson.D{{Key: "_id", Value: fmt.Sprint(i)}, {Key: "pad", Value: strings.Repeat("x", 1024)}}
	}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: documents}}
	responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor)}
	var finds atomic.Int32
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "find" {
			finds.Add(1)
		}
	}}
	adapter := batchMockAdapter(t, responses, monitor)
	plans := make([]*execution.Plan, 128)
	probes := make([]*recordMemoryProbe, 128)
	for i := range plans {
		opts := batchOperationOptions{resource: "db/records/s:" + fmt.Sprint(i), action: "read"}
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		probe := &recordMemoryProbe{limit: 64 << 20}
		work.Memory = probe
		plans[i] = work
		probes[i] = probe
	}
	results := adapter.executeRecords(t.Context(), plans)
	for i, result := range results {
		document := result.GetReadResult().GetDocument()
		if document == nil || bson.Raw(document.Data).Lookup("_id").StringValue() != fmt.Sprint(i) || probes[i].result != len(document.Data)+execution.ResultOverheadBytes {
			t.Fatal(i, result, probes[i])
		}
	}
	if finds.Load() != 1 || probes[0].peak >= 64<<20 {
		t.Fatal("small batch used per-document maximum reply reservations", finds.Load(), probes[0].peak)
	}
}

func TestMongoReadRejectsCapacityBeforeDocumentCopy(t *testing.T) {
	document := bson.D{{Key: "_id", Value: "large"}, {Key: "pad", Value: strings.Repeat("x", 2<<20)}}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document}}}
	responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor)}
	adapter := batchMockAdapter(t, responses, nil)
	opts := batchOperationOptions{resource: "db/records/s:large", action: "read"}
	work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
	if failure != nil {
		t.Fatal(failure)
	}
	probe := &recordMemoryProbe{limit: 64 << 20, resultLimit: 1024}
	work.Memory = probe
	result := adapter.executeRecords(t.Context(), []*execution.Plan{work})[0].GetReadResult()
	if result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || result.GetDocument() != nil || probe.calls != 1 || probe.result != 0 {
		t.Fatal(result, probe)
	}
}

func TestMongoDeferredExpressionValidationFailsBeforeAnyBackendCommand(t *testing.T) {
	var commands atomic.Int32
	monitor := &event.CommandMonitor{Started: func(context.Context, *event.CommandStartedEvent) { commands.Add(1) }}
	adapter := batchMockAdapter(t, nil, monitor)
	invalid := bson.D{{Key: "$rename", Value: bson.D{{Key: "a", Value: "b"}}}}
	operation := expressionOperation("db/records/s:invalid", expressionBSON(t, invalid))
	record, err := execution.NewRecord("mongo", 1, operation.Command)
	if err != nil {
		t.Fatal(err)
	}
	work, failure := adapter.PrepareRecord(t.Context(), record)
	if failure != nil {
		t.Fatal(failure)
	}
	probe := &recordMemoryProbe{limit: 64 << 20}
	work.Memory = probe
	result := adapter.executeRecords(t.Context(), []*execution.Plan{work})[0].GetMutationResult()
	if result.GetOutcome() != pb.MutationOutcome_NOT_STARTED || result.GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT || commands.Load() != 0 || probe.peak == 0 {
		t.Fatal(result, commands.Load(), probe.peak)
	}
}

func TestMongoPreparationBorrowsLargeLuaInputWithoutDecoding(t *testing.T) {
	fields := make(bson.D, 10000)
	for i := range fields {
		fields[i] = bson.E{Key: fmt.Sprint(i), Value: nil}
	}
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	opts := batchOperationOptions{resource: "db/records/s:item", action: "program", program: "return function(current, incoming) return incoming end"}
	operation := batchOperation(t, opts)
	input := &pb.Document{ContentType: "application/bson", Data: expressionBSON(t, fields)}
	operation.Command.GetMutate().GetAtomicTransform().GetLua().Input = input
	record, err := execution.NewRecord("mongo", 1, operation.Command)
	if err != nil {
		t.Fatal(err)
	}
	var work *execution.Plan
	allocations := testing.AllocsPerRun(20, func() {
		var failure *pb.Failure
		work, failure = adapter.PrepareRecord(t.Context(), record)
		if failure != nil {
			t.Fatal(failure)
		}
	})
	if allocations > 20 || work.Backend.(*plan).program.Source != "" || work.Backend.(*plan).program.Input.Kind != value.Missing {
		t.Fatal("unadmitted Lua plan materialized source/input", allocations)
	}
	probe := &recordMemoryProbe{limit: recordWorking([]*execution.Plan{work}) + 1}
	work.Memory = probe
	result := adapter.executeRecords(t.Context(), []*execution.Plan{work})[0].GetMutationResult()
	if result.GetOutcome() != pb.MutationOutcome_NOT_STARTED || result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("input decoded before its allocation was approved", result)
	}
}

func TestMongoMaximumPlainPutValidatesWithoutDisposableValueTree(t *testing.T) {
	document := bson.D{{Key: "_id", Value: "large"}, {Key: "pad", Value: ""}, {Key: "active", Value: true}}
	document[1].Value = strings.Repeat("x", (16<<20)-64-len(expressionBSON(t, document)))
	opts := batchOperationOptions{resource: "db/records/s:large", action: "put", document: document}
	operation := batchOperation(t, opts)
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	record, err := execution.NewRecord("mongo", 1, operation.Command)
	if err != nil {
		t.Fatal(err)
	}
	work, failure := adapter.PrepareRecord(t.Context(), record)
	if failure != nil {
		t.Fatal(failure)
	}
	// At 256 MiB, B is 119.466 MiB and the input promise is 32 MiB.
	probe := &recordMemoryProbe{limit: 87 << 20}
	work.Memory = probe
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	failure = adapter.validateRecord(t.Context(), work)
	runtime.ReadMemStats(&after)
	if failure != nil || probe.peak > probe.limit || after.TotalAlloc-before.TotalAlloc > 256<<10 {
		t.Fatal("plain BSON validation copied/materialized its large scalar", failure, probe.peak, after.TotalAlloc-before.TotalAlloc)
	}
}

func TestMongoPlainPutRawValidationMatchesOriginalDecodeContract(t *testing.T) {
	decimal, err := bson.ParseDecimal128("123456789.0123456789")
	if err != nil {
		t.Fatal(err)
	}
	oid := bson.NewObjectID()
	canonical := bson.D{{Key: "0", Value: "one"}, {Key: "1", Value: int32(2)}}
	noncanonical := bson.D{{Key: "00", Value: "one"}}
	canonicalArray := bson.RawValue{Type: bson.TypeArray, Value: expressionBSON(t, canonical)}
	noncanonicalArray := bson.RawValue{Type: bson.TypeArray, Value: expressionBSON(t, noncanonical)}
	malformedBool := bson.RawValue{Type: bson.TypeBoolean, Value: []byte{2}}
	cases := []struct {
		name     string
		resource string
		document bson.D
		valid    bool
	}{
		{"scalar types", "s:item", bson.D{{Key: "_id", Value: "item"}, {Key: "null", Value: nil}, {Key: "bool", Value: true}, {Key: "int32", Value: int32(1)}, {Key: "int64", Value: int64(1 << 40)}, {Key: "double", Value: 1.25}, {Key: "text", Value: "文档"}}, true},
		{"extended types", "s:item", bson.D{{Key: "_id", Value: "item"}, {Key: "oid", Value: oid}, {Key: "decimal", Value: decimal}, {Key: "binary", Value: bson.Binary{Subtype: 0x80, Data: []byte{0, 255}}}, {Key: "datetime", Value: bson.DateTime(1234)}, {Key: "timestamp", Value: bson.Timestamp{T: 42, I: 3}}, {Key: "regex", Value: bson.Regex{Pattern: "a.*", Options: "i"}}}, true},
		{"duplicate ordinary field", "s:item", bson.D{{Key: "_id", Value: "item"}, {Key: "a", Value: 1}, {Key: "a", Value: 2}}, true},
		{"duplicate nested field", "s:item", bson.D{{Key: "_id", Value: "item"}, {Key: "nested", Value: bson.D{{Key: "a", Value: 1}, {Key: "a", Value: 2}}}}, true},
		{"nonfinite doubles", "s:item", bson.D{{Key: "_id", Value: "item"}, {Key: "nan", Value: math.NaN()}, {Key: "positive infinity", Value: math.Inf(1)}, {Key: "negative infinity", Value: math.Inf(-1)}}, true},
		{"int32 identity", "i:7", bson.D{{Key: "_id", Value: int32(7)}}, true},
		{"int64 identity", "i:7", bson.D{{Key: "_id", Value: int64(7)}}, true},
		{"objectid identity", "oid:" + oid.Hex(), bson.D{{Key: "_id", Value: oid}}, true},
		{"duplicate identity", "s:item", bson.D{{Key: "_id", Value: "item"}, {Key: "_id", Value: "item"}}, false},
		{"identity not first", "s:item", bson.D{{Key: "a", Value: 1}, {Key: "_id", Value: "item"}}, false},
		{"different identity", "s:item", bson.D{{Key: "_id", Value: "other"}}, false},
		{"missing identity", "s:item", bson.D{{Key: "a", Value: 1}}, false},
		{"canonical array", "s:item", bson.D{{Key: "_id", Value: "item"}, {Key: "array", Value: canonicalArray}}, true},
		{"noncanonical array", "s:item", bson.D{{Key: "_id", Value: "item"}, {Key: "array", Value: noncanonicalArray}}, false},
		{"invalid field UTF8", "s:item", bson.D{{Key: "_id", Value: "item"}, {Key: string([]byte{255}), Value: 1}}, false},
		{"invalid scalar UTF8", "s:item", bson.D{{Key: "_id", Value: "item"}, {Key: "text", Value: string([]byte{255})}}, false},
		{"unsupported code", "s:item", bson.D{{Key: "_id", Value: "item"}, {Key: "code", Value: bson.JavaScript("return 1")}}, false},
		{"invalid bool", "s:item", bson.D{{Key: "_id", Value: "item"}, {Key: "bool", Value: malformedBool}}, false},
	}
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := expressionBSON(t, tc.document)
			id, err := parseID(tc.resource)
			if err != nil {
				t.Fatal(err)
			}
			// This is the original plain Put check from adapter.prepareRecord:
			// Decode, then an unambiguous first _id equal to the resource.
			decoded, decodeErr := Decode(t.Context(), raw)
			originalValid := decodeErr == nil
			if originalValid {
				got, lookupErr := decoded.Lookup("_id")
				originalValid = lookupErr == nil && len(decoded.Fields) != 0 && decoded.Fields[0].Name == "_id" && equalID(got, id)
			}
			if originalValid != tc.valid {
				t.Fatal("fixture does not match the original plain Put contract", decodeErr)
			}
			opts := batchOperationOptions{resource: "db/records/" + tc.resource, action: "put", document: tc.document}
			operation := batchOperation(t, opts)
			record, err := execution.NewRecord("mongo", 1, operation.Command)
			if err != nil {
				t.Fatal(err)
			}
			work, failure := adapter.PrepareRecord(t.Context(), record)
			if failure == nil {
				probe := &recordMemoryProbe{limit: 64 << 20}
				work.Memory = probe
				failure = adapter.validateRecord(t.Context(), work)
			}
			if (failure == nil) != originalValid {
				t.Fatal("admitted raw validation changed plain Put acceptance", failure)
			}
			if originalValid && !bytes.Equal(work.Backend.(*plan).document, raw) {
				t.Fatal("plain Put changed document bytes")
			}
		})
	}
}

func TestMongoUnsupportedResourceArityKeepsFourSegmentsAndNoBackendIO(t *testing.T) {
	var commands atomic.Int32
	monitor := &event.CommandMonitor{Started: func(context.Context, *event.CommandStartedEvent) { commands.Add(1) }}
	adapter := batchMockAdapter(t, nil, monitor)
	read := &pb.ReadRequest{Resource: strings.Repeat("part/", 1_000_000) + "last"}
	operation := &pb.Command_Read{Read: read}
	command := &pb.Command{Operation: operation}
	record, err := execution.NewRecord("mongo", 1, command)
	if err != nil {
		t.Fatal(err)
	}
	work, failure := adapter.PrepareRecord(t.Context(), record)
	if work != nil || failure.GetCode() != pb.FailureCode_INVALID_ARGUMENT || len(record.Segments()) != 4 || commands.Load() != 0 {
		t.Fatal("unsupported arity escaped early target validation", failure, len(record.Segments()), commands.Load())
	}
}
