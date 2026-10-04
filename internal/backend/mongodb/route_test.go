package mongodb

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestRouteCallRelativeTargetAndLargeRead(t *testing.T) {
	document := bson.D{{Key: "_id", Value: "a"}, {Key: "pad", Value: ""}}
	empty, _ := bson.Marshal(document)
	document[1].Value = strings.Repeat("x", protocol.MaxDocument-len(empty))
	raw, err := bson.Marshal(document)
	if err != nil || len(raw) != protocol.MaxDocument {
		t.Fatal("invalid large fixture", len(raw), err)
	}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document}}}
	replies := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor)}
	adapter := batchMockAdapter(t, replies, nil)
	adapter.config.MaxReadSize = protocol.MaxDocument
	request := &pb.ReadRequest{Resource: "db/records/s:a"}
	items := make([]*pb.ReadRequest, 9)
	for i := range items {
		items[i] = request
	}
	batch := &pb.ReadBatch{Requests: items}
	records, failure := execution.NewReadRecords("mongo", batch.Requests, execution.BackendBatchBytes)
	if failure != nil {
		t.Fatal(failure)
	}
	work, failure := adapter.PrepareRecord(records[8])
	if failure != nil {
		t.Fatal(failure)
	}
	if request.Resource != "db/records/s:a" || work.Operation.Index != 9 || work.ID != 9 || work.BatchKey != "db.records" {
		t.Fatal("wire Command mutated or association lost", work)
	}
	events := 0
	emit := func(plan *execution.Plan, output *execution.Output) error {
		events++
		if plan != work || output.Result.Index != 9 || len(output.Result.Read.GetDocument().Data) != protocol.MaxDocument {
			t.Fatal("large read/result framing failed", output.Result)
		}
		return nil
	}
	feedback := adapter.Execute(context.Background(), []*execution.Plan{work}, emit)
	if events != 1 || feedback != execution.Healthy {
		t.Fatal("large legal record failed", events, feedback)
	}
	request.Resource = "weir://mongo/db/records/s:a"
	if _, err := execution.NewReadRecords("mongo", batch.Requests, execution.BackendBatchBytes); err == nil {
		t.Fatal("accepted absolute wire resource")
	}
	request.Resource = "db/records/s:a"
	emptyRecord := &execution.Record{}
	if _, failure := adapter.PrepareRecord(emptyRecord); failure == nil {
		t.Fatal("accepted unconstructed record")
	}
}

func TestRouteLuaPlanSharesBoundedTransaction(t *testing.T) {
	adapter := &Adapter{config: Config{Store: "mongo"}}
	program := &pb.ProgramTransform{Runtime: "lua.v1", Source: []byte(`return weir.keep()`)}
	form := &pb.Transform_Program{Program: program}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation := &pb.MutateRequest{Resource: "db/records/s:a", Action: action}
	batch := &pb.MutationBatch{Requests: []*pb.MutateRequest{mutation}}
	records, failure := execution.NewMutationRecords("mongo", batch.Requests, execution.BackendBatchBytes)
	if failure != nil {
		t.Fatal(failure)
	}
	work, failure := adapter.PrepareRecord(records[0])
	if failure != nil {
		t.Fatal(failure)
	}
	if work.Singleton || work.Backend.(*plan).program == nil || work.BatchKey != "db.records/lua" || work.WorkingBytes != programWorkingBytes {
		t.Fatal("Lua plan did not declare bounded transaction batching")
	}
}

func TestRouteNativeBackendBudgetExcludesOutputStall(t *testing.T) {
	response := bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 0}}
	replies := []bson.D{collectionQualificationResponse("db", "records"), response}
	adapter := batchMockAdapter(t, replies, nil)
	command := bson.D{{Key: "count", Value: "records"}}
	raw, err := bson.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := &pb.Document{MediaType: NativeDescriptor}
	open := &pb.NativeOpen{Resource: "db/records", Descriptor_: descriptor, BodyMediaType: "application/bson"}
	native := &pb.NativeRequest{Open: open, Body: raw}
	variant := &pb.Command_Native{Native: native}
	call := &pb.Command{Operation: variant}
	work, failure := adapter.PrepareCommand(1, call)
	if failure != nil {
		t.Fatal(failure)
	}
	work.BackendTimeout = 50 * time.Millisecond
	var end *pb.NativeEnd
	emit := func(_ *execution.Plan, output *execution.Output) error {
		event := output.Event
		if event.GetHead() != nil || event.GetChunk() != nil {
			time.Sleep(75 * time.Millisecond)
		}
		if event.GetNativeEnd() != nil {
			end = event.GetNativeEnd()
		}
		return nil
	}
	started := time.Now()
	adapter.Execute(context.Background(), []*execution.Plan{work}, emit)
	if end.GetCompletion() != pb.NativeCompletion_RESPONSE_COMPLETE || end.GetFailure() != nil || time.Since(started) < 3*work.BackendTimeout {
		t.Fatal("output stalls consumed backend I/O budget", end, time.Since(started))
	}
}
