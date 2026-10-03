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
	variant := &pb.Command_Read{Read: request}
	call := &pb.Command{Version: 1, Operation: variant}
	work, failure := adapter.PrepareCommand(9, call)
	if failure != nil {
		t.Fatal(failure)
	}
	if request.Resource != "db/records/s:a" || work.Operation.Index != 9 || work.ID != 9 || work.BatchKey != "db.records" {
		t.Fatal("wire Command mutated or association lost", work)
	}
	events := 0
	emit := func(plan *execution.Plan, event *pb.Event) error {
		events++
		if plan != work || event.Version != 1 || event.GetResult().Index != 9 || len(event.GetResult().GetRead().GetDocument().Data) != protocol.MaxDocument {
			t.Fatal("large read/result framing failed", event.GetResult())
		}
		return nil
	}
	feedback := adapter.Execute(context.Background(), []*execution.Plan{work}, emit)
	if events != 1 || feedback != execution.Healthy {
		t.Fatal("large legal record failed", events, feedback)
	}
	request.Resource = "weir://mongo/db/records/s:a"
	if _, failure := adapter.PrepareCommand(10, call); failure == nil {
		t.Fatal("accepted obsolete absolute wire resource")
	}
	request.Resource = "db/records/s:a"
	call.Version = 2
	if _, failure := adapter.PrepareCommand(10, call); failure == nil {
		t.Fatal("accepted unknown payload version")
	}
	call.Version = 1
	if _, failure := adapter.PrepareCommand(0, call); failure == nil {
		t.Fatal("accepted zero ID")
	}
}

func TestRouteLuaPlanKeepsIndependentTransaction(t *testing.T) {
	adapter := &Adapter{config: Config{Store: "mongo"}}
	program := &pb.ProgramTransform{Runtime: "lua.v1", Source: []byte(`return weir.keep()`)}
	form := &pb.Transform_Program{Program: program}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation := &pb.MutateRequest{Resource: "db/records/s:a", Action: action}
	variant := &pb.Command_Mutate{Mutate: mutation}
	call := &pb.Command{Version: 1, Operation: variant}
	work, failure := adapter.PrepareCommand(1, call)
	if failure != nil {
		t.Fatal(failure)
	}
	if !work.Singleton || work.Backend.(*plan).program == nil {
		t.Fatal("Lua could be combined into another request's transaction")
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
	call := &pb.Command{Version: 1, Operation: variant}
	work, failure := adapter.PrepareCommand(1, call)
	if failure != nil {
		t.Fatal(failure)
	}
	work.BackendTimeout = 50 * time.Millisecond
	var end *pb.NativeEnd
	emit := func(_ *execution.Plan, event *pb.Event) error {
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
