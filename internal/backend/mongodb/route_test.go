package mongodb

import (
	"context"
	"strings"
	"testing"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
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
	request := &pb.ReadRequest{Resource: "db/records/s:a"}
	variant := &pb.Call_Read{Read: request}
	call := &pb.Call{Version: 1, Operation: variant}
	work, failure := adapter.PrepareCall(9, call)
	if failure != nil {
		t.Fatal(failure)
	}
	if request.Resource != "db/records/s:a" || work.Operation.Index != 9 || work.ID != 9 || work.BatchKey != "db.records" {
		t.Fatal("wire Call mutated or association lost", work)
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
	if _, failure := adapter.PrepareCall(10, call); failure == nil {
		t.Fatal("accepted obsolete absolute wire resource")
	}
	request.Resource = "db/records/s:a"
	call.Version = 2
	if _, failure := adapter.PrepareCall(10, call); failure == nil {
		t.Fatal("accepted unknown payload version")
	}
	call.Version = 1
	if _, failure := adapter.PrepareCall(0, call); failure == nil {
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
	variant := &pb.Call_Mutate{Mutate: mutation}
	call := &pb.Call{Version: 1, Operation: variant}
	work, failure := adapter.PrepareCall(1, call)
	if failure != nil {
		t.Fatal(failure)
	}
	if !work.Singleton || work.Backend.(*plan).program == nil {
		t.Fatal("Lua could be combined into another request's transaction")
	}
}
