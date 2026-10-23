package mongodb

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestRouteCallRelativeTargetAndLargeRead(t *testing.T) {
	document := bson.D{{Key: "_id", Value: "a"}, {Key: "pad", Value: ""}}
	empty, _ := bson.Marshal(document)
	document[1].Value = strings.Repeat("x", (2<<20)-len(empty))
	raw, err := bson.Marshal(document)
	if err != nil || len(raw) != (2<<20) {
		t.Fatal("invalid large fixture", len(raw), err)
	}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document}}}
	replies := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor)}
	adapter := batchMockAdapter(t, replies, nil)

	request := &pb.ReadRequest{Resource: "db/records/s:a"}
	operation := &pb.Command_Read{Read: request}
	command := &pb.Command{Operation: operation}
	record, err := execution.NewRecord("mongo", 9, command)
	if err != nil {
		t.Fatal(err)
	}
	work, failure := adapter.PrepareRecord(context.Background(), record)
	if failure != nil {
		t.Fatal(failure)
	}
	if request.Resource != "db/records/s:a" || work.BatchKey != "db.records" {
		t.Fatal("wire Command mutated", work)
	}
	events := adapter.ExecuteRecords(context.Background(), []*execution.Plan{work})
	if len(events) != 1 || len(events[0].GetReadResult().GetDocument().Data) != (2<<20) {
		t.Fatal("large legal record failed", events)
	}
	emptyRecord := &execution.Record{}
	if _, failure := adapter.PrepareRecord(context.Background(), emptyRecord); failure == nil {
		t.Fatal("accepted unconstructed record")
	}
}

func TestRouteLuaPlanSharesBoundedTransaction(t *testing.T) {
	adapter := &Adapter{config: Config{Store: "mongo"}}
	initializeTestLifetime(adapter)
	program := &pb.LuaTransform{Source: []byte(`return function(current, incoming) return weir.keep() end`)}
	form := &pb.Transform_Lua{Lua: program}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation := &pb.MutateRequest{Resource: "db/records/s:a", Action: action}
	operation := &pb.Command_Mutate{Mutate: mutation}
	command := &pb.Command{Operation: operation}
	record, err := execution.NewRecord("mongo", 1, command)
	if err != nil {
		t.Fatal(err)
	}
	work, failure := adapter.PrepareRecord(context.Background(), record)
	if failure != nil {
		t.Fatal(failure)
	}
	if work.Command.GetNative() != nil || work.Backend.(*plan).program == nil || work.BatchKey != "db.records/lua" {
		t.Fatal("Lua plan did not declare bounded transaction batching")
	}
}

func TestRouteNativeBackendDeadlineExcludesOutputStall(t *testing.T) {
	response := bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 0}}
	replies := []bson.D{response}
	adapter := batchMockAdapter(t, replies, nil)
	command := bson.D{{Key: "count", Value: "records"}}
	raw, err := bson.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	nativeBody := &pb.Document{ContentType: "application/bson", Data: []byte{5, 0, 0, 0, 0}}
	open := &pb.NativeRequest{Resource: "db", Request: nativeBody}
	open.Request.Data = raw
	native := open
	variant := &pb.Command_Native{Native: native}
	call := &pb.Command{Operation: variant}
	work, failure := adapter.PrepareNative(context.Background(), call)
	if failure != nil {
		t.Fatal(failure)
	}

	emit := func(event *pb.Event) error {
		time.Sleep(75 * time.Millisecond)
		return nil
	}
	started := time.Now()
	end := adapter.ExecuteNative(context.Background(), work, emit)
	if end.GetCompletion() != pb.NativeCompletion_RESPONSE_COMPLETE || end.GetFailure() != nil || time.Since(started) < 150*time.Millisecond {
		t.Fatal("output stalls consumed backend deadline", end, time.Since(started))
	}
}

func TestRouteNativeDeliveryFailureEndsIncomplete(t *testing.T) {
	for _, failChunk := range []bool{false, true} {
		name := "complete"
		if failChunk {
			name = "delivery_failure"
		}
		t.Run(name, func(t *testing.T) {
			response := bson.D{{Key: "ok", Value: 1}, {Key: "pad", Value: strings.Repeat("x", 1<<20)}}
			replies := []bson.D{response}
			adapter := batchMockAdapter(t, replies, nil)
			command := bson.D{{Key: "ping", Value: 1}}
			raw, err := bson.Marshal(command)
			if err != nil {
				t.Fatal(err)
			}
			body := &pb.Document{ContentType: "application/bson", Data: raw}
			request := &pb.NativeRequest{Resource: "db", Request: body}
			variant := &pb.Command_Native{Native: request}
			call := &pb.Command{Operation: variant}
			work, failure := adapter.PrepareNative(t.Context(), call)
			if failure != nil {
				t.Fatal(failure)
			}
			chunks := 0
			emit := func(event *pb.Event) error {
				if event.GetChunk() != nil {
					chunks++
					if failChunk {
						return errors.New("test output canceled")
					}
				}
				return nil
			}
			end := adapter.ExecuteNative(t.Context(), work, emit)
			if chunks == 0 {
				t.Fatal("Native reply sent no chunk")
			}
			if failChunk && end.Completion != pb.NativeCompletion_RESPONSE_INCOMPLETE || !failChunk && end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE {
				t.Fatal("delivery outcome changed Native completion evidence", end)
			}
		})
	}
}
