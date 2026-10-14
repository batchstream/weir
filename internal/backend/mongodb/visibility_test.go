package mongodb

import (
	"context"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoVisibilityUsesMajorityAcknowledgementForEitherOption(t *testing.T) {
	for _, wait := range []bool{false, true} {
		item := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
		cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: bson.A{item}}}
		reply := bson.D{
			{Key: "ok", Value: 1}, {Key: "cursor", Value: cursor},
			{Key: "nErrors", Value: 0}, {Key: "nInserted", Value: 0},
			{Key: "nDeleted", Value: 0}, {Key: "nMatched", Value: 1},
			{Key: "nModified", Value: 1}, {Key: "nUpserted", Value: 0},
		}
		writes := 0
		monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
			if e.CommandName == "bulkWrite" {
				writes++
				concern := e.Command.Lookup("writeConcern").Document()
				if concern.Lookup("w").StringValue() != "majority" {
					t.Error("mutation visibility requires the existing majority acknowledgement")
				}
			}
		}}
		responses := []bson.D{collectionQualificationResponse("db", "records"), reply}
		adapter := batchMockAdapter(t, responses, monitor)
		document := bson.D{{Key: "_id", Value: "item"}, {Key: "n", Value: int32(2)}}
		opts := batchOperationOptions{resource: "db/records/s:item", action: "put", document: document}
		request := batchOperation(t, opts)
		if wait {
			options := &pb.MutationOptions{Visibility: pb.MutationVisibility_SEARCH_VISIBLE}
			request.Command.GetMutate().Options = options
		}
		work, failure := prepareTestRecord(adapter, request)
		if failure != nil {
			t.Fatal("visibility option unsupported", wait, failure)
		}
		works := []*execution.Plan{work}
		mutation := adapter.executeRecords(t.Context(), works)[0].GetMutationResult()
		if mutation.GetOutcome() != pb.MutationOutcome_APPLIED || mutation.GetFailure() != nil || writes != 1 {
			t.Fatal("majority acknowledgement changed with visibility option", wait, mutation, writes)
		}
	}
}
