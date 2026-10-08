package mongodb

import (
	"context"
	"strings"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoReadUsesOnlyProtocolDocumentBound(t *testing.T) {
	sizes := []int{1024, (16 << 10) + 1, 70 << 10, protocol.MaxDocument, protocol.MaxDocument + 1}
	for _, size := range sizes {
		document := bson.D{{Key: "_id", Value: "read"}, {Key: "pad", Value: ""}}
		empty := expressionBSON(t, document)
		document[1].Value = strings.Repeat("x", size-len(empty))
		readCursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document}}}
		item := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
		writeCursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: bson.A{item}}}
		writeReply := bson.D{
			{Key: "ok", Value: 1}, {Key: "cursor", Value: writeCursor},
			{Key: "nErrors", Value: 0}, {Key: "nInserted", Value: 0},
			{Key: "nDeleted", Value: 0}, {Key: "nMatched", Value: 1},
			{Key: "nModified", Value: 1}, {Key: "nUpserted", Value: 0},
		}
		responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(readCursor), writeReply}
		adapter := batchMockAdapter(t, responses, nil)

		writeDocument := bson.D{{Key: "_id", Value: "write"}, {Key: "pad", Value: strings.Repeat("x", 2048)}}
		readOptions := batchOperationOptions{resource: "db/records/s:read", action: "read", index: 9}
		writeOptions := batchOperationOptions{resource: "db/records/s:write", action: "put", index: 8, document: writeDocument}
		var plans []*execution.Plan
		for _, opts := range []batchOperationOptions{readOptions, writeOptions} {
			work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
			if failure != nil {
				t.Fatal("read boundary affected mutation admission", failure)
			}
			plans = append(plans, work)
		}
		results := adapter.executeRecords(context.Background(), plans)
		read := results[0].GetReadResult()
		if size <= protocol.MaxDocument && len(read.GetDocument().Data) != size || size > protocol.MaxDocument && read.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
			t.Fatal("read size boundary was not enforced", size, read)
		}
		if results[1].GetMutationResult().Outcome != pb.MutationOutcome_APPLIED || results[1].GetMutationResult().Failure != nil {
			t.Fatal("acknowledged write was affected by a read limit", results[1])
		}
	}
}
