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

func TestMongoReadSizeConfigurationAndBudgets(t *testing.T) {
	for _, limit := range []int{0, 1023, 1024, 16 << 10, protocol.MaxDocument, protocol.MaxDocument + 1} {
		config := Config{Store: "mongo", URI: "mongodb://127.0.0.1:27017", Pool: 4, MaxReadSize: limit}
		valid := limit == 0 || limit >= 1024 && limit <= protocol.MaxDocument
		if (ValidateConfig(config) == nil) != valid {
			t.Fatal("invalid maximum read size accepted", limit)
		}
		if !valid {
			continue
		}
		adapter := &Adapter{config: config}
		request := &pb.ReadRequest{Resource: "db/records/s:id"}
		callOperation := &pb.Command_Read{Read: request}
		callCommand := &pb.Command{Operation: callOperation}
		call := &pb.ExecuteRequest{Index: 1, Command: callCommand}
		work, failure := prepareTestRecord(adapter, call)
		if failure != nil {
			t.Fatal(failure)
		}
		if limit == 0 {
			limit = 16 << 10
		}
		if work.ResultBytes != limit+execution.ResultOverheadBytes || work.WorkingBytes != 2*scanNativeLimit+max(1<<20, 4*limit) {
			t.Fatal("read declaration was not reflected in resource bounds", limit, work.ResultBytes, work.WorkingBytes)
		}
	}
}

func TestMongoReadSizeLimitDoesNotConstrainOrMisreportWrites(t *testing.T) {
	cases := []struct {
		limit int
		size  int
	}{
		{1024, 1024}, {1024, 1025},
		{0, 16 << 10}, {0, (16 << 10) + 1},
		{protocol.MaxDocument, (16 << 10) + 1},
	}
	for _, tc := range cases {
		size, limit := tc.size, tc.limit
		if limit == 0 {
			limit = 16 << 10
		}
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
		adapter.config.MaxReadSize = tc.limit
		writeDocument := bson.D{{Key: "_id", Value: "write"}, {Key: "pad", Value: strings.Repeat("x", 2048)}}
		readOptions := batchOperationOptions{resource: "db/records/s:read", action: "read", index: 9}
		writeOptions := batchOperationOptions{resource: "db/records/s:write", action: "put", index: 8, document: writeDocument}
		var plans []*execution.Plan
		for _, opts := range []batchOperationOptions{readOptions, writeOptions} {
			work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
			if failure != nil {
				t.Fatal("read size profile affected mutation admission", failure)
			}
			plans = append(plans, work)
		}
		results, _ := adapter.executeRecords(context.Background(), plans)
		read := results[0].GetReadResult()
		if size <= limit && len(read.GetDocument().Data) != size || size > limit && read.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
			t.Fatal("read size boundary was not enforced", size, read)
		}
		if results[1].GetMutationResult().Outcome != pb.MutationOutcome_APPLIED || results[1].GetMutationResult().Failure != nil {
			t.Fatal("acknowledged write was affected by a read limit", results[1])
		}
	}
}
