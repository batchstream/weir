package mongodb

import (
	"bytes"
	"context"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestMongoScanProjectionKeepsPrivateCheckpointAndNativeBSON(t *testing.T) {
	for _, mode := range []pb.ProjectionMode{pb.ProjectionMode_INCLUDE, pb.ProjectionMode_EXCLUDE} {
		t.Run(mode.String(), func(t *testing.T) {
			fields := []string{"_id"}
			document := bson.D{{Key: "_id", Value: "original"}, {Key: "n", Value: int32(7)}}
			expected := document[1:]
			if mode == pb.ProjectionMode_INCLUDE {
				fields = []string{"n"}
			} else {
				native := bson.E{Key: "native", Value: bson.Timestamp{T: 99, I: 3}}
				regex := bson.E{Key: "regex", Value: bson.Regex{Pattern: "x", Options: "i"}}
				document = append(document, native, regex)
				expected = document[1:]
			}
			cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document}}}
			replies := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor)}
			adapter := batchMockAdapter(t, replies, nil)
			projection := &pb.Projection{Mode: mode, Fields: fields}
			request := &pb.ScanRequest{Resource: "db/records", Projection: projection, PageSize: 1}
			work, failure := adapter.prepareScan(request)
			if failure != nil {
				t.Fatal(failure)
			}
			page, signal := adapter.fetchScan(context.Background(), work)
			want, _ := bson.Marshal(expected)
			if page.Failure != nil || signal != execution.Healthy || len(page.Documents) != 1 || !bytes.Equal(page.Documents[0].Data, want) || !page.Complete || len(page.NextContinuationToken) == 0 {
				t.Fatal("projection damaged native BSON or checkpoint", page.Failure)
			}
			state, err := protocol.DecodeScanToken(page.NextContinuationToken, "mongodb", work.Backend.(*scanPlan).fingerprint)
			if err != nil || bson.Raw(state).Lookup("last").Document().Lookup("_id").StringValue() != "original" {
				t.Fatal("projected result lost private identity", err)
			}
			request.ContinuationToken = page.NextContinuationToken
			projection.Fields = []string{"different"}
			if _, failure := adapter.prepareScan(request); failure.GetCode() != pb.FailureCode_INVALID_ARGUMENT {
				t.Fatal("projection change resumed old checkpoint", failure)
			}
		})
	}
}

func TestMongoNativeCommandFailureClassification(t *testing.T) {
	cases := []struct {
		code    int32
		failure pb.FailureCode
	}{
		{code: 18, failure: pb.FailureCode_UNAUTHENTICATED},
		{code: 13, failure: pb.FailureCode_PERMISSION_DENIED},
		{code: 26, failure: pb.FailureCode_TARGET_NOT_FOUND},
	}
	for _, item := range cases {
		native := mongo.CommandError{Code: item.code, Message: "private native message"}
		failure := backendFailure(context.Background(), native)
		command := bson.D{{Key: "code", Value: item.code}, {Key: "errmsg", Value: "private native message"}}
		raw, _ := bson.Marshal(command)
		unknown := protocol.Mutation(pb.MutationOutcome_UNKNOWN, protocol.Fail(pb.FailureCode_UNAVAILABLE, "unknown"))
		rejection := writeRejection(bson.Raw(raw).Lookup("code"), unknown)
		if failure.GetCode() != item.failure || rejection.Outcome != pb.MutationOutcome_NOT_APPLIED || rejection.GetFailure().GetCode() != item.failure {
			t.Fatal("native classification changed", failure, rejection)
		}
	}
}

func TestMongoScanProjectionRejectsDollarSegmentsBeforeBackend(t *testing.T) {
	adapter := &Adapter{}
	for _, field := range []string{"$price", "price.$amount", "_id.part"} {
		projection := &pb.Projection{Mode: pb.ProjectionMode_INCLUDE, Fields: []string{field}}
		request := &pb.ScanRequest{Resource: "db/records", Projection: projection}
		if _, failure := adapter.prepareScan(request); failure.GetCode() != pb.FailureCode_UNSUPPORTED {
			t.Fatal("Mongo-specific path reached backend", field, failure)
		}
	}
}
