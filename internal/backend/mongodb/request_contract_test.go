package mongodb

import (
	"context"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

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
