package mongodb

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Cancel during a wide conversion without timing sleeps or a backend operation.
// Done and Err retain the normal context cancellation contract.
type cancelDuringConversion struct {
	context.Context
	cancel context.CancelFunc
	checks atomic.Int32
}

func (ctx *cancelDuringConversion) Err() error {
	if ctx.checks.Add(1) == 64 {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

func TestMongoPreparationCancellationKeepsCallerFailure(t *testing.T) {
	items := make(bson.A, 10000)
	for i := range items {
		items[i] = int32(i)
	}
	document := bson.D{{Key: "_id", Value: "item"}, {Key: "items", Value: items}}
	raw := expressionBSON(t, document)
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	for _, action := range []string{"put", "lua", "expression"} {
		t.Run(action, func(t *testing.T) {
			body := &pb.Document{ContentType: "application/bson", Data: raw}
			request := &pb.MutateRequest{Resource: "db/records/s:item"}
			switch action {
			case "put":
				put := &pb.MutateRequest_Put{Put: body}
				request.Action = put
			case "lua":
				program := &pb.LuaTransform{Source: []byte("return function(current, incoming) return incoming end"), Input: body}
				form := &pb.Transform_Lua{Lua: program}
				transform := &pb.Transform{Form: form}
				atomic := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
				request.Action = atomic
			case "expression":
				fields := bson.D{{Key: "items", Value: items}}
				update := bson.D{{Key: "$set", Value: fields}}
				body = &pb.Document{ContentType: ExpressionContentType, Data: expressionBSON(t, update)}
				form := &pb.Transform_BackendExpression{BackendExpression: body}
				transform := &pb.Transform{Form: form}
				atomic := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
				request.Action = atomic
			}
			operation := &pb.Command_Mutate{Mutate: request}
			command := &pb.Command{Operation: operation}
			record, err := execution.NewRecord("mongo", 1, command)
			if err != nil {
				t.Fatal(err)
			}
			base, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctx := &cancelDuringConversion{Context: base, cancel: cancel}
			work, failure := adapter.PrepareRecord(ctx, record)
			if work != nil || failure.GetCode() != pb.FailureCode_CANCELLED || ctx.Err() != context.Canceled {
				t.Fatal("conversion cancellation was misreported as invalid data", work, failure)
			}
		})
	}
}

func TestMongoStoredProgramDecodeCancellationNeverWrites(t *testing.T) {
	items := make(bson.A, 10000)
	for i := range items {
		items[i] = int32(i)
	}
	document := bson.D{{Key: "_id", Value: "item"}, {Key: "items", Value: items}}
	raw := expressionBSON(t, document)
	program := &luaengine.Program{Source: "return function(current, incoming) return current end"}
	native := &plan{id: "item", program: program}
	work := &execution.Plan{Backend: native}
	batch := &programBatch{plans: []*execution.Plan{work}, results: make([]*pb.MutationResult, 1)}
	base, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &cancelDuringConversion{Context: base, cancel: cancel}
	adapter := &Adapter{}
	positions := []int{0}
	documents := []bson.Raw{raw}
	writes, indexes := adapter.transformPrograms(ctx, batch, positions, documents)
	result := batch.results[0]
	if len(writes) != 0 || len(indexes) != 0 || result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_CANCELLED {
		t.Fatal("stored conversion cancellation became unsupported data or a write", result)
	}
}

func TestMongoStoredProgramCallerDeadlineDoesNotCancelBatch(t *testing.T) {
	items := make(bson.A, 500000)
	for i := range items {
		items[i] = int32(i)
	}
	document := bson.D{{Key: "_id", Value: "large"}, {Key: "items", Value: items}}
	raw := expressionBSON(t, document)
	largeProgram := &luaengine.Program{Source: "return function(current, incoming) return current end"}
	largeNative := &plan{id: "large", program: largeProgram}
	caller, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	largeWork := &execution.Plan{Backend: largeNative, Context: caller}
	healthyProgram := &luaengine.Program{Source: "return function(current, incoming) return {} end"}
	healthyNative := &plan{id: "healthy", program: healthyProgram}
	healthyWork := &execution.Plan{Backend: healthyNative, Context: t.Context()}
	batch := &programBatch{plans: []*execution.Plan{largeWork, healthyWork}, results: make([]*pb.MutationResult, 2)}
	positions := []int{0, 1}
	documents := []bson.Raw{raw, nil}
	adapter := &Adapter{}
	if caller.Err() != nil {
		t.Fatal("caller expired before conversion started")
	}
	writes, indexes := adapter.transformPrograms(t.Context(), batch, positions, documents)
	result := batch.results[0]
	if result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_DEADLINE_EXCEEDED || !largeProgram.ObservedAt.IsZero() {
		t.Fatal("caller deadline did not interrupt stored BSON decoding", result, largeProgram.ObservedAt)
	}
	if t.Context().Err() != nil || batch.results[1] != nil || len(writes) != 1 || len(indexes) != 1 || indexes[0] != 1 || writes[0].Backend.(*plan).action != "create" {
		t.Fatal("one caller's cancellation stopped its healthy batch peer", batch.results, writes, indexes)
	}
}
