package mongodb

import (
	"context"
	"reflect"
	"strings"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func authorizationRejection() bson.D {
	reply := bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: int32(13)}, {Key: "codeName", Value: "Unauthorized"}, {Key: "errmsg", Value: "not authorized"}}
	return reply
}

func authorizationInsertReply(count int) bson.D {
	items := make(bson.A, count)
	for i := range items {
		item := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: int32(i)}, {Key: "n", Value: 1}}
		items[i] = item
	}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: items}}
	reply := bson.D{
		{Key: "ok", Value: 1}, {Key: "cursor", Value: cursor}, {Key: "nErrors", Value: 0},
		{Key: "nInserted", Value: count}, {Key: "nDeleted", Value: 0}, {Key: "nMatched", Value: 0},
		{Key: "nModified", Value: 0}, {Key: "nUpserted", Value: 0},
	}
	return reply
}

func authorizationPlans(t *testing.T, adapter *Adapter, resources []string) []*execution.Plan {
	t.Helper()
	plans := make([]*execution.Plan, 0, len(resources))
	for _, resource := range resources {
		_, id, valid := strings.Cut(resource, "/s:")
		if !valid {
			t.Fatal("authorization test requires a string record ID")
		}
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: 1}}
		opts := batchOperationOptions{resource: resource, action: "create", document: document}
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	return plans
}

func TestMongoCrossNamespaceAuthorizationRejectionIsolatesWrites(t *testing.T) {
	for _, zeroCounts := range []bool{false, true} {
		name := "absent counts"
		if zeroCounts {
			name = "zero counts"
		}
		t.Run(name, func(t *testing.T) {
			rejection := authorizationRejection()
			if zeroCounts {
				for _, key := range []string{"nErrors", "nInserted", "nDeleted", "nMatched", "nModified", "nUpserted"} {
					field := bson.E{Key: key, Value: int64(0)}
					rejection = append(rejection, field)
				}
			}
			responses := []bson.D{
				collectionQualificationResponse("db", "allowed"), collectionQualificationResponse("db", "denied"), collectionQualificationResponse("other", "allowed"),
				rejection, authorizationInsertReply(2), authorizationRejection(), authorizationInsertReply(1),
			}
			var targets [][]string
			var sizes []int
			monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
				if e.CommandName != "bulkWrite" {
					return
				}
				namespaces, _ := e.Command.Lookup("nsInfo").Array().Values()
				ops, _ := e.Command.Lookup("ops").Array().Values()
				batchTargets := make([]string, len(namespaces))
				for i, namespace := range namespaces {
					batchTargets[i] = namespace.Document().Lookup("ns").StringValue()
				}
				for _, op := range ops {
					index := op.Document().Lookup("insert").Int32()
					if index < 0 || int(index) >= len(namespaces) || len(namespaces) == 1 && index != 0 {
						t.Error("isolation retained an invalid namespace index", e.Command)
					}
				}
				targets = append(targets, batchTargets)
				sizes = append(sizes, len(ops))
			}}
			adapter := batchMockAdapter(t, responses, monitor)
			resources := []string{"db/allowed/s:first", "db/denied/s:first", "other/allowed/s:first", "db/allowed/s:second"}
			plans := authorizationPlans(t, adapter, resources)
			results := adapter.executeRecords(t.Context(), plans)
			for i, result := range results {
				want := pb.MutationOutcome_APPLIED
				if i == 1 {
					want = pb.MutationOutcome_NOT_APPLIED
					if result.GetMutationResult().GetFailure().GetCode() != pb.FailureCode_PERMISSION_DENIED {
						t.Fatal("denied namespace lost its failure", results)
					}
				}
				if result.GetMutationResult().GetOutcome() != want {
					t.Fatal("denied namespace poisoned healthy callers", i, results)
				}
			}
			wantTargets := [][]string{{"db.allowed", "db.denied", "other.allowed"}, {"db.allowed"}, {"db.denied"}, {"other.allowed"}}
			if !reflect.DeepEqual(targets, wantTargets) || !reflect.DeepEqual(sizes, []int{4, 2, 1, 1}) {
				t.Fatal("namespace isolation was not bounded or lost batching", targets, sizes)
			}
		})
	}
}

func TestMongoCrossNamespaceExecutionEvidenceDoesNotIsolate(t *testing.T) {
	itemRejection := bson.D{{Key: "ok", Value: 0}, {Key: "idx", Value: 0}, {Key: "code", Value: int32(13)}}
	itemApplied := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 1}, {Key: "n", Value: 1}}
	perItem := authorizationInsertReply(1)
	perItem[1].Value = bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: bson.A{itemRejection, itemApplied}}}
	perItem[2].Value = 1
	firstPage := authorizationInsertReply(2)
	firstItem := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}}
	firstPage[1].Value = bson.D{{Key: "id", Value: int64(99)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: bson.A{firstItem}}}
	cleanup := bson.D{{Key: "ok", Value: 1}, {Key: "cursorsKilled", Value: bson.A{int64(99)}}, {Key: "cursorsAlive", Value: bson.A{}}, {Key: "cursorsNotFound", Value: bson.A{}}, {Key: "cursorsUnknown", Value: bson.A{}}}
	authentication := authorizationRejection()
	authentication[1].Value = int32(18)
	cases := []struct {
		name      string
		responses []bson.D
	}{
		{name: "per item unauthorized", responses: []bson.D{perItem}},
		{name: "getMore unauthorized", responses: []bson.D{firstPage, authorizationRejection(), cleanup}},
		{name: "authentication", responses: []bson.D{authentication}},
		{name: "transport failure"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writes := 0
			monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
				if e.CommandName == "bulkWrite" {
					writes++
				}
			}}
			adapter := batchMockAdapter(t, tc.responses, monitor)
			plans := authorizationPlans(t, adapter, []string{"db/allowed/s:first", "db/denied/s:first"})
			results := adapter.executeWrites(t.Context(), plans)
			if writes != 1 {
				t.Fatal("non-initial-authorization evidence replayed mutation", writes)
			}
			if tc.name == "per item unauthorized" && (results[0].GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_APPLIED || results[1].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED) {
				t.Fatal("per-item evidence lost caller correspondence", results)
			}
			if tc.name == "getMore unauthorized" && (results[0].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || results[1].GetMutationResult().GetOutcome() != pb.MutationOutcome_UNKNOWN) {
				t.Fatal("getMore rejection changed existing mutation evidence", results)
			}
		})
	}
}

func TestMongoCrossNamespaceAmbiguousRejectionDoesNotReplay(t *testing.T) {
	cases := []struct {
		name  string
		field bson.E
	}{
		{name: "cursor", field: bson.E{Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}}}},
		{name: "write concern", field: bson.E{Key: "writeConcernError", Value: bson.D{{Key: "code", Value: 64}}}},
		{name: "inserted", field: bson.E{Key: "nInserted", Value: 1}},
		{name: "deleted", field: bson.E{Key: "nDeleted", Value: 1}},
		{name: "matched", field: bson.E{Key: "nMatched", Value: 1}},
		{name: "modified", field: bson.E{Key: "nModified", Value: 1}},
		{name: "upserted", field: bson.E{Key: "nUpserted", Value: 1}},
		{name: "invalid count", field: bson.E{Key: "nInserted", Value: "zero"}},
		{name: "item errors", field: bson.E{Key: "writeErrors", Value: bson.A{}}},
		{name: "error labels", field: bson.E{Key: "errorLabels", Value: bson.A{"RetryableWriteError"}}},
		{name: "unknown field", field: bson.E{Key: "unexpected", Value: true}},
		{name: "duplicate field", field: bson.E{Key: "code", Value: int32(13)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rejection := append(authorizationRejection(), tc.field)
			writes := 0
			monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
				if e.CommandName == "bulkWrite" {
					writes++
				}
			}}
			adapter := batchMockAdapter(t, []bson.D{rejection}, monitor)
			plans := authorizationPlans(t, adapter, []string{"db/allowed/s:first", "db/denied/s:first"})
			adapter.executeWrites(t.Context(), plans)
			if writes != 1 {
				t.Fatal("ambiguous rejection replayed mutation", writes)
			}
		})
	}
}

func TestMongoCrossNamespaceAuthorizationIsolationHonorsCancellation(t *testing.T) {
	for _, wholeBatch := range []bool{false, true} {
		name := "one caller"
		if wholeBatch {
			name = "whole batch"
		}
		t.Run(name, func(t *testing.T) {
			caller, cancel := context.WithCancel(t.Context())
			defer cancel()
			writes := 0
			monitor := &event.CommandMonitor{
				Started: func(_ context.Context, e *event.CommandStartedEvent) {
					if e.CommandName == "bulkWrite" {
						writes++
					}
				},
				Failed: func(_ context.Context, e *event.CommandFailedEvent) {
					if e.CommandName == "bulkWrite" {
						cancel()
					}
				},
			}
			responses := []bson.D{authorizationRejection(), authorizationRejection()}
			adapter := batchMockAdapter(t, responses, monitor)
			plans := authorizationPlans(t, adapter, []string{"db/allowed/s:first", "db/denied/s:first"})
			ctx := t.Context()
			if wholeBatch {
				ctx = caller
			} else {
				plans[0].Context = caller
			}
			results := adapter.executeWrites(ctx, plans)
			for i, result := range results {
				if wholeBatch || i == 0 {
					if result.GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_STARTED || result.GetMutationResult().GetFailure().GetCode() != pb.FailureCode_CANCELLED {
						t.Fatal("canceled work started after initial rejection", results)
					}
				} else if result.GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetMutationResult().GetFailure().GetCode() != pb.FailureCode_PERMISSION_DENIED {
					t.Fatal("live denied caller lost its independent result", results)
				}
			}
			want := 2
			if wholeBatch {
				want = 1
			}
			if writes != want {
				t.Fatal("canceled namespace was dispatched", writes, want)
			}
		})
	}
}
