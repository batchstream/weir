package mongodb

import (
	"context"
	"fmt"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoNonterminalReplyKeepsMessageWindowThroughGetMore(t *testing.T) {
	first := bson.D{{Key: "_id", Value: "a"}}
	last := bson.D{{Key: "_id", Value: "b"}}
	firstCursor := bson.D{{Key: "id", Value: int64(55)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{first}}}
	lastCursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "nextBatch", Value: bson.A{last}}}
	responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(firstCursor), readCursorResponse(lastCursor)}
	probe := &recordMemoryProbe{limit: 64 << 20}
	full, lastCapacity, finds, more := 0, 0, 0, 0
	monitor := &event.CommandMonitor{
		Started: func(_ context.Context, e *event.CommandStartedEvent) {
			if e.CommandName == "endSessions" {
				return
			}
			if probe.working != full {
				t.Error("business IO lost its negotiated window", e.CommandName, probe.working, full)
			}
			if e.CommandName == "find" {
				finds++
			} else if e.CommandName == "getMore" {
				more++
			}
		},
		Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
			if e.CommandName == "getMore" {
				lastCapacity = cap(e.Reply)
			}
		},
	}
	adapter := batchMockAdapter(t, responses, monitor)
	plans := make([]*execution.Plan, 2)
	for i, id := range []string{"a", "b"} {
		opts := batchOperationOptions{resource: "db/records/s:" + id, action: "read"}
		work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		work.Memory = probe
		plans[i] = work
	}
	full = recordWorking(plans)
	results := adapter.executeRecords(t.Context(), plans)
	for i, id := range []string{"a", "b"} {
		if bson.Raw(results[i].GetReadResult().GetDocument().GetData()).Lookup("_id").StringValue() != id {
			t.Fatal("cursor pages did not preserve complete records", i)
		}
	}
	expected := full - adapter.nativeReplyBytes() + lastCapacity + 36
	if finds != 1 || more != 1 || len(probe.resultWorking) != 2 || probe.resultWorking[0] != full || probe.resultWorking[1] != expected {
		t.Fatal("nonterminal and terminal pages had the same speculative lifetime", finds, more, probe.resultWorking, expected)
	}
}

func TestMongoFollowingQualificationAndWriteRestoreMessageWindow(t *testing.T) {
	for _, otherTarget := range []bool{false, true} {
		for _, deny := range []bool{false, true} {
			t.Run(fmt.Sprintf("other_target=%v/deny_restore=%v", otherTarget, deny), func(t *testing.T) {
				document := bson.D{{Key: "_id", Value: "a"}}
				readCursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document}}}
				responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(readCursor)}
				collection := "records"
				if otherTarget {
					collection = "other"
					responses = append(responses, collectionQualificationResponse("db", collection))
				}
				item := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}}
				writeCursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: bson.A{item}}}
				writeReply := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: writeCursor}, {Key: "nErrors", Value: 0}, {Key: "nInserted", Value: 1}, {Key: "nDeleted", Value: 0}, {Key: "nMatched", Value: 0}, {Key: "nModified", Value: 0}, {Key: "nUpserted", Value: 0}}
				responses = append(responses, writeReply)
				probe := &recordMemoryProbe{limit: 64 << 20, denyRestore: deny}
				full, qualifications, writes := 0, 0, 0
				monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
					if e.CommandName == "endSessions" {
						return
					}
					if probe.working != full {
						t.Error("new IO started before restoring negotiated capacity", e.CommandName, probe.working, full)
					}
					if e.CommandName == "listCollections" {
						qualifications++
					} else if e.CommandName == "bulkWrite" {
						writes++
					}
				}}
				adapter := batchMockAdapter(t, responses, monitor)
				readOptions := batchOperationOptions{resource: "db/records/s:a", action: "read"}
				read, failure := prepareTestRecord(adapter, batchOperation(t, readOptions))
				if failure != nil {
					t.Fatal(failure)
				}
				writeOptions := batchOperationOptions{resource: "db/" + collection + "/s:a", action: "create", document: document}
				write, failure := prepareTestRecord(adapter, batchOperation(t, writeOptions))
				if failure != nil {
					t.Fatal(failure)
				}
				read.Memory, write.Memory = probe, probe
				plans := []*execution.Plan{read, write}
				full = recordWorking(plans)
				results := adapter.executeRecords(t.Context(), plans)
				if results[0].GetReadResult().GetDocument() == nil {
					t.Fatal("later budget denial erased the complete read", results[0])
				}
				result := results[1].GetMutationResult()
				if deny {
					if result.GetOutcome() != pb.MutationOutcome_NOT_STARTED || result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || writes != 0 || qualifications != 1 {
						t.Fatal("unbudgeted IO or incorrect mutation evidence", result, qualifications, writes)
					}
				} else {
					expectedQualifications := 1
					if otherTarget {
						expectedQualifications++
					}
					if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.Failure != nil || writes != 1 || qualifications != expectedQualifications {
						t.Fatal("restored capacity did not preserve normal mutation", result, qualifications, writes)
					}
				}
			})
		}
	}
}

func TestMongoCursorCleanupRestoresBudgetAndSkipsUnapprovedIO(t *testing.T) {
	for _, deny := range []bool{false, true} {
		t.Run(fmt.Sprint(deny), func(t *testing.T) {
			probe := &recordMemoryProbe{working: 1024, reduced: true, denyRestore: deny}
			full, calls := 48_004_096, 0
			monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
				if e.CommandName == "endSessions" {
					return
				}
				calls++
				if e.CommandName != "killCursors" || probe.working != full {
					t.Error("cleanup IO escaped the restored window", e.CommandName, probe.working)
				}
			}}
			cleanup := bson.D{{Key: "ok", Value: 1}, {Key: "cursorsKilled", Value: bson.A{int64(55)}}, {Key: "cursorsNotFound", Value: bson.A{}}, {Key: "cursorsAlive", Value: bson.A{}}, {Key: "cursorsUnknown", Value: bson.A{}}}
			responses := []bson.D{cleanup}
			adapter := batchMockAdapter(t, responses, monitor)
			session, err := adapter.client.StartSession()
			if err != nil {
				t.Fatal(err)
			}
			memory := &recordMemory{owner: probe, base: full}
			ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
			state := &recordCursor{target: namespace{database: "db", collection: "records"}, session: session, cursor: 55, cursorKnown: true}
			failure := adapter.closeRecordCursorState(ctx, state)
			if state.session != nil || state.cursor != 0 || !state.closed {
				t.Fatal("local cursor ownership survived cleanup", state)
			}
			if deny {
				if failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || calls != 0 {
					t.Fatal("unapproved cleanup IO was sent", failure, calls)
				}
			} else if failure != nil || calls != 1 {
				t.Fatal("budgeted cleanup did not execute exactly once", failure, calls)
			}
		})
	}
}
