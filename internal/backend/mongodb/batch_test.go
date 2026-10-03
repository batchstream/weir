package mongodb

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
)

type batchOperationOptions struct {
	resource, action string
	index            uint64
	document         bson.D
	program          string
}

func batchOperation(t testing.TB, opts batchOperationOptions) *pb.Operation {
	t.Helper()
	op := &pb.Operation{Index: opts.index}
	if opts.action == "read" {
		read := &pb.ReadRequest{Resource: opts.resource}
		op.Operation = &pb.Operation_Read{Read: read}
		return op
	}
	request := &pb.MutateRequest{Resource: opts.resource}
	if opts.action == "program" {
		program := &pb.ProgramTransform{Runtime: "lua.v1", Source: []byte(opts.program)}
		form := &pb.Transform_Program{Program: program}
		transform := &pb.Transform{Form: form}
		request.Action = &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	} else if opts.action == "delete" {
		empty := &pb.Empty{}
		request.Action = &pb.MutateRequest_Delete{Delete: empty}
	} else {
		raw := expressionBSON(t, opts.document)
		document := &pb.Document{MediaType: "application/bson", Data: raw}
		switch opts.action {
		case "create":
			request.Action = &pb.MutateRequest_Create{Create: document}
		case "put":
			request.Action = &pb.MutateRequest_Put{Put: document}
		case "replace":
			request.Action = &pb.MutateRequest_Replace{Replace: document}
		case "expression":
			document.MediaType = ExpressionMedia
			form := &pb.Transform_BackendExpression{BackendExpression: document}
			transform := &pb.Transform{Form: form}
			request.Action = &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
		default:
			t.Fatal("invalid test action", opts.action)
		}
	}
	op.Operation = &pb.Operation_Mutate{Mutate: request}
	return op
}

func batchMockAdapter(t *testing.T, responses []bson.D, monitor *event.CommandMonitor) *Adapter {
	t.Helper()
	deployment := drivertest.NewMockDeployment(responses...)
	opts := options.Client().SetRetryReads(false).SetRetryWrites(false).SetMaxAdaptiveRetries(0).SetMonitor(monitor)
	opts.Deployment = deployment
	client, err := mongo.Connect(opts)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{Store: "mongo"}
	a := &Adapter{client: client, config: config}
	t.Cleanup(func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	})
	return a
}

func readCursorResponse(cursor bson.D) bson.D {
	response := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: cursor}}
	return response
}

func TestMongoPointReadRejectsMalformedCursorEvidence(t *testing.T) {
	document := bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: 1}}
	unexpected := bson.D{{Key: "_id", Value: "unrequested"}}
	cases := []struct {
		name   string
		cursor bson.D
	}{
		{name: "missing id", cursor: bson.D{{Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{}}}},
		{name: "missing namespace", cursor: bson.D{{Key: "id", Value: int64(0)}, {Key: "firstBatch", Value: bson.A{}}}},
		{name: "missing batch", cursor: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}}},
		{name: "unexpected id", cursor: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{unexpected}}}},
		{name: "duplicate id", cursor: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document, document}}}},
		{name: "partial", cursor: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document}}, {Key: "partialResultsReturned", Value: true}}},
		{name: "empty live cursor", cursor: bson.D{{Key: "id", Value: int64(8)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response := readCursorResponse(tc.cursor)
			cleanup := bson.D{{Key: "ok", Value: 1}, {Key: "cursorsKilled", Value: bson.A{int64(8)}}, {Key: "cursorsAlive", Value: bson.A{}}, {Key: "cursorsNotFound", Value: bson.A{}}, {Key: "cursorsUnknown", Value: bson.A{}}}
			responses := []bson.D{collectionQualificationResponse("db", "records"), response, cleanup}
			a := batchMockAdapter(t, responses, nil)
			var plans []*execution.Plan
			for _, name := range []string{"a", "b"} {
				opts := batchOperationOptions{resource: "weir://mongo/db/records/s:" + name, action: "read"}
				p, failure := prepareTestRecord(a, batchOperation(t, opts))
				if failure != nil {
					t.Fatal(failure)
				}
				plans = append(plans, p)
			}
			replies, _ := a.executeRecords(context.Background(), plans)
			for _, reply := range replies {
				if reply.GetRead().GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE {
					t.Fatal("malformed response justified a read or missing record", reply)
				}
			}
		})
	}
}

func TestMongoPointReadMatchesTypedIDsAcrossPages(t *testing.T) {
	objectID := bson.NewObjectID()
	integerDoc := bson.D{{Key: "_id", Value: int32(42)}}
	objectDoc := bson.D{{Key: "_id", Value: objectID}}
	stringDoc := bson.D{{Key: "_id", Value: "a"}}
	first := bson.D{{Key: "id", Value: int64(17)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{integerDoc}}}
	next := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "nextBatch", Value: bson.A{objectDoc, stringDoc}}}
	responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(first), readCursorResponse(next)}
	var commands []string
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) { commands = append(commands, e.CommandName) }}
	a := batchMockAdapter(t, responses, monitor)
	var plans []*execution.Plan
	for i, name := range []string{"s:a", "oid:" + objectID.Hex(), "i:42", "s:missing"} {
		opts := batchOperationOptions{resource: "weir://mongo/db/records/" + name, action: "read", index: uint64(i + 9)}
		p, failure := prepareTestRecord(a, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, p)
	}
	replies, _ := a.executeRecords(context.Background(), plans)
	for i, reply := range replies {
		if reply.Index != uint64(i+9) || i < 3 && reply.GetRead().GetDocument() == nil || i == 3 && reply.GetRead().GetMissing() == nil {
			t.Fatal("result identity/order mismatch", replies)
		}
	}
	if len(commands) != 3 || commands[0] != "listCollections" || commands[1] != "find" || commands[2] != "getMore" {
		t.Fatal("read batch was not a single cursor", commands)
	}
}

func TestMongoBulkItemEvidence(t *testing.T) {
	matched := bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}, {Key: "nModified", Value: 0}}
	missing := bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 0}, {Key: "nModified", Value: 0}}
	rejected := bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 11000}}
	uncertain := bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 391}}
	cases := []struct {
		name, action string
		doc          bson.D
		concern      bool
		outcome      pb.MutationOutcome
	}{
		{name: "replace noop", action: "replace", doc: matched, outcome: pb.MutationOutcome_APPLIED},
		{name: "replace missing", action: "replace", doc: missing, outcome: pb.MutationOutcome_NOT_APPLIED},
		{name: "expression missing", action: "expression", doc: missing, outcome: pb.MutationOutcome_NOT_APPLIED},
		{name: "duplicate no count", action: "create", doc: rejected, outcome: pb.MutationOutcome_NOT_APPLIED},
		{name: "reauth uncertain", action: "expression", doc: uncertain, outcome: pb.MutationOutcome_UNKNOWN},
		{name: "write concern", action: "replace", doc: matched, concern: true, outcome: pb.MutationOutcome_UNKNOWN},
		{name: "missing count", action: "replace", doc: bson.D{{Key: "ok", Value: 1}, {Key: "nModified", Value: 0}}, outcome: pb.MutationOutcome_UNKNOWN},
		{name: "missing modified count", action: "expression", doc: bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}}, outcome: pb.MutationOutcome_UNKNOWN},
		{name: "error with modification", action: "expression", doc: bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 112}, {Key: "n", Value: 0}, {Key: "nModified", Value: 1}}, outcome: pb.MutationOutcome_UNKNOWN},
		{name: "error with upsert", action: "put", doc: bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 11000}, {Key: "upserted", Value: bson.D{{Key: "_id", Value: "a"}}}}, outcome: pb.MutationOutcome_UNKNOWN},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fields, err := scanFields(expressionBSON(t, tc.doc))
			if err != nil {
				t.Fatal(err)
			}
			native := &plan{action: tc.action}
			result := bulkItemReply(native, fields, tc.concern)
			if result.Outcome != tc.outcome {
				t.Fatal(result)
			}
		})
	}
}

func TestMongoBulkCursorRequiresConsistentSummary(t *testing.T) {
	native := &plan{action: "create"}
	work := &execution.Plan{Backend: native}
	item := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: bson.A{item}}}
	response := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: cursor}, {Key: "nErrors", Value: 0}, {Key: "nInserted", Value: 0}, {Key: "nDeleted", Value: 0}, {Key: "nMatched", Value: 0}, {Key: "nModified", Value: 0}, {Key: "nUpserted", Value: 0}}
	state := &writeBatch{plans: []*execution.Plan{work}, results: make([]*pb.MutationResult, 1)}
	state.cursor.items = 1
	if state.reply(expressionBSON(t, response), true) {
		t.Fatal("contradictory inserted count accepted")
	}
}

func TestMongoBulkReplyQualifiesReplicaSetMetadata(t *testing.T) {
	native := &plan{action: "create"}
	work := &execution.Plan{Backend: native}
	item := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: bson.A{item}}}
	base := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: cursor}, {Key: "nErrors", Value: 0}, {Key: "nInserted", Value: 1}, {Key: "nDeleted", Value: 0}, {Key: "nMatched", Value: 0}, {Key: "nModified", Value: 0}, {Key: "nUpserted", Value: 0}}
	opTime := bson.D{{Key: "ts", Value: bson.Timestamp{T: 1, I: 2}}, {Key: "t", Value: int64(1)}}
	valid := bson.D{{Key: "electionId", Value: bson.NewObjectID()}, {Key: "opTime", Value: opTime}}
	cases := []struct {
		name   string
		fields bson.D
		valid  bool
	}{
		{name: "replica metadata", fields: valid, valid: true},
		{name: "unknown metadata", fields: bson.D{{Key: "unrecognized", Value: 1}}},
		{name: "bad election id", fields: bson.D{{Key: "electionId", Value: "wrong type"}}},
		{name: "bad opTime", fields: bson.D{{Key: "opTime", Value: 1}}},
		{name: "opTime missing term", fields: bson.D{{Key: "opTime", Value: bson.D{{Key: "ts", Value: bson.Timestamp{T: 1}}}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := &writeBatch{plans: []*execution.Plan{work}, results: make([]*pb.MutationResult, 1)}
			state.cursor.items = 1
			response := append(append(bson.D(nil), base...), tc.fields...)
			if state.reply(expressionBSON(t, response), true) != tc.valid {
				t.Fatal("unexpected metadata qualification", response)
			}
		})
	}
}

func TestMongoVerboseWriteCursorKeepsItemIndexesAndSessionAcrossPages(t *testing.T) {
	firstItem := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 1}, {Key: "n", Value: 1}}
	nextItem := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}}
	firstCursor := bson.D{{Key: "id", Value: int64(91)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: bson.A{firstItem}}}
	nextCursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "nextBatch", Value: bson.A{nextItem}}}
	first := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: firstCursor}, {Key: "nErrors", Value: 0}, {Key: "nInserted", Value: 2}, {Key: "nDeleted", Value: 0}, {Key: "nMatched", Value: 0}, {Key: "nModified", Value: 0}, {Key: "nUpserted", Value: 0}}
	next := readCursorResponse(nextCursor)
	responses := []bson.D{collectionQualificationResponse("db", "records"), first, next}
	var commands []string
	var sessions []bson.Raw
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName != "bulkWrite" && e.CommandName != "getMore" {
			return
		}
		commands = append(commands, e.CommandName)
		sessions = append(sessions, append(bson.Raw(nil), e.Command.Lookup("lsid").Document()...))
		if e.CommandName == "bulkWrite" && e.Command.Lookup("errorsOnly").Boolean() {
			t.Error("verbose results were not requested")
		}
	}}
	a := batchMockAdapter(t, responses, monitor)
	var plans []*execution.Plan
	for i, id := range []string{"a", "b"} {
		document := bson.D{{Key: "_id", Value: id}}
		opts := batchOperationOptions{resource: "weir://mongo/db/records/s:" + id, action: "create", index: uint64(i + 8), document: document}
		p, failure := prepareTestRecord(a, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, p)
	}
	replies, signal := a.executeRecords(context.Background(), plans)
	for i, reply := range replies {
		if reply.Index != uint64(i+8) || reply.GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
			t.Fatal("verbose results lost item correspondence", replies)
		}
	}
	if signal != execution.Healthy || len(commands) != 2 || commands[0] != "bulkWrite" || commands[1] != "getMore" || !bytes.Equal(sessions[0], sessions[1]) {
		t.Fatal("cursor was restarted or changed session", commands, signal)
	}
}

func TestMongoReadContinuationStopsWhenItsCallersCancel(t *testing.T) {
	caller, cancel := context.WithCancel(context.Background())
	defer cancel()
	document := bson.D{{Key: "_id", Value: "a"}}
	cursor := bson.D{{Key: "id", Value: int64(19)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document}}}
	cleanup := bson.D{{Key: "ok", Value: 1}, {Key: "cursorsKilled", Value: bson.A{int64(19)}}, {Key: "cursorsAlive", Value: bson.A{}}, {Key: "cursorsNotFound", Value: bson.A{}}, {Key: "cursorsUnknown", Value: bson.A{}}}
	responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor), cleanup}
	var commands []string
	monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
		commands = append(commands, e.CommandName)
		if e.CommandName == "find" {
			cancel()
		}
	}}
	a := batchMockAdapter(t, responses, monitor)
	var plans []*execution.Plan
	for _, id := range []string{"a", "b"} {
		opts := batchOperationOptions{resource: "weir://mongo/db/records/s:" + id, action: "read"}
		p, failure := prepareTestRecord(a, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
		p.Context = caller
		plans = append(plans, p)
	}
	replies, signal := a.executeRecords(context.Background(), plans)
	if replies[0].GetRead().GetDocument() == nil || replies[1].GetRead().GetFailure().GetCode() != pb.FailureCode_CANCELLED || signal != execution.Neutral {
		t.Fatal("canceled reads consumed further cursor work", replies, signal)
	}
	if len(commands) != 3 || commands[0] != "listCollections" || commands[1] != "find" || commands[2] != "killCursors" {
		t.Fatal("canceled cursor was continued", commands)
	}
}

func TestMongoCanceledProgramBatchHasNeutralFeedback(t *testing.T) {
	caller, cancel := context.WithCancel(context.Background())
	cancel()
	native := &plan{action: "program"}
	op := &pb.Operation{}
	p := &execution.Plan{Operation: op, Backend: native, Context: caller}
	a := &Adapter{}
	replies, signal := a.executeRecords(context.Background(), []*execution.Plan{p})
	if replies[0].GetMutation().Outcome != pb.MutationOutcome_NOT_STARTED || signal != execution.Neutral {
		t.Fatal("cancellation generated healthy feedback", replies, signal)
	}
}

func TestMongoBulkCursorRejectsDuplicateAndMissingIndexes(t *testing.T) {
	first := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}}
	duplicate := first
	missing := bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}}
	for _, bad := range []bson.D{duplicate, missing} {
		native := &plan{action: "create"}
		work := &execution.Plan{Backend: native}
		state := &writeBatch{plans: []*execution.Plan{work, work}, results: make([]*pb.MutationResult, 2)}
		state.cursor.items = 2
		cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin.$cmd.bulkWrite"}, {Key: "firstBatch", Value: bson.A{first, bad}}}
		response := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: cursor}, {Key: "nErrors", Value: 0}, {Key: "nInserted", Value: 2}, {Key: "nDeleted", Value: 0}, {Key: "nMatched", Value: 0}, {Key: "nModified", Value: 0}, {Key: "nUpserted", Value: 0}}
		if state.reply(expressionBSON(t, response), true) {
			t.Fatal("invalid index correspondence was accepted")
		}
	}
}

func TestMongoBatchBoundsRejectBeforeBackendWork(t *testing.T) {
	config := Config{Store: "mongo"}
	a := &Adapter{config: config}
	opts := batchOperationOptions{resource: "weir://mongo/db/records/s:a", action: "read"}
	p, failure := prepareTestRecord(a, batchOperation(t, opts))
	if failure != nil {
		t.Fatal(failure)
	}
	for _, count := range []int{1, 129} {
		plans := make([]*execution.Plan, count)
		for i := range plans {
			plans[i] = p
		}
		if count == 1 {
			p.Bytes = protocol.MaxBatchRequestBytes + 1
		}
		results, _ := a.executeRecords(context.Background(), plans)
		for _, result := range results {
			if result.GetRead().GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
				t.Fatal(result)
			}
		}
		p.Bytes = protocol.MaxDocument
	}
}

func TestMongoReadBatchAccepts513DistinctDocuments(t *testing.T) {
	const count = 513
	documents := make(bson.A, count)
	plans := make([]*execution.Plan, count)
	for i := range documents {
		documents[i] = bson.D{{Key: "_id", Value: fmt.Sprintf("item-%d", i)}, {Key: "n", Value: i}}
	}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: documents}}
	responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor)}
	finds := 0
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "find" {
			finds++
		}
	}}
	adapter := batchMockAdapter(t, responses, monitor)
	for i := range plans {
		opts := batchOperationOptions{resource: fmt.Sprintf("weir://mongo/db/records/s:item-%d", i), action: "read", index: uint64(i + 1)}
		var failure *pb.Failure
		plans[i], failure = prepareTestRecord(adapter, batchOperation(t, opts))
		if failure != nil {
			t.Fatal(failure)
		}
	}
	replies, _ := adapter.executeRecords(t.Context(), plans)
	for i, reply := range replies {
		if reply.Index != uint64(i+1) || reply.GetRead().GetDocument() == nil {
			t.Fatalf("record %d failed: %v", i, reply)
		}
	}
	if finds != 1 {
		t.Fatal("batch split by an arbitrary item limit", finds)
	}
}
