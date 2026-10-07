package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
	"github.com/batchstream/weir/internal/value"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const programAttempts = 5
const programCleanup = 200 * time.Millisecond

// The shared envelope holds at most 8 MiB of source BSON plus encoded writes.
// Lua evaluates one document at a time within the 64 MiB workspace reservation.
const programRetainedBytes = 8 << 20
const programWorkingBytes = 64 << 20

var errProgramBatchBound = errors.New("program batch retained byte bound")
var errProgramReadEvidence = errors.New("program read acknowledgement incomplete")

type programBatch struct {
	plans   []*execution.Plan
	results []*pb.MutationResult
}

// Programs share a short snapshot transaction only when their target and typed
// identities are distinct. Caller contexts govern evaluation and admission to W,
// while the shared execution context governs the transaction's wire calls.
func (a *Adapter) executePrograms(ctx context.Context, plans []*execution.Plan) []*pb.Event {
	results := make([]*pb.Event, len(plans))
	for start := 0; start < len(plans); {
		target := plans[start].Backend.(*plan).target
		ids := make(map[any]bool)
		end := start
		for end < len(plans) {
			native := plans[end].Backend.(*plan)
			if native.target != target || ids[native.id] {
				break
			}
			ids[native.id] = true
			end++
		}
		batch := &programBatch{plans: plans[start:end], results: make([]*pb.MutationResult, end-start)}
		a.runPrograms(ctx, batch)
		for i, result := range batch.results {
			value := &pb.Event_MutationResult{MutationResult: result}
			event := &pb.Event{Value: value}
			results[start+i] = event
		}
		start = end
	}
	return results
}
func (a *Adapter) runPrograms(ctx context.Context, batch *programBatch) {
	for i, work := range batch.plans {
		if result := unstarted(ctx, work); result != nil {
			batch.results[i] = result.GetMutationResult()
		}
	}
	active := batch.active()
	if len(active) == 0 {
		return
	}
	target := batch.plans[active[0]].Backend.(*plan).target
	if failure := a.qualifyTarget(ctx, target); failure != nil {
		batch.fail(pb.MutationOutcome_NOT_STARTED, failure)
		return
	}
	session, err := a.client.StartSession()
	if err != nil {
		batch.fail(pb.MutationOutcome_NOT_APPLIED, backendFailure(ctx, err))
		return
	}
	ambiguous := false
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), programCleanup)
		defer cancel()
		if ambiguous {
			cancel()
		}
		session.EndSession(cleanup)
	}()
	txctx := mongo.NewSessionContext(ctx, session)
	for attempt := 0; attempt < programAttempts && ctx.Err() == nil; attempt++ {
		batch.cancelled(ctx)
		active = batch.active()
		if len(active) == 0 {
			return
		}
		transaction := options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority()).SetReadPreference(readpref.Primary())
		if err = session.StartTransaction(transaction); err != nil {
			batch.fail(pb.MutationOutcome_NOT_APPLIED, backendFailure(ctx, err))
			return
		}
		readPlans := make([]*execution.Plan, len(active))
		for i, position := range active {
			readPlans[i] = batch.plans[position]
		}
		documents, readErr := a.readPrograms(txctx, readPlans)
		if readErr != nil {
			aborted := a.abortProgramTransaction(session)
			if !aborted {
				batch.fail(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_UNAVAILABLE, "MongoDB transaction rollback unconfirmed"))
				return
			}
			if errors.Is(readErr, errProgramBatchBound) && len(active) > 1 {
				a.splitPrograms(ctx, batch, active)
				return
			}
			if programHasLabel(readErr, "TransientTransactionError") && pauseProgram(ctx) {
				continue
			}
			batch.fail(pb.MutationOutcome_NOT_APPLIED, backendFailure(ctx, readErr))
			return
		}
		writes, writePositions, transformErr := a.transformPrograms(txctx, batch, active, documents)
		if transformErr != nil {
			aborted := a.abortProgramTransaction(session)
			if !aborted {
				batch.fail(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_UNAVAILABLE, "MongoDB transaction rollback unconfirmed"))
				return
			}
			if errors.Is(transformErr, errProgramBatchBound) && len(batch.active()) > 1 {
				a.splitPrograms(ctx, batch, batch.active())
				return
			}
			failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Lua batch exceeds retained document bound")
			batch.fail(pb.MutationOutcome_NOT_APPLIED, failure)
			return
		}
		// A cancellation during another member's Lua evaluation only excludes
		// this caller's not-yet-sent write. Nothing is retracted after W starts.
		keptWrites := writes[:0]
		keptPositions := writePositions[:0]
		for i, position := range writePositions {
			work := batch.plans[position]
			if failure := programCallerFailure(ctx, work); failure != nil {
				batch.results[position] = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
				continue
			}
			keptWrites = append(keptWrites, writes[i])
			keptPositions = append(keptPositions, position)
		}
		writes, writePositions = keptWrites, keptPositions
		if len(writes) == 0 {
			a.abortProgramTransaction(session)
			return
		}
		replies, writeErr := a.writePrograms(txctx, writes)
		if writeErr != nil {
			aborted := a.abortProgramTransaction(session)
			if !aborted {
				batch.fail(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_UNAVAILABLE, "MongoDB transaction rollback unconfirmed"))
				return
			}
			if programHasLabel(writeErr, "TransientTransactionError") && pauseProgram(ctx) {
				continue
			}
			batch.fail(pb.MutationOutcome_NOT_APPLIED, backendFailure(ctx, writeErr))
			return
		}
		rejected, retry := false, false
		for i, reply := range replies {
			if reply == nil || reply.Outcome == pb.MutationOutcome_UNKNOWN {
				rejected = true
				continue
			}
			if reply.Outcome != pb.MutationOutcome_APPLIED {
				rejected = true
				// A missing insert can race an external writer. One fresh
				// snapshot distinguishes that race from a stable constraint.
				if reply.GetFailure().GetCode() == pb.FailureCode_CONFLICT || writes[i].Backend.(*plan).action == "create" && attempt == 0 {
					retry = true
				}
			}
		}
		if rejected {
			aborted := a.abortProgramTransaction(session)
			if !aborted {
				batch.fail(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_UNAVAILABLE, "MongoDB transaction rollback unconfirmed"))
				return
			}
			if !retry {
				for i, reply := range replies {
					if reply != nil && reply.Outcome == pb.MutationOutcome_NOT_APPLIED {
						batch.results[writePositions[i]] = reply
					}
				}
				if len(batch.active()) == len(active) {
					batch.fail(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_UNAVAILABLE, "MongoDB batch write acknowledgement incomplete"))
					return
				}
			}
			if pauseProgram(ctx) {
				continue
			}
			break
		}
		retryTransaction := false
		for commitAttempt := 0; commitAttempt < programAttempts && ctx.Err() == nil; commitAttempt++ {
			commitContext, release := nativeAttemptContext(txctx)
			err = session.CommitTransaction(commitContext)
			release()
			if err == nil {
				batch.fail(pb.MutationOutcome_APPLIED, nil)
				return
			}
			if !ambiguous && !programHasLabel(err, "UnknownTransactionCommitResult") && programHasLabel(err, "TransientTransactionError") {
				retryTransaction = true
				break
			}
			ambiguous = true
			if !pauseProgram(ctx) {
				break
			}
		}
		if !retryTransaction || !pauseProgram(ctx) {
			break
		}
	}
	outcome := pb.MutationOutcome_NOT_APPLIED
	failure := protocol.Fail(pb.FailureCode_CONFLICT, "MongoDB transaction retry limit exceeded")
	if ambiguous {
		outcome = pb.MutationOutcome_UNKNOWN
		failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "MongoDB commit acknowledgement is ambiguous")
	}
	if ctx.Err() != nil {
		failure = protocol.ContextFailure(ctx)
	}
	batch.fail(outcome, failure)
	return
}

func (b *programBatch) active() []int {
	positions := make([]int, 0, len(b.plans))
	for i, result := range b.results {
		if result == nil {
			positions = append(positions, i)
		}
	}
	return positions
}

func (b *programBatch) fail(outcome pb.MutationOutcome, failure *pb.Failure) {
	for i, result := range b.results {
		if result == nil {
			b.results[i] = protocol.Mutation(outcome, failure)
		}
	}
}

func (b *programBatch) cancelled(ctx context.Context) {
	for i, work := range b.plans {
		if b.results[i] == nil {
			if failure := programCallerFailure(ctx, work); failure != nil {
				b.results[i] = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
			}
		}
	}
}

func programCallerFailure(ctx context.Context, work *execution.Plan) *pb.Failure {
	if work.Context != nil && work.Context.Err() != nil {
		return protocol.ContextFailure(work.Context)
	}
	if ctx.Err() != nil {
		return protocol.ContextFailure(ctx)
	}
	return nil
}

func (a *Adapter) splitPrograms(ctx context.Context, batch *programBatch, positions []int) {
	for _, part := range [][]int{positions[:len(positions)/2], positions[len(positions)/2:]} {
		plans := make([]*execution.Plan, len(part))
		for i, position := range part {
			plans[i] = batch.plans[position]
		}
		child := &programBatch{plans: plans, results: make([]*pb.MutationResult, len(plans))}
		a.runPrograms(ctx, child)
		for i, position := range part {
			batch.results[position] = child.results[i]
		}
	}
	return
}

func (a *Adapter) readPrograms(ctx context.Context, plans []*execution.Plan) ([]bson.Raw, error) {
	target := plans[0].Backend.(*plan).target
	ids := make(bson.A, len(plans))
	positions := make(map[any]int, len(plans))
	for i, work := range plans {
		id := work.Backend.(*plan).id
		ids[i], positions[id] = id, i
	}
	selector := bson.D{{Key: "$in", Value: ids}}
	filter := bson.D{{Key: "_id", Value: selector}}
	command := bson.D{{Key: "find", Value: target.collection}, {Key: "filter", Value: filter}, {Key: "limit", Value: int64(len(ids))}, {Key: "batchSize", Value: int32(len(ids))}, {Key: "allowPartialResults", Value: false}}
	state := &recordCursor{target: target, items: len(ids)}
	documents := make([]bson.Raw, len(plans))
	held, received := 0, 0
	for page := 0; page < len(plans); page++ {
		raw, err := a.client.Database(target.database).RunCommand(ctx, command).Raw()
		if err != nil {
			return nil, err
		}
		fields, err := scanFields(raw)
		valid := err == nil && len(raw) <= scanNativeLimit && scanOK(fields["ok"])
		for key := range fields {
			if key != "ok" && key != "cursor" && key != "operationTime" && key != "$clusterTime" {
				valid = false
			}
		}
		var pageDocuments []bson.Raw
		if valid {
			pageDocuments, valid = cursorDocuments(fields, state, target.String(), page == 0)
		}
		if !valid || len(pageDocuments) > len(plans)-received || len(pageDocuments) == 0 && state.cursor != 0 {
			return nil, errProgramReadEvidence
		}
		for _, raw := range pageDocuments {
			id, validID := rawRecordID(raw.Lookup("_id"))
			position, found := positions[id]
			if !validID || !found || documents[position] != nil {
				return nil, errProgramReadEvidence
			}
			if len(raw) > protocol.MaxDocument {
				// Mark only this item as undecodable without retaining an
				// oversized backend document in the transaction workspace.
				documents[position] = bson.Raw{}
				received++
				continue
			}
			if len(raw) > programRetainedBytes-held {
				return nil, errProgramBatchBound
			}
			documents[position] = append(bson.Raw(nil), raw...)
			held += len(raw)
			received++
		}
		if state.cursor == 0 {
			return documents, nil
		}
		if received == len(plans) {
			return nil, errProgramReadEvidence
		}
		command = bson.D{{Key: "getMore", Value: state.cursor}, {Key: "collection", Value: target.collection}, {Key: "batchSize", Value: int32(len(plans) - received)}}
	}
	return nil, errProgramReadEvidence
}

func (a *Adapter) transformPrograms(ctx context.Context, batch *programBatch, positions []int, documents []bson.Raw) ([]*execution.Plan, []int, error) {
	writes := make([]*execution.Plan, 0, len(positions))
	writePositions := make([]int, 0, len(positions))
	held := 0
	for _, raw := range documents {
		held += len(raw)
	}
	for i, position := range positions {
		work := batch.plans[position]
		native := work.Backend.(*plan)
		raw := documents[i]
		documents[i] = nil
		held -= len(raw)
		if failure := programCallerFailure(ctx, work); failure != nil {
			batch.results[position] = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
			continue
		}
		missing := raw == nil
		current := value.Value{Kind: value.Missing}
		if !missing {
			var err error
			current, err = Decode(raw)
			if err != nil {
				failure := protocol.Fail(pb.FailureCode_UNSUPPORTED, "stored document contains unsupported BSON values")
				batch.results[position] = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
				continue
			}
		}
		program := *native.program
		program.Current = current
		evaluateContext, cancel := context.WithCancel(ctx)
		var stop func() bool
		if work.Context != nil {
			if deadline, exists := work.Context.Deadline(); exists {
				cancel()
				evaluateContext, cancel = context.WithDeadline(ctx, deadline)
			}
			stop = context.AfterFunc(work.Context, cancel)
		}
		transformed, transformErr := luaengine.Evaluate(evaluateContext, program)
		if stop != nil {
			stop()
		}
		cancel()
		if failure := programCallerFailure(ctx, work); failure != nil {
			batch.results[position] = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
			continue
		}
		if transformErr != nil {
			result := luaProgramFailure(ctx, transformErr)
			batch.results[position] = result
			continue
		}
		prepared := &plan{target: native.target, id: native.id}
		switch transformed.Action {
		case "keep":
			batch.results[position] = protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
			continue
		case "reject":
			failure := protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, transformed.Message)
			batch.results[position] = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
			continue
		case "delete":
			if missing {
				batch.results[position] = protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
				continue
			}
			prepared.action = "delete"
		case "replace":
			identity, identityErr := mongoIdentity(native.id)
			if !missing && identityErr == nil {
				storedID, lookupErr := current.Lookup("_id")
				if lookupErr != nil || !equalID(storedID, native.id) {
					identityErr = errProgramReadEvidence
				} else {
					identity.Value = storedID
				}
			}
			replacement, valid := withMongoIdentity(transformed.Value, identity, native.id)
			if identityErr != nil || !valid {
				failure := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "Lua replacement changes record identity")
				batch.results[position] = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
				continue
			}
			encoded, encodeErr := Encode(replacement)
			if encodeErr != nil {
				failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Lua replacement exceeds BSON limits")
				batch.results[position] = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
				continue
			}
			if len(encoded) > programRetainedBytes-held {
				return nil, nil, errProgramBatchBound
			}
			held += len(encoded)
			prepared.document = encoded
			prepared.action = "replace"
			if missing {
				prepared.action = "create"
			}
		default:
			failure := protocol.Fail(pb.FailureCode_INTERNAL, "Lua evaluation returned an invalid action")
			batch.results[position] = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
			continue
		}
		write := &execution.Plan{Command: work.Command, Backend: prepared}
		writes = append(writes, write)
		writePositions = append(writePositions, position)
	}
	return writes, writePositions, nil
}

func (a *Adapter) writePrograms(ctx context.Context, plans []*execution.Plan) ([]*pb.MutationResult, error) {
	target := plans[0].Backend.(*plan).target
	ops := make(bson.A, len(plans))
	for i, work := range plans {
		native := work.Backend.(*plan)
		filter := bson.D{{Key: "_id", Value: native.id}}
		var op bson.D
		switch native.action {
		case "create":
			op = bson.D{{Key: "insert", Value: int32(0)}, {Key: "document", Value: native.document}}
		case "replace":
			op = bson.D{{Key: "update", Value: int32(0)}, {Key: "filter", Value: filter}, {Key: "updateMods", Value: native.document}, {Key: "multi", Value: false}, {Key: "upsert", Value: false}}
		case "delete":
			op = bson.D{{Key: "delete", Value: int32(0)}, {Key: "filter", Value: filter}, {Key: "multi", Value: false}}
		}
		ops[i] = op
	}
	namespaceInfo := bson.D{{Key: "ns", Value: target.String()}}
	cursorOpts := bson.D{{Key: "batchSize", Value: int32(len(plans))}}
	command := bson.D{{Key: "bulkWrite", Value: int32(1)}, {Key: "ops", Value: ops}, {Key: "nsInfo", Value: bson.A{namespaceInfo}}, {Key: "ordered", Value: true}, {Key: "errorsOnly", Value: false}, {Key: "cursor", Value: cursorOpts}}
	state := &writeBatch{plans: plans, results: make([]*pb.MutationResult, len(plans))}
	state.cursor.items = len(plans)
	state.cursor.target = namespace{database: "admin", collection: "$cmd.bulkWrite"}
	for page := 0; page < len(plans); page++ {
		raw, err := a.client.Database("admin").RunCommand(ctx, command).Raw()
		if err != nil {
			return nil, err
		}
		valid := state.reply(raw, page == 0)
		if !valid {
			return nil, errProgramReadEvidence
		}
		if state.cursor.cursor == 0 {
			return state.results, nil
		}
		command = bson.D{{Key: "getMore", Value: state.cursor.cursor}, {Key: "collection", Value: "$cmd.bulkWrite"}, {Key: "batchSize", Value: int32(len(plans) - state.received)}}
	}
	return nil, errProgramReadEvidence
}

func mongoIdentity(id any) (value.Field, error) {
	identityDocument := bson.D{{Key: "_id", Value: id}}
	raw, err := bson.Marshal(identityDocument)
	if err != nil {
		var zero value.Field
		return zero, err
	}
	identity, err := Decode(raw)
	if err != nil || len(identity.Fields) != 1 {
		var zero value.Field
		return zero, fmt.Errorf("invalid encoded identity")
	}
	return identity.Fields[0], nil
}

func withMongoIdentity(document value.Value, identity value.Field, id any) (value.Value, bool) {
	if document.Kind != value.Object || value.Validate(document) != nil {
		var zero value.Value
		return zero, false
	}
	existing, err := document.Lookup("_id")
	if err != nil || existing.Kind != value.Missing && !equalID(existing, id) {
		var zero value.Value
		return zero, false
	}
	result := value.Value{Kind: value.Object, Fields: make([]value.Field, 0, len(document.Fields)+1)}
	result.Fields = append(result.Fields, identity)
	for _, field := range document.Fields {
		if field.Name != "_id" {
			result.Fields = append(result.Fields, field)
		}
	}
	if err := value.Validate(result); err != nil {
		var zero value.Value
		return zero, false
	}
	return result, true
}

func luaProgramFailure(ctx context.Context, err error) *pb.MutationResult {
	if ctx.Err() != nil {
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.ContextFailure(ctx))
	}
	code := pb.FailureCode_INVALID_ARGUMENT
	message := "Lua program evaluation failed"
	if errors.Is(err, context.DeadlineExceeded) {
		code = pb.FailureCode_DEADLINE_EXCEEDED
		message = "Lua program execution limit exceeded"
	}
	failure := protocol.Fail(code, message)
	return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
}

func programHasLabel(err error, label string) bool {
	var serverError mongo.ServerError
	return errors.As(err, &serverError) && serverError.HasErrorLabel(label)
}

func (a *Adapter) abortProgramTransaction(session *mongo.Session) bool {
	cleanup, cancel := context.WithTimeout(context.Background(), programCleanup)
	defer cancel()
	bounded, release := nativeAttemptContext(cleanup)
	defer release()
	ctx := mongo.NewSessionContext(bounded, session)
	concern := bson.D{{Key: "w", Value: "majority"}}
	command := bson.D{{Key: "abortTransaction", Value: int32(1)}, {Key: "writeConcern", Value: concern}}
	raw, err := a.client.Database("admin").RunCommand(ctx, command).Raw()
	// The driver suppresses abort wire errors. Use it only to advance local
	// state after the separately observed acknowledgement; cancellation keeps
	// that state transition from sending a second abort command.
	local, stop := context.WithCancel(context.Background())
	stop()
	_ = session.AbortTransaction(local)
	if err == nil {
		fields, parseErr := scanFields(raw)
		return parseErr == nil && scanOK(fields["ok"]) && fields["writeConcernError"].Type == 0
	}
	var commandError mongo.CommandError
	// This helper is only used before any commit is dispatched, so an explicit
	// NoSuchTransaction reply proves the uncommitted transaction is absent.
	return errors.As(err, &commandError) && commandError.Code == 251
}

func pauseProgram(ctx context.Context) bool {
	timer := time.NewTimer(5 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
