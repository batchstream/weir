package mongodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
	"github.com/batchstream/weir/internal/value"
	"github.com/iceisfun/golua/budget"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

var errProgramBatchBound = errors.New("program batch requires another native exchange")
var errProgramReadEvidence = errors.New("program read acknowledgement incomplete")

type programBatch struct {
	plans   []*execution.Plan
	results []*pb.MutationResult
}

// Programs with one target and distinct typed identities share a snapshot
// transaction. Caller contexts govern evaluation; the batch context governs IO.
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
		cleanup, cancel := context.WithTimeout(context.Background(), a.options().CleanupTimeout)
		defer cancel()
		if ambiguous {
			cancel()
		}
		session.EndSession(cleanup)
	}()
	for attempt := 0; attempt < a.options().RetryAttempts && ctx.Err() == nil; attempt++ {
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
		state := &programAttempt{batch: batch, session: session, active: active, number: attempt, ambiguous: &ambiguous}
		switch a.runProgramAttempt(ctx, state) {
		case programAttemptFinished:
			return
		case programAttemptSplit:
			a.splitPrograms(ctx, batch, batch.active())
			return
		case programAttemptRetry:
			continue
		}
		break
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

type programAttemptStatus uint8

const (
	programAttemptFinished programAttemptStatus = iota
	programAttemptRetry
	programAttemptSplit
	programAttemptExhausted
)

type programAttempt struct {
	batch     *programBatch
	session   *mongo.Session
	active    []int
	number    int
	ambiguous *bool
}

// Each attempt retires its retained documents before another snapshot, split,
// or group can reuse the shared record window. Results have separate owners.
type programMemoryScope struct {
	memory    *recordMemory
	held      int
	documents []bson.Raw
	writes    []*execution.Plan
}

func newProgramMemoryScope(ctx context.Context) *programMemoryScope {
	scope := &programMemoryScope{}
	if memory, ok := ctx.Value(recordMemoryContextKey).(*recordMemory); ok {
		scope.memory, scope.held = memory, memory.held
	}
	return scope
}

func (s *programMemoryScope) close() {
	clear(s.documents[:cap(s.documents)])
	s.documents = nil
	clear(s.writes[:cap(s.writes)])
	s.writes = nil
	if s.memory != nil {
		s.memory.held = s.held
	}
}

func (s *programMemoryScope) filterCancelled(ctx context.Context, batch *programBatch, positions []int) []int {
	kept := 0
	for i, position := range positions {
		work := batch.plans[position]
		if failure := programCallerFailure(ctx, work); failure != nil {
			batch.results[position] = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
			continue
		}
		s.writes[kept], positions[kept] = s.writes[i], position
		kept++
	}
	// Truncation alone leaves cancelled replacements reachable in the backing array.
	clear(s.writes[kept:])
	s.writes = s.writes[:kept]
	return positions[:kept]
}

func (a *Adapter) runProgramAttempt(ctx context.Context, state *programAttempt) programAttemptStatus {
	scope := newProgramMemoryScope(ctx)
	defer scope.close()
	batch, session, active, attempt := state.batch, state.session, state.active, state.number
	txctx := mongo.NewSessionContext(ctx, session)
	readPlans := make([]*execution.Plan, len(active))
	for i, position := range active {
		readPlans[i] = batch.plans[position]
	}
	var readErr error
	scope.documents, readErr = a.readPrograms(txctx, readPlans)
	if readErr != nil {
		var commandError mongo.CommandError
		settled := errors.As(readErr, &commandError) && len(commandError.Raw) != 0 || errors.Is(readErr, errProgramBatchBound)
		aborted := a.abortProgramTransaction(session, settled)
		if !aborted {
			batch.fail(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_UNAVAILABLE, "MongoDB transaction rollback unconfirmed"))
			return programAttemptFinished
		}
		if errors.Is(readErr, errProgramBatchBound) && len(active) > 1 {
			return programAttemptSplit
		}
		if programHasLabel(readErr, "TransientTransactionError") && pauseProgram(ctx, a.options().RetryBackoff) {
			return programAttemptRetry
		}
		failure := backendFailure(ctx, readErr)
		if errors.Is(readErr, execution.ErrExecutionMemory) {
			failure = recordMemoryFailure()
		}
		batch.fail(pb.MutationOutcome_NOT_APPLIED, failure)
		return programAttemptFinished
	}
	var writePositions []int
	scope.writes, writePositions = a.transformPrograms(txctx, batch, active, scope.documents)
	// A cancellation during another member's Lua evaluation only excludes
	// this caller's not-yet-sent write. Nothing is retracted after W starts.
	writePositions = scope.filterCancelled(ctx, batch, writePositions)
	if len(scope.writes) == 0 {
		a.abortProgramTransaction(session, true)
		return programAttemptFinished
	}
	replies, settled, writeErr := a.writePrograms(txctx, scope.writes)
	if writeErr != nil {
		var commandError mongo.CommandError
		settled := errors.As(writeErr, &commandError) && len(commandError.Raw) != 0 || errors.Is(writeErr, errProgramBatchBound)
		aborted := a.abortProgramTransaction(session, settled)
		if !aborted {
			batch.fail(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_UNAVAILABLE, "MongoDB transaction rollback unconfirmed"))
			return programAttemptFinished
		}
		if errors.Is(writeErr, errProgramBatchBound) && len(batch.active()) > 1 {
			return programAttemptSplit
		}
		if programHasLabel(writeErr, "TransientTransactionError") && pauseProgram(ctx, a.options().RetryBackoff) {
			return programAttemptRetry
		}
		failure := backendFailure(ctx, writeErr)
		if errors.Is(writeErr, execution.ErrExecutionMemory) {
			failure = recordMemoryFailure()
		}
		batch.fail(pb.MutationOutcome_NOT_APPLIED, failure)
		return programAttemptFinished
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
			if reply.GetFailure().GetCode() == pb.FailureCode_CONFLICT || scope.writes[i].Backend.(*plan).action == "create" && attempt == 0 {
				retry = true
			}
		}
	}
	if rejected {
		aborted := a.abortProgramTransaction(session, settled)
		if !aborted {
			batch.fail(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_UNAVAILABLE, "MongoDB transaction rollback unconfirmed"))
			return programAttemptFinished
		}
		if !retry {
			for i, reply := range replies {
				if reply != nil && reply.Outcome == pb.MutationOutcome_NOT_APPLIED {
					batch.results[writePositions[i]] = reply
				}
			}
			if len(batch.active()) == len(active) {
				batch.fail(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_UNAVAILABLE, "MongoDB batch write acknowledgement incomplete"))
				return programAttemptFinished
			}
		}
		if pauseProgram(ctx, a.options().RetryBackoff) {
			return programAttemptRetry
		}
		return programAttemptExhausted
	}
	retryTransaction := false
	for commitAttempt := 0; commitAttempt < a.options().RetryAttempts && ctx.Err() == nil; commitAttempt++ {
		commitContext, release := nativeAttemptContext(txctx)
		err := session.CommitTransaction(commitContext)
		release()
		if err == nil {
			batch.fail(pb.MutationOutcome_APPLIED, nil)
			return programAttemptFinished
		}
		if !*state.ambiguous && !programHasLabel(err, "UnknownTransactionCommitResult") && programHasLabel(err, "TransientTransactionError") {
			retryTransaction = true
			break
		}
		*state.ambiguous = true
		if !pauseProgram(ctx, a.options().RetryBackoff) {
			break
		}
	}
	if !retryTransaction || !pauseProgram(ctx, a.options().RetryBackoff) {
		return programAttemptExhausted
	}
	return programAttemptRetry
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

// MongoDB applies its native response boundary. Only an explicitly smaller
// exchange target adds a conservative document-count hint to cursor pages.
func (a *Adapter) programBatchSize(items int) int32 {
	nativeBytes := a.maxDocumentBytes
	if nativeBytes <= 0 {
		nativeBytes = 16 << 20
	}
	if a.options().ExchangeBytes >= nativeBytes {
		return int32(items)
	}
	return int32(min(items, max(1, a.options().ExchangeBytes/a.documentBytes())))
}

func (a *Adapter) readPrograms(ctx context.Context, plans []*execution.Plan) ([]bson.Raw, error) {
	ctx, release := nativeAttemptContext(ctx)
	defer release()
	target := plans[0].Backend.(*plan).target
	ids := make(bson.A, len(plans))
	positions := make(map[any]int, len(plans))
	for i, work := range plans {
		id := work.Backend.(*plan).id
		ids[i], positions[id] = id, i
	}
	selector := bson.D{{Key: "$in", Value: ids}}
	filter := bson.D{{Key: "_id", Value: selector}}
	var command any = bson.D{{Key: "find", Value: target.collection}, {Key: "filter", Value: filter}, {Key: "limit", Value: int64(len(ids))}, {Key: "batchSize", Value: a.programBatchSize(len(ids))}, {Key: "allowPartialResults", Value: false}}
	encoded, err := bson.Marshal(command)
	if err != nil {
		return nil, err
	}
	if len(plans) > 1 && len(encoded) > a.commandBytes() {
		return nil, errProgramBatchBound
	}
	command = bson.Raw(encoded)
	state := &recordCursor{target: target, items: len(ids)}
	scope := newProgramMemoryScope(ctx)
	complete := false
	defer func() {
		if !complete {
			scope.close()
		}
	}()
	documents := make([]bson.Raw, len(plans))
	scope.documents = documents
	received := 0
	for page := 0; page <= len(plans); page++ {
		if err := reserveRecordWorking(ctx, plans, 0); err != nil {
			return nil, err
		}
		raw, err := a.client.Database(target.database).RunCommand(ctx, command).Raw()
		if err != nil {
			return nil, err
		}
		fields, err := bsonFields(raw)
		valid := err == nil && bsonCommandOK(fields["ok"])
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
			if err := reserveRecordWorking(ctx, plans, len(raw)); err != nil {
				return nil, err
			}
			if memory, ok := ctx.Value(recordMemoryContextKey).(*recordMemory); ok {
				memory.held += len(raw)
			}
			document := make([]byte, len(raw))
			copy(document, raw)
			documents[position] = document
			received++
		}
		if state.cursor == 0 {
			// The attempt takes ownership only after complete read evidence.
			complete = true
			return documents, nil
		}
		command = bson.D{{Key: "getMore", Value: state.cursor}, {Key: "collection", Value: target.collection}, {Key: "batchSize", Value: a.programBatchSize(max(1, len(plans)-received))}}
	}
	return nil, errProgramReadEvidence
}

func (a *Adapter) transformPrograms(ctx context.Context, batch *programBatch, positions []int, documents []bson.Raw) ([]*execution.Plan, []int) {
	writes := make([]*execution.Plan, 0, len(positions))
	writePositions := make([]int, 0, len(positions))
	for i, position := range positions {
		work := batch.plans[position]
		raw := documents[i]
		documents[i] = nil
		write, result := transformProgram(ctx, work, raw)
		bytes := len(raw)
		raw = nil
		if memory, ok := ctx.Value(recordMemoryContextKey).(*recordMemory); ok {
			memory.held -= bytes
		}
		if result != nil {
			batch.results[position] = result
		} else {
			writes = append(writes, write)
			writePositions = append(writePositions, position)
		}
	}
	return writes, writePositions
}

func transformProgram(ctx context.Context, work *execution.Plan, raw bson.Raw) (write *execution.Plan, result *pb.MutationResult) {
	if failure := programCallerFailure(ctx, work); failure != nil {
		return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
	}
	if work.Context != nil {
		programContext, cancel := context.WithCancel(ctx)
		if deadline, exists := work.Context.Deadline(); exists {
			cancel()
			programContext, cancel = context.WithDeadline(ctx, deadline)
		}
		stop := context.AfterFunc(work.Context, cancel)
		defer stop()
		defer cancel()
		ctx = programContext
	}
	var allocationErr error
	defer func() {
		if allocationErr != nil {
			write = nil
			failure := fmt.Errorf("%w: %w", execution.ErrExecutionMemory, allocationErr)
			result = execution.LuaFailure(ctx, failure)
		}
	}()
	defer budget.Recover(&allocationErr)
	input := work.Command.GetMutate().GetAtomicTransform().GetLua().GetInput()
	inputBytes := []byte(nil)
	if input != nil {
		inputBytes = input.Data
	}
	plans := []*execution.Plan{work}
	decodedWorking, err := programWorking(bsonDecodeWorking(raw), bsonDecodeWorking(inputBytes))
	if err != nil {
		return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
	}
	if err := reserveRecordWorking(ctx, plans, decodedWorking); err != nil {
		return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
	}
	native := work.Backend.(*plan)
	missing := raw == nil
	current := value.Value{Kind: value.Missing}
	if !missing {
		var err error
		current, err = Decode(ctx, raw)
		if err != nil {
			if failure := programCallerFailure(ctx, work); failure != nil {
				return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
			}
			failure := protocol.Fail(pb.FailureCode_UNSUPPORTED, "stored document contains unsupported BSON values")
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
		}
	}
	if native.program.ObservedAt.IsZero() {
		native.program.ObservedAt = time.Now()
	}
	program := *native.program
	program.Current = current
	luaBytes, programScratch := 0, decodedWorking
	memory, managed := ctx.Value(recordMemoryContextKey).(*recordMemory)
	if managed && memory.owner != nil || !managed && work.Memory != nil {
		program.Reserve = func(bytes int) error {
			working, err := programWorking(programScratch, bytes)
			if err != nil {
				return err
			}
			if err := reserveRecordWorking(ctx, plans, working); err != nil {
				return err
			}
			luaBytes = bytes
			return nil
		}
	}
	if input != nil {
		decoded, err := Decode(ctx, input.Data)
		if err != nil {
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid BSON transform input"))
		}
		program.Input = decoded
	}
	transformed, transformErr := luaengine.Evaluate(ctx, program)
	if failure := programCallerFailure(ctx, work); failure != nil {
		return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
	}
	if transformErr != nil {
		return nil, execution.LuaFailure(ctx, transformErr)
	}
	luaBytes = transformed.Allocations.UsedBytes()
	prepared := &plan{target: native.target, id: native.id}
	switch transformed.Action {
	case "keep":
		return nil, protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
	case "reject":
		if work.Memory != nil {
			retained, err := programWorking(len(transformed.Message), execution.ResultOverheadBytes)
			if err != nil {
				return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
			}
			if err := work.Memory.ReserveResult(retained); err != nil {
				return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
			}
		}
		failure := protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, transformed.Message)
		return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
	case "delete":
		if missing {
			return nil, protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
		}
		prepared.action = "delete"
	case "replace":
		// The bounded identity codec and the replacement's field copy run
		// before Encode. Their validation maps belong to the same Lua scope.
		transformed.Allocations.RequireAllocation(4096)
		identity, identityErr := mongoIdentity(ctx, native.id)
		if !missing && identityErr == nil {
			storedID, lookupErr := current.Lookup("_id")
			if lookupErr != nil || !equalID(storedID, native.id) {
				identityErr = errProgramReadEvidence
			} else {
				identity.Value = storedID
			}
		}
		var field value.Field
		transformed.Allocations.RequireArray(unsafe.Sizeof(field), len(transformed.Value.Fields)+1)
		if err := luaengine.ReserveValueValidation(transformed.Allocations, transformed.Value); err != nil {
			return nil, execution.LuaFailure(ctx, err)
		}
		if err := luaengine.ReserveValueValidation(transformed.Allocations, transformed.Value); err != nil {
			return nil, execution.LuaFailure(ctx, err)
		}
		transformed.Allocations.RequireAllocation(1024)
		replacement, valid := withMongoIdentity(ctx, transformed.Value, identity, native.id)
		if identityErr != nil || !valid {
			if failure := programCallerFailure(ctx, work); failure != nil {
				return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
			}
			failure := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "Lua replacement changes record identity")
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
		}
		valueBytes, err := transformed.ValueBytes(ctx, replacement, false)
		if err != nil {
			return nil, execution.LuaFailure(ctx, err)
		}
		programScratch, err = programWorking(decodedWorking, valueBytes, valueBytes, valueBytes, valueBytes)
		if err != nil {
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
		}
		working, err := programWorking(programScratch, luaBytes)
		if err != nil {
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
		}
		if err := reserveRecordWorking(ctx, plans, working); err != nil {
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
		}
		encoded, encodeErr := Encode(ctx, replacement)
		if encodeErr != nil {
			if failure := programCallerFailure(ctx, work); failure != nil {
				return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
			}
			failure := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "Lua replacement contains invalid BSON values")
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
		}
		if !missing && bytes.Equal(encoded, raw) {
			return nil, protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
		}
		prepared.document = encoded
		if memory, ok := ctx.Value(recordMemoryContextKey).(*recordMemory); ok {
			memory.held += cap(encoded)
		}
		prepared.action = "replace"
		if missing {
			prepared.action = "create"
		}
	default:
		failure := protocol.Fail(pb.FailureCode_INTERNAL, "Lua evaluation returned an invalid action")
		return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
	}
	if failure := programCallerFailure(ctx, work); failure != nil {
		return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
	}
	write = &execution.Plan{Command: work.Command, Backend: prepared, Memory: work.Memory}
	return write, nil
}

func (a *Adapter) writePrograms(ctx context.Context, plans []*execution.Plan) ([]*pb.MutationResult, bool, error) {
	ctx, release := nativeAttemptContext(ctx)
	defer release()
	framing := 0
	for _, work := range plans {
		framing += 2*len(work.Backend.(*plan).document) + execution.EntryOverheadBytes
	}
	if err := reserveRecordWorking(ctx, plans, framing); err != nil {
		return nil, true, err
	}
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
	var command any = bson.D{{Key: "bulkWrite", Value: int32(1)}, {Key: "ops", Value: ops}, {Key: "nsInfo", Value: bson.A{namespaceInfo}}, {Key: "ordered", Value: true}, {Key: "errorsOnly", Value: false}, {Key: "cursor", Value: cursorOpts}}
	encoded, err := bson.Marshal(command)
	if err != nil {
		return nil, false, err
	}
	if len(plans) > 1 && len(encoded) > a.commandBytes() {
		return nil, false, errProgramBatchBound
	}
	command = bson.Raw(encoded)
	state := &writeBatch{plans: plans, results: make([]*pb.MutationResult, len(plans))}
	state.cursor.items = len(plans)
	state.cursor.target = namespace{database: "admin", collection: "$cmd.bulkWrite"}
	for page := 0; page < len(plans); page++ {
		raw, err := a.client.Database("admin").RunCommand(ctx, command).Raw()
		if err != nil {
			return nil, false, err
		}
		valid := state.reply(raw, page == 0)
		if !valid {
			return nil, false, errProgramReadEvidence
		}
		if state.cursor.cursor == 0 {
			settled := !state.concern && !state.uncertainCounts && state.counts == state.observed
			for i, reply := range state.results {
				// Ordered writes stop at the first native error. A verified
				// error and matching totals prove the nil tail was not sent.
				if reply == nil {
					settled = settled && state.observed[0] == 1 && i >= state.received
				} else if reply.Outcome == pb.MutationOutcome_UNKNOWN {
					settled = false
				}
			}
			return state.results, settled, nil
		}
		command = bson.D{{Key: "getMore", Value: state.cursor.cursor}, {Key: "collection", Value: "$cmd.bulkWrite"}, {Key: "batchSize", Value: int32(len(plans) - state.received)}}
	}
	return nil, false, errProgramReadEvidence
}

func mongoIdentity(ctx context.Context, id any) (value.Field, error) {
	identityDocument := bson.D{{Key: "_id", Value: id}}
	raw, err := bson.Marshal(identityDocument)
	if err != nil {
		var zero value.Field
		return zero, err
	}
	identity, err := Decode(ctx, raw)
	if err != nil || len(identity.Fields) != 1 {
		var zero value.Field
		return zero, fmt.Errorf("invalid encoded identity")
	}
	return identity.Fields[0], nil
}

func withMongoIdentity(ctx context.Context, document value.Value, identity value.Field, id any) (value.Value, bool) {
	if document.Kind != value.Object || value.Validate(ctx, document) != nil {
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
	if err := value.Validate(ctx, result); err != nil {
		var zero value.Value
		return zero, false
	}
	return result, true
}

func programHasLabel(err error, label string) bool {
	var serverError mongo.ServerError
	return errors.As(err, &serverError) && serverError.HasErrorLabel(label)
}

func (a *Adapter) abortProgramTransaction(session *mongo.Session, settled bool) bool {
	cleanup, cancel := context.WithTimeout(context.Background(), a.options().CleanupTimeout)
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
		fields, parseErr := bsonFields(raw)
		return parseErr == nil && bsonCommandOK(fields["ok"]) && fields["writeConcernError"].Type == 0
	}
	var commandError mongo.CommandError
	if !errors.As(err, &commandError) || commandError.Code != 251 {
		return false
	}
	// A complete response settles the last command on this exclusively owned
	// session. NoSuchTransaction then confirms the server already aborted it.
	if settled {
		return true
	}
	// Kill any work still in flight on this exclusively owned session. Future
	// operations can reuse a killed session, so neither NoSuchTransaction nor
	// this best-effort cleanup proves rollback or permits rebuilding it.
	retire := bson.D{{Key: "killSessions", Value: bson.A{session.ID()}}}
	_ = a.client.Database("admin").RunCommand(bounded, retire).Err()
	return false
}

func pauseProgram(ctx context.Context, backoff time.Duration) bool {
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
