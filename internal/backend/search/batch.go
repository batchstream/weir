package search

import (
	"bytes"
	"context"
	"encoding/json"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
)

const batchBodyLimit = 8 << 20
const batchOperationLimit = 128
const getFramingLimit = 32 << 10
const getBatchItems = (batchBodyLimit - getFramingLimit) / (protocol.MaxDocument + getFramingLimit)

type observedRecord struct {
	reply    *getReply
	failure  *pb.Failure
	feedback execution.Feedback
}

// mget uses real-time source and preserves the entire request's positional ID
// correspondence before publishing any document. The response and JSON walker
// are bounded for every document in each sequential request.
func (a *Adapter) mget(ctx context.Context, works []*execution.Plan) []observedRecord {
	observed := make([]observedRecord, len(works))
	for start := 0; start < len(works); start += getBatchItems {
		end := min(start+getBatchItems, len(works))
		group := make([]*execution.Plan, 0, end-start)
		indexes := make([]int, 0, end-start)
		ids := make([]string, 0, end-start)
		for i := start; i < end; i++ {
			work := works[i]
			caller := ctx
			if work.Context != nil && work.Context.Err() != nil {
				caller = work.Context
			}
			if caller.Err() != nil {
				observation := observedRecord{failure: protocol.ContextFailure(caller), feedback: execution.Neutral}
				observed[i] = observation
				continue
			}
			group = append(group, work)
			indexes = append(indexes, i)
			ids = append(ids, work.Backend.(*plan).id)
		}
		if len(group) == 0 {
			continue
		}
		body := map[string]any{"ids": ids}
		encoded, _ := json.Marshal(body)
		call := exchange{
			path: "/" + a.config.Index + "/_mget?realtime=true", body: encoded,
			contentType: "application/json", limit: len(group)*(protocol.MaxDocument+getFramingLimit) + getFramingLimit,
			jsonNodes: len(group)*(16384+32) + 1,
		}
		status, raw, err := a.request(ctx, call)
		failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "record response unavailable or incomplete")
		feedback := execution.Neutral
		switch {
		case ctx.Err() != nil:
			failure = protocol.ContextFailure(ctx)
		case err == errTransport:
			feedback = execution.Congested
		case err == nil && (status == 429 || status == 503):
			failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend capacity unavailable")
			feedback = execution.Congested
		}
		for _, i := range indexes {
			observation := observedRecord{failure: failure, feedback: feedback}
			observed[i] = observation
		}
		if err != nil || status != 200 {
			continue
		}
		var envelope struct {
			Docs []getReply `json:"docs"`
		}
		if json.Unmarshal(raw, &envelope) != nil || len(envelope.Docs) != len(group) {
			continue
		}
		corresponds := true
		for i, reply := range envelope.Docs {
			if reply.Index != a.config.Index || reply.ID != ids[i] {
				corresponds = false
				break
			}
		}
		if !corresponds {
			continue
		}
		for i := range envelope.Docs {
			reply := &envelope.Docs[i]
			observation := observedRecord{reply: reply, feedback: execution.Healthy}
			switch {
			case reply.Error != nil:
				observation.reply = nil
				observation.failure = failure
				observation.feedback = execution.Neutral
				if reply.Found == nil && reply.Seq == nil && reply.Term == nil && len(reply.Source) == 0 {
					if rejected, sample := a.reject(reply.Error.Type, reply.Status); rejected != nil {
						observation.failure, observation.feedback = rejected, sample
					} else if reply.Error.Type == "index_not_found_exception" {
						observation.failure = protocol.Fail(pb.FailureCode_NOT_FOUND, "configured index missing")
					}
				}
			case reply.Found == nil || reply.Status != 0:
				observation.reply, observation.failure, observation.feedback = nil, failure, execution.Neutral
			case !*reply.Found:
				if reply.Seq != nil || reply.Term != nil || len(reply.Source) != 0 {
					observation.reply, observation.failure, observation.feedback = nil, failure, execution.Neutral
				}
			case reply.Seq == nil || reply.Term == nil || *reply.Seq < 0 || *reply.Term < 1 || !object(reply.Source):
				observation.reply, observation.failure, observation.feedback = nil, failure, execution.Neutral
			case validateJSON(reply.Source, 16384) != nil:
				observation.reply, observation.failure, observation.feedback = nil, failure, execution.Neutral
			case len(reply.Source) > protocol.MaxDocument:
				observation.reply = nil
				observation.failure = protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "stored record exceeds read limit")
				observation.feedback = execution.Neutral
			}
			observed[indexes[i]] = observation
		}
	}
	return observed
}

func combineFeedback(current, next execution.Feedback) execution.Feedback {
	if current == execution.Congested || next == execution.Congested {
		return execution.Congested
	}
	if current == execution.Neutral || next == execution.Neutral {
		return execution.Neutral
	}
	return execution.Healthy
}

type programExecution struct {
	ctx      context.Context
	cancel   context.CancelFunc
	stop     func() bool
	attempts int
}

type recordWrite struct {
	work                 *execution.Plan
	position, start, end int
}

type recordBatch struct {
	adapter  *Adapter
	ctx      context.Context
	results  []*pb.BulkResult
	programs []*programExecution
	feedback execution.Feedback
	request  bytes.Buffer
	pending  []recordWrite
	retry    []int
}

func (b *recordBatch) callerFailure(work *execution.Plan, position int) *pb.Failure {
	if work.Context != nil && work.Context.Err() != nil {
		return protocol.ContextFailure(work.Context)
	}
	if b.ctx.Err() != nil {
		return protocol.ContextFailure(b.ctx)
	}
	if program := b.programs[position]; program != nil && program.ctx.Err() != nil {
		return protocol.Fail(pb.FailureCode_DEADLINE_EXCEEDED, "Lua transform execution deadline exceeded")
	}
	return nil
}

func (b *recordBatch) closePrograms() {
	for _, program := range b.programs {
		if program == nil {
			continue
		}
		if program.stop != nil {
			program.stop()
		}
		program.cancel()
	}
}

// appendWrite frames one document at a time. Generated Lua sources are released
// after framing; the batch retains at most one bounded NDJSON request.
func (b *recordBatch) appendWrite(work *execution.Plan, position int, current *getReply) {
	native := work.Backend.(*plan)
	metadata := map[string]any{"_index": b.adapter.config.Index, "_id": native.id}
	action := native.action
	switch action {
	case "expression":
		action = "update"
		metadata["retry_on_conflict"] = 0
	case "replace":
		action = "index"
		metadata["pipeline"] = "_none"
	default:
		if action != "delete" {
			metadata["pipeline"] = "_none"
		}
	}
	if current != nil && *current.Found {
		metadata["if_seq_no"] = *current.Seq
		metadata["if_primary_term"] = *current.Term
	}
	header := map[string]any{action: metadata}
	encoded, _ := json.Marshal(header)
	var entry bytes.Buffer
	entry.Write(encoded)
	entry.WriteByte('\n')
	if native.source != nil {
		if err := json.Compact(&entry, native.source); err != nil {
			panic("validated source changed")
		}
		entry.WriteByte('\n')
	}
	if b.request.Len()+entry.Len() > batchBodyLimit {
		b.flush()
	}
	start := b.request.Len()
	b.request.Write(entry.Bytes())
	pending := recordWrite{work: work, position: position, start: start, end: b.request.Len()}
	b.pending = append(b.pending, pending)
	if native.program != nil {
		native.source = nil
	}
}

func (b *recordBatch) flush() {
	if len(b.pending) == 0 {
		return
	}
	body := b.request.Bytes()
	length := 0
	works := make([]*execution.Plan, 0, len(b.pending))
	positions := make([]int, 0, len(b.pending))
	for _, item := range b.pending {
		if failure := b.callerFailure(item.work, item.position); failure != nil {
			b.results[item.position] = protocol.ResultError(item.work.Operation, pb.MutationOutcome_NOT_APPLIED, failure)
			b.feedback = combineFeedback(b.feedback, execution.Neutral)
			continue
		}
		length += copy(body[length:], body[item.start:item.end])
		works = append(works, item.work)
		positions = append(positions, item.position)
	}
	if len(works) != 0 {
		call := exchange{path: "/_bulk?pipeline=_none&refresh=false&wait_for_active_shards=1&timeout=1s", body: body[:length], limit: responseLimit}
		status, raw, err := b.adapter.request(b.ctx, call)
		replies, sample := b.adapter.bulkResults(works, status, raw, err)
		b.feedback = combineFeedback(b.feedback, sample)
		for i, position := range positions {
			b.results[position] = replies[i]
			program := b.programs[position]
			mutation := replies[i].GetMutation()
			if program == nil || mutation.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || mutation.GetFailure().GetCode() != pb.FailureCode_CONFLICT {
				continue
			}
			if program.attempts < programAttempts {
				b.retry = append(b.retry, position)
			} else {
				failure := protocol.Fail(pb.FailureCode_CONFLICT, "Search record changed during every transform attempt")
				b.results[position] = protocol.ResultError(works[i].Operation, pb.MutationOutcome_NOT_APPLIED, failure)
			}
		}
	}
	b.request.Reset()
	clear(b.pending)
	b.pending = b.pending[:0]
}

func (a *Adapter) Execute(ctx context.Context, works []*execution.Plan) ([]*pb.BulkResult, execution.Feedback) {
	if len(works) == 0 {
		return nil, execution.Neutral
	}
	batch := recordBatch{adapter: a, ctx: ctx, results: make([]*pb.BulkResult, len(works)), programs: make([]*programExecution, len(works)), feedback: execution.Healthy}
	defer batch.closePrograms()
	totalBytes := 0
	for _, work := range works {
		totalBytes += work.Bytes
	}
	if len(works) > batchOperationLimit || totalBytes > batchBodyLimit {
		failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "batch exceeds execution bounds")
		for i, work := range works {
			batch.results[i] = protocol.ResultError(work.Operation, pb.MutationOutcome_NOT_STARTED, failure)
		}
		return batch.results, execution.Neutral
	}
	positions := make([]int, 0, len(works))
	for i, work := range works {
		if failure := batch.callerFailure(work, i); failure != nil {
			batch.results[i] = protocol.ResultError(work.Operation, pb.MutationOutcome_NOT_STARTED, failure)
			batch.feedback = combineFeedback(batch.feedback, execution.Neutral)
			continue
		}
		if work.Backend.(*plan).action == "program" {
			programContext, cancel := context.WithTimeout(ctx, programLifetime)
			program := &programExecution{ctx: programContext, cancel: cancel}
			if work.Context != nil {
				program.stop = context.AfterFunc(work.Context, cancel)
			}
			batch.programs[i] = program
		}
		positions = append(positions, i)
	}
	if len(positions) == 0 {
		return batch.results, batch.feedback
	}
	caps, failure, sample := a.inspect(ctx, false)
	if failure != nil {
		batch.feedback = combineFeedback(batch.feedback, sample)
	}
	for _, i := range positions {
		work := works[i]
		native := work.Backend.(*plan)
		denied := failure
		if denied == nil {
			switch {
			case native.action == "read" && !caps.source:
				denied = protocol.Fail(pb.FailureCode_UNSUPPORTED, "stored full source required for this operation")
			case (native.action == "program" || native.action == "expression") && (!caps.source || !caps.nativeWrite):
				denied = protocol.Fail(pb.FailureCode_UNSUPPORTED, "transform requires full stored source and no default or final pipeline")
			case native.action != "read" && native.action != "delete" && !caps.write:
				denied = protocol.Fail(pb.FailureCode_UNSUPPORTED, "stored full source and no final pipeline required for this operation")
			}
		}
		if denied != nil {
			batch.results[i] = protocol.ResultError(work.Operation, pb.MutationOutcome_NOT_APPLIED, denied)
			batch.feedback = combineFeedback(batch.feedback, execution.Neutral)
		}
	}
	for len(positions) != 0 {
		readWorks := make([]*execution.Plan, 0, len(positions))
		readPositions := make([]int, 0, len(positions))
		for _, i := range positions {
			work := works[i]
			if batch.results[i] != nil {
				continue
			}
			if denied := batch.callerFailure(work, i); denied != nil {
				batch.results[i] = protocol.ResultError(work.Operation, pb.MutationOutcome_NOT_APPLIED, denied)
				batch.feedback = combineFeedback(batch.feedback, execution.Neutral)
				continue
			}
			action := work.Backend.(*plan).action
			if action == "read" || action == "replace" || action == "program" {
				readWorks = append(readWorks, work)
				readPositions = append(readPositions, i)
			}
		}
		observed := make([]*getReply, len(works))
		for offset, record := range a.mget(ctx, readWorks) {
			i := readPositions[offset]
			work := works[i]
			batch.feedback = combineFeedback(batch.feedback, record.feedback)
			if work.Backend.(*plan).action == "read" {
				batch.results[i] = readResult(work, record.reply, record.failure)
			} else if record.failure != nil {
				batch.results[i] = protocol.ResultError(work.Operation, pb.MutationOutcome_NOT_APPLIED, record.failure)
			} else {
				observed[i] = record.reply
			}
		}
		for _, i := range positions {
			work := works[i]
			if batch.results[i] != nil {
				continue
			}
			if denied := batch.callerFailure(work, i); denied != nil {
				batch.results[i] = protocol.ResultError(work.Operation, pb.MutationOutcome_NOT_APPLIED, denied)
				batch.feedback = combineFeedback(batch.feedback, execution.Neutral)
				continue
			}
			native := work.Backend.(*plan)
			current := observed[i]
			if native.action == "replace" && !*current.Found {
				denied := protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "record missing")
				batch.results[i] = protocol.ResultError(work.Operation, pb.MutationOutcome_NOT_APPLIED, denied)
				batch.feedback = combineFeedback(batch.feedback, execution.Neutral)
				continue
			}
			if native.action == "program" {
				program := batch.programs[i]
				program.attempts++
				next, mutation, sample := evaluateProgram(program.ctx, native, current)
				if denied := batch.callerFailure(work, i); denied != nil {
					mutation = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, denied)
					sample = execution.Neutral
				}
				batch.feedback = combineFeedback(batch.feedback, sample)
				if mutation != nil {
					batch.results[i] = protocol.ResultError(work.Operation, mutation.Outcome, mutation.Failure)
					continue
				}
				copy := *work
				copy.Backend = next
				work = &copy
			}
			batch.appendWrite(work, i, current)
			observed[i] = nil
		}
		batch.flush()
		positions = batch.retry
		batch.retry = nil
		for _, i := range positions {
			batch.results[i] = nil
		}
	}
	return batch.results, batch.feedback
}
