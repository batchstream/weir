package search

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

const batchBodyLimit = execution.BackendBatchBytes

const getFramingLimit = 32 << 10

type observedRecord struct {
	reply   *getReply
	failure *pb.Failure
}

type recordGroup struct {
	index     string
	works     []*execution.Plan
	positions []int
}

// mget groups the bounded batch by concrete index. Identical IDs in different
// indexes remain separate records, with results restored to caller positions.
func (a *Adapter) mget(ctx context.Context, works []*execution.Plan) []observedRecord {
	byIndex := make(map[string]*recordGroup)
	groups := make([]*recordGroup, 0)
	for i, work := range works {
		index := work.Backend.(*plan).index
		group := byIndex[index]
		if group == nil {
			group = &recordGroup{index: index}
			byIndex[index] = group
			groups = append(groups, group)
		}
		group.works = append(group.works, work)
		group.positions = append(group.positions, i)
	}
	observed := make([]observedRecord, len(works))
	for _, group := range groups {
		for i, record := range a.mgetIndex(ctx, group.index, group.works) {
			observed[group.positions[i]] = record
		}
	}
	return observed
}

// Each real-time pre-read verifies the entire positional ID and index
// correspondence before publishing a document. Response and JSON bounds apply
// to every document in each sequential request.
func (a *Adapter) mgetIndex(ctx context.Context, index string, works []*execution.Plan) []observedRecord {
	observed := make([]observedRecord, len(works))
	for start := 0; start < len(works); {
		end := start
		bound := getFramingLimit
		for end < len(works) {
			item := len(works[end].Backend.(*plan).id) + 16
			if bound+item > batchBodyLimit {
				break
			}
			bound += item
			end++
		}
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
				observation := observedRecord{failure: protocol.ContextFailure(caller)}
				observed[i] = observation
				continue
			}
			group = append(group, work)
			indexes = append(indexes, i)
			ids = append(ids, work.Backend.(*plan).id)
		}
		start = end
		if len(group) == 0 {
			continue
		}
		body := map[string]any{"ids": ids}
		encoded, _ := json.Marshal(body)
		call := exchange{
			path: "/" + url.PathEscape(index) + "/_mget?realtime=true", body: encoded,
			contentType: "application/json", limit: batchBodyLimit,
			jsonNodes: len(group)*(16384+32) + 1,
		}
		status, raw, err := a.request(ctx, call)
		failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "record response unavailable or incomplete")
		switch {
		case ctx.Err() != nil:
			failure = protocol.ContextFailure(ctx)
		case err == errResponseLimit:
			failure = protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "record response exceeds configured read bound")
		case err == nil && (status == 429 || status == 503):
			failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend capacity unavailable")
		case err == nil && status != 200:
			failure = a.nativeResponseFailure(status, raw)
		}
		for _, i := range indexes {
			observation := observedRecord{failure: failure}
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
			if reply.Index != index || reply.ID != ids[i] {
				corresponds = false
				break
			}
		}
		if !corresponds {
			continue
		}
		for i := range envelope.Docs {
			reply := &envelope.Docs[i]
			observation := observedRecord{reply: reply}
			switch {
			case reply.Error != nil:
				observation.reply = nil
				observation.failure = failure
				if reply.Found == nil && reply.Seq == nil && reply.Term == nil && len(reply.Source) == 0 {
					if rejected := a.reject(reply.Error.Type, reply.Status); rejected != nil {
						observation.failure = rejected
					} else if reply.Error.Type == "index_not_found_exception" {
						observation.failure = protocol.Fail(pb.FailureCode_TARGET_NOT_FOUND, "requested index missing")
					}
				}
			case reply.Found == nil || reply.Status != 0:
				observation.reply, observation.failure = nil, failure
			case !*reply.Found:
				if reply.Seq != nil || reply.Term != nil || len(reply.Source) != 0 {
					observation.reply, observation.failure = nil, failure
				}
			case reply.Seq == nil || reply.Term == nil || *reply.Seq < 0 || *reply.Term < 1 || !object(reply.Source):
				observation.reply, observation.failure = nil, failure
			case validateJSON(reply.Source, 16384) != nil:
				observation.reply, observation.failure = nil, failure
			case len(reply.Source) > a.sourceLimit(group[i]):
				observation.reply = nil
				observation.failure = protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "stored record exceeds read limit")
			}
			observed[indexes[i]] = observation
		}
	}
	return observed
}

type recordWrite struct {
	work                 *execution.Plan
	position, start, end int
}

type recordBatch struct {
	adapter  *Adapter
	ctx      context.Context
	results  []*pb.Event
	attempts []int
	request  bytes.Buffer
	pending  []recordWrite
	retry    []int
}

func (b *recordBatch) callerFailure(work *execution.Plan) *pb.Failure {
	if work.Context != nil && work.Context.Err() != nil {
		return protocol.ContextFailure(work.Context)
	}
	if b.ctx.Err() != nil {
		return protocol.ContextFailure(b.ctx)
	}
	return nil
}

// appendWrite frames one document at a time. Generated Lua sources are released
// after framing; the batch retains at most one bounded NDJSON request.
func (b *recordBatch) appendWrite(work *execution.Plan, position int, current *getReply) {
	native := work.Backend.(*plan)
	metadata := map[string]any{"_index": native.index, "_id": native.id}
	action := native.bulkAction()
	switch native.action {
	case "expression":
		metadata["retry_on_conflict"] = 0
	case "replace":
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
		if failure := b.callerFailure(item.work); failure != nil {
			b.results[item.position] = execution.FailedEvent(item.work.Command, pb.MutationOutcome_NOT_APPLIED, failure)
			continue
		}
		length += copy(body[length:], body[item.start:item.end])
		works = append(works, item.work)
		positions = append(positions, item.position)
	}
	if len(works) != 0 {
		call := exchange{
			path:     "/_bulk?pipeline=_none&refresh=false&wait_for_active_shards=1&timeout=1s",
			body:     body[:length],
			limit:    responseLimit,
			mutation: true,
		}
		status, raw, err := b.adapter.request(b.ctx, call)
		replies := b.adapter.bulkResults(works, status, raw, err)
		for i, position := range positions {
			b.results[position] = replies[i]
			program := works[i].Backend.(*plan).program
			mutation := replies[i].GetMutationResult()
			if program == nil || mutation.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || mutation.GetFailure().GetCode() != pb.FailureCode_CONFLICT {
				continue
			}
			if b.attempts[position] < programAttempts {
				b.retry = append(b.retry, position)
			} else {
				failure := protocol.Fail(pb.FailureCode_CONFLICT, "Search record changed during every transform attempt")
				b.results[position] = execution.FailedEvent(works[i].Command, pb.MutationOutcome_NOT_APPLIED, failure)
			}
		}
	}
	b.request.Reset()
	clear(b.pending)
	b.pending = b.pending[:0]
}

func (a *Adapter) executeRecords(ctx context.Context, works []*execution.Plan) []*pb.Event {
	if len(works) == 0 {
		return nil
	}
	batch := recordBatch{
		adapter:  a,
		ctx:      ctx,
		results:  make([]*pb.Event, len(works)),
		attempts: make([]int, len(works)),
	}
	totalBytes := 0
	for _, work := range works {
		totalBytes += work.Bytes
	}
	if totalBytes > batchBodyLimit {
		failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "batch exceeds execution bounds")
		for i, work := range works {
			batch.results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_STARTED, failure)
		}
		return batch.results
	}
	positions := make([]int, 0, len(works))
	for i, work := range works {
		if failure := batch.callerFailure(work); failure != nil {
			batch.results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_STARTED, failure)
			continue
		}
		positions = append(positions, i)
	}
	if len(positions) == 0 {
		return batch.results
	}
	type qualification struct {
		caps    capabilities
		failure *pb.Failure
	}
	// Failed checks are shared only within this bounded Execute call. Successful
	// target capabilities are retained by the Adapter's bounded metadata cache.
	qualified := make(map[string]qualification)
	for _, i := range positions {
		work := works[i]
		if failure := batch.callerFailure(work); failure != nil {
			batch.results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_APPLIED, failure)
			continue
		}
		native := work.Backend.(*plan)
		target, exists := qualified[native.index]
		if !exists {
			caps, failure := a.inspect(ctx, native.index, false)
			target = qualification{caps: caps, failure: failure}
			qualified[native.index] = target
		}
		if failure := batch.callerFailure(work); failure != nil {
			batch.results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_APPLIED, failure)
			continue
		}
		caps := target.caps
		denied := target.failure
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
			batch.results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_APPLIED, denied)
		}
	}
	for len(positions) != 0 {
		// Bound the retained pre-read sources before Lua runs. Only one chunk's
		// sources and one framed write body coexist; small ordinary reads still
		// collect up to the complete response bound in a single native request.
		for start := 0; start < len(positions); {
			end, readBytes := start, getFramingLimit
			for end < len(positions) {
				work := works[positions[end]]
				native := work.Backend.(*plan)
				if batch.results[positions[end]] == nil && (native.action == "read" || native.action == "replace" || native.action == "program") {
					bound := a.sourceLimit(work) + getFramingLimit
					if bound > batchBodyLimit-readBytes {
						break
					}
					readBytes += bound
				}
				end++
			}
			chunk := positions[start:end]
			readWorks := make([]*execution.Plan, 0, len(chunk))
			readPositions := make([]int, 0, len(chunk))
			for _, i := range chunk {
				work := works[i]
				if batch.results[i] != nil {
					continue
				}
				if denied := batch.callerFailure(work); denied != nil {
					batch.results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_APPLIED, denied)
					continue
				}
				action := work.Backend.(*plan).action
				if action == "read" || action == "replace" || action == "program" {
					readWorks = append(readWorks, work)
					readPositions = append(readPositions, i)
				}
			}
			observed := make(map[int]*getReply, len(readWorks))
			for offset, record := range a.mget(ctx, readWorks) {
				i := readPositions[offset]
				work := works[i]
				if work.Backend.(*plan).action == "read" {
					batch.results[i] = readResult(work, record.reply, record.failure)
				} else if record.failure != nil {
					batch.results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_APPLIED, record.failure)
				} else {
					observed[i] = record.reply
				}
			}
			for _, i := range chunk {
				work := works[i]
				if batch.results[i] != nil {
					continue
				}
				if denied := batch.callerFailure(work); denied != nil {
					batch.results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_APPLIED, denied)
					continue
				}
				native := work.Backend.(*plan)
				current := observed[i]
				delete(observed, i)
				if native.action == "replace" && !*current.Found {
					denied := protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "record missing")
					batch.results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_APPLIED, denied)
					continue
				}
				if native.action == "program" {
					batch.attempts[i]++
					programContext, cancel := context.WithCancel(ctx)
					var stop func() bool
					if work.Context != nil {
						stop = context.AfterFunc(work.Context, cancel)
					}
					next, mutation := evaluateProgram(programContext, native, current)
					if stop != nil {
						stop()
					}
					cancel()
					if denied := batch.callerFailure(work); denied != nil {
						mutation = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, denied)
					}
					if mutation != nil {
						batch.results[i] = execution.FailedEvent(work.Command, mutation.Outcome, mutation.Failure)
						continue
					}
					copy := *work
					copy.Backend = next
					work = &copy
				}
				batch.appendWrite(work, i, current)
			}
			start = end
		}

		batch.flush()
		positions = batch.retry
		batch.retry = nil
		for _, i := range positions {
			batch.results[i] = nil
		}
	}
	return batch.results
}
