package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/url"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

const getFramingLimit = 32 << 10

type observedRecord struct {
	reply   *getReply
	failure *pb.Failure
}

type readIdentity struct {
	index string
	id    string
}

// A real-time observation can serve every caller of the same concrete record.
// Keep index in the identity so equal IDs in different indexes stay separate.
func (a *Adapter) mget(ctx context.Context, works []*execution.Plan) []observedRecord {
	observed := make([]observedRecord, len(works))
	unique := make([]*execution.Plan, 0, len(works))
	positions := make([]int, len(works))
	identities := make(map[readIdentity]int, len(works))
	for i, work := range works {
		caller := ctx
		if work.Context != nil && work.Context.Err() != nil {
			caller = work.Context
		}
		if caller.Err() != nil {
			observed[i].failure = protocol.ContextFailure(caller)
			continue
		}
		native := work.Backend.(*plan)
		identity := readIdentity{index: native.index, id: native.id}
		position, exists := identities[identity]
		if !exists {
			position = len(unique)
			identities[identity] = position
			unique = append(unique, work)
		}
		positions[i] = position
	}
	if len(unique) == 0 {
		return observed
	}
	replies := a.mgetUnique(ctx, unique)
	for i, position := range positions {
		if works[i].Context != nil && works[i].Context.Err() != nil {
			observed[i].reply = nil
			observed[i].failure = protocol.ContextFailure(works[i].Context)
			continue
		}
		if observed[i].failure == nil {
			observed[i] = replies[position]
		}
	}
	return observed
}

// Each real-time pre-read verifies the entire positional ID and index
// correspondence before publishing a document. Every response is validated in
// full before its documents become observations.
func (a *Adapter) mgetUnique(ctx context.Context, works []*execution.Plan) []observedRecord {
	observed := make([]observedRecord, len(works))
	readOnly := true
	for _, work := range works {
		readOnly = readOnly && work.Backend.(*plan).action == "read"
	}
	for start := 0; start < len(works); {
		end := start
		bound := getFramingLimit
		if readOnly {
			bound = 16
		}
		for end < len(works) {
			native := works[end].Backend.(*plan)
			item := a.options().BatchBytes + len(native.index) + len(native.id) + getFramingLimit
			if readOnly {
				// JSON can escape each byte to six bytes. Pure reads use the real
				// response byte window instead of assuming every source is 4 MiB.
				item = 6*(len(native.index)+len(native.id)) + 64
			}
			if end > start && item > a.options().ExchangeBytes-bound {
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
			if ctx.Err() != nil {
				observed[i].failure = protocol.ContextFailure(ctx)
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
		index := group[0].Backend.(*plan).index
		for _, work := range group[1:] {
			if work.Backend.(*plan).index != index {
				index = ""
				break
			}
		}
		body := map[string]any{"ids": ids}
		path := "/" + url.PathEscape(index) + "/_mget?realtime=true"
		if index == "" {
			type recordReference struct {
				Index string `json:"_index"`
				ID    string `json:"_id"`
			}
			docs := make([]recordReference, len(group))
			for i, work := range group {
				native := work.Backend.(*plan)
				docs[i] = recordReference{Index: native.index, ID: native.id}
			}
			body = map[string]any{"docs": docs}
			path = "/_mget?realtime=true"
		}
		encoded, _ := json.Marshal(body)
		call := exchange{
			path: path, body: encoded,
			contentType: "application/json", operation: "mget", records: len(group)}
		if readOnly && len(group) > 1 {
			call.responseLimit = a.options().ExchangeBytes
		}
		status, raw, err := a.request(ctx, call)
		if err == errResponseWindow && status == 200 {
			// No partial body is retained or published. Reads can safely retry in
			// smaller groups; a single large source retains its existing contract.
			middle := len(group) / 2
			replies := a.mgetUnique(ctx, group[:middle])
			replies = append(replies, a.mgetUnique(ctx, group[middle:])...)
			for i, position := range indexes {
				observed[position] = replies[i]
			}
			continue
		}
		failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "record response unavailable or incomplete")
		switch {
		case errors.Is(err, execution.ErrExecutionMemory):
			failure = recordMemoryFailure()
		case ctx.Err() != nil:
			failure = protocol.ContextFailure(ctx)
		case err == nil && (status == 429 || status == 503):
			failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend capacity unavailable")
		case err == nil && status != 200:
			failure = a.nativeResponseFailure(ctx, status, raw)
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
		if err := reserveSearchScratch(ctx, cap(raw)+jsonDecodeWorking(raw)); err != nil {
			for _, i := range indexes {
				observed[i].failure = recordMemoryFailure()
			}
			continue
		}
		if json.Unmarshal(raw, &envelope) != nil || len(envelope.Docs) != len(group) {
			continue
		}
		raw = nil
		if memory := searchMemory(ctx); memory != nil {
			bytes := 0
			for _, reply := range envelope.Docs {
				bytes += cap(reply.Source)
			}
			if err := memory.set(memory.sources+bytes, memory.frame, 0); err != nil {
				for _, i := range indexes {
					observed[i].failure = recordMemoryFailure()
				}
				continue
			}
		}
		corresponds := true
		for i, reply := range envelope.Docs {
			if reply.Index != group[i].Backend.(*plan).index || reply.ID != ids[i] {
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
			if reply.Found != nil && *reply.Found && len(reply.Source) != 0 {
				if err := reserveSearchScratch(ctx, jsonDecodeWorking(reply.Source)); err != nil {
					observation.reply = nil
					observation.failure = recordMemoryFailure()
					observed[indexes[i]] = observation
					continue
				}
			}
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
			case validateJSON(ctx, reply.Source) != nil:
				observation.reply, observation.failure = nil, failure
				if ctx.Err() != nil {
					observation.failure = protocol.ContextFailure(ctx)
				}
			}
			observed[indexes[i]] = observation
			if memory := searchMemory(ctx); memory != nil {
				_ = memory.set(memory.sources, memory.frame, 0)
			}
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
	works    []*execution.Plan
	results  []*pb.Event
	attempts []int
	request  bytes.Buffer
	pending  []recordWrite
	retry    []int
	barriers map[string][]int
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
// after framing; a single large entry can exceed the batch window.
func (b *recordBatch) appendWrite(work *execution.Plan, position int, current *getReply) {
	native := work.Backend.(*plan)
	if len(b.pending) != 0 && b.pending[0].work.Backend.(*plan).waitUntilVisible != native.waitUntilVisible {
		b.flush()
	}
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
	upper := 6*(len(native.index)+len(native.id)) + len(native.source) + 2048
	if memory := searchMemory(b.ctx); memory != nil {
		peak := b.request.Cap() + upper + 2*(b.request.Len()+upper)
		if err := memory.set(memory.sources, peak, 0); err != nil {
			b.results[position] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_APPLIED, recordMemoryFailure())
			return
		}
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
	settings := b.adapter.options()
	window := min(settings.BatchBytes, settings.ExchangeBytes)
	if b.request.Len()+entry.Len() > window {
		b.flush()
	}
	start := b.request.Len()
	b.request.Write(entry.Bytes())
	pending := recordWrite{work: work, position: position, start: start, end: b.request.Len()}
	b.pending = append(b.pending, pending)
	if memory := searchMemory(b.ctx); memory != nil {
		_ = memory.set(memory.sources, b.request.Cap(), 0)
	}
	if native.program != nil {
		bytes := cap(native.source)
		native.source = nil
		if memory := searchMemory(b.ctx); memory != nil {
			_ = memory.set(max(0, memory.sources-bytes), memory.frame, 0)
		}
	}
	if b.request.Len() >= window {
		b.flush()
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
		refresh := "false"
		if works[0].Backend.(*plan).waitUntilVisible {
			refresh = "wait_for"
		}
		call := exchange{
			path: "/_bulk?pipeline=_none&refresh=" + refresh + "&wait_for_active_shards=1",
			body: body[:length], mutation: true, operation: "bulk", records: len(works),
		}
		status, raw, err := b.adapter.request(b.ctx, call)
		response := bulkResponse{works: works, status: status, raw: raw, err: err}
		replies := b.adapter.bulkResults(b.ctx, response)
		b.queueBulkVisibilityBarriers(works, positions, replies, raw)
		for i, position := range positions {
			b.results[position] = replies[i]
			program := works[i].Backend.(*plan).program
			mutation := replies[i].GetMutationResult()
			if program == nil || mutation.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || mutation.GetFailure().GetCode() != pb.FailureCode_CONFLICT {
				continue
			}
			if b.attempts[position] < b.adapter.options().RetryAttempts {
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
	memory := &recordMemory{}
	for _, work := range works {
		// Known input copies and the complete reply scratch are separate live
		// allocations. Dynamic frames/sources must not consume the ACK promise.
		memory.base += 2*work.Bytes + execution.EntryOverheadBytes
		if work.Command.GetMutate() != nil {
			memory.reply += bulkAckWorking(work.Backend.(*plan).id)
		}
		if memory.owner == nil {
			memory.owner = work.Memory
		}
	}
	if memory.reply != 0 {
		// Exactly Runtime's sum(Plan.WorkingBytes) + max(IOReserve):
		// sum(known input) + sum(item ACK) + one envelope/header window.
		memory.reply += (64 << 10) + bulkAckEnvelopeWorking
	} else {
		memory.base += 64 << 10
	}
	ctx = context.WithValue(ctx, recordMemoryContextKey, memory)
	works = append([]*execution.Plan(nil), works...)
	readOnly := true
	for _, work := range works {
		readOnly = readOnly && work.Backend.(*plan).action == "read"
	}
	batch := recordBatch{
		adapter:  a,
		ctx:      ctx,
		works:    works,
		results:  make([]*pb.Event, len(works)),
		attempts: make([]int, len(works)),
	}
	positions := make([]int, 0, len(works))
	for i, work := range works {
		if failure := a.validateRecord(ctx, work); failure != nil {
			batch.results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_STARTED, failure)
			continue
		}
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
	// Resolve each target once for this execution. In-flight reads and conditional
	// writes retain that physical index even if an alias changes concurrently.
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
			case native.action != "read" && !caps.writable:
				denied = protocol.Fail(pb.FailureCode_UNSUPPORTED, "alias is not a write target")
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
			continue
		}
		resolved := *native
		resolved.index = caps.index
		resolvedWork := *work
		resolvedWork.Backend = &resolved
		works[i] = &resolvedWork
	}
	for len(positions) != 0 {
		// Bound the retained pre-read sources before Lua runs. Only one chunk's
		// sources and one framed write body coexist. The batch window estimates
		// read capacity rather than rejecting individual large sources.
		for start := 0; start < len(positions); {
			end, readBytes := start, getFramingLimit
			identities := make(map[readIdentity]bool)
			for end < len(positions) {
				work := works[positions[end]]
				native := work.Backend.(*plan)
				if !readOnly && batch.results[positions[end]] == nil && (native.action == "read" || native.action == "replace" || native.action == "program") {
					identity := readIdentity{index: native.index, id: native.id}
					if !identities[identity] {
						bound := a.options().BatchBytes + len(native.index) + len(native.id) + getFramingLimit
						if end > start && bound > a.options().ExchangeBytes-readBytes {
							break
						}
						readBytes += bound
						identities[identity] = true
					}
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
					if record.failure == nil && record.reply != nil && record.reply.Found != nil && *record.reply.Found && work.Memory != nil {
						if err := work.Memory.ReserveResult(cap(record.reply.Source) + execution.ResultOverheadBytes); err != nil {
							record.reply = nil
							record.failure = recordMemoryFailure()
						}
					}
					batch.results[i] = readResult(record.reply, record.failure)
				} else if record.failure != nil {
					batch.results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_APPLIED, record.failure)
				} else {
					observed[i] = record.reply
				}
			}
			if memory := searchMemory(ctx); memory != nil {
				bytes := 0
				for _, reply := range observed {
					bytes += cap(reply.Source)
				}
				_ = memory.set(bytes, memory.frame, 0)
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
					next, mutation := a.evaluateProgram(programContext, native, current)
					if stop != nil {
						stop()
					}
					cancel()
					if denied := batch.callerFailure(work); denied != nil {
						mutation = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, denied)
					}
					if mutation != nil {
						batch.results[i] = execution.FailedEvent(work.Command, mutation.Outcome, mutation.Failure)
						if native.waitUntilVisible && mutation.Outcome == pb.MutationOutcome_APPLIED && mutation.Failure == nil {
							batch.queueVisibilityBarrier(native.index, i)
						}
						continue
					}
					copy := *work
					copy.Backend = next
					work = &copy
				}
				batch.appendWrite(work, i, current)
				if memory := searchMemory(ctx); memory != nil && current != nil {
					bytes := cap(current.Source)
					current = nil
					_ = memory.set(max(0, memory.sources-bytes), memory.frame, 0)
				}
			}
			start = end
		}

		batch.flush()
		batch.flushVisibilityBarriers()
		positions = batch.retry
		batch.retry = nil
		for _, i := range positions {
			batch.results[i] = nil
		}
	}
	return batch.results
}
