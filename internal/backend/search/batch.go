package search

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

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
		if failure := work.Ended(ctx); failure != nil {
			observed[i].failure = failure
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
		if works[i].Context.Err() != nil {
			observed[i] = observedRecord{failure: protocol.ContextFailure(works[i].Context)}
			continue
		}
		if observed[i].failure == nil {
			observed[i] = replies[position]
		}
	}
	return observed
}

// mgetUnique verifies the entire positional ID and index correspondence of a
// reply before publishing any of its documents. A reply larger than the exchange
// window is dropped whole and read again in smaller groups; one source alone may
// exceed it.
func (a *Adapter) mgetUnique(ctx context.Context, works []*execution.Plan) []observedRecord {
	observed := make([]observedRecord, len(works))
	failAll := func(failure *pb.Failure) []observedRecord {
		for i := range observed {
			observed[i] = observedRecord{failure: failure}
		}
		return observed
	}
	if ctx.Err() != nil {
		return failAll(protocol.ContextFailure(ctx))
	}
	ids := make([]string, len(works))
	index := works[0].Backend.(*plan).index
	for i, work := range works {
		native := work.Backend.(*plan)
		ids[i] = native.id
		if native.index != index {
			index = ""
		}
	}
	body := map[string]any{"ids": ids}
	path := "/" + url.PathEscape(index) + "/_mget?realtime=true"
	if index == "" {
		type recordReference struct {
			Index string `json:"_index"`
			ID    string `json:"_id"`
		}
		docs := make([]recordReference, len(works))
		for i, work := range works {
			native := work.Backend.(*plan)
			docs[i] = recordReference{Index: native.index, ID: native.id}
		}
		body = map[string]any{"docs": docs}
		path = "/_mget?realtime=true"
	}
	encoded, _ := json.Marshal(body)
	call := exchange{path: path, body: encoded, contentType: "application/json", operation: "mget", records: len(works)}
	if len(works) > 1 {
		call.responseLimit = a.options().ExchangeBytes
	}
	status, raw, err := a.request(ctx, call)
	if err == errResponseWindow && status == 200 {
		middle := len(works) / 2
		return append(a.mgetUnique(ctx, works[:middle]), a.mgetUnique(ctx, works[middle:])...)
	}
	failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "record response unavailable or incomplete")
	switch {
	case ctx.Err() != nil:
		return failAll(protocol.ContextFailure(ctx))
	case err == nil && (status == 429 || status == 503):
		return failAll(protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend capacity unavailable"))
	case err == nil && status != 200:
		return failAll(a.nativeResponseFailure(ctx, status, raw))
	case err != nil:
		return failAll(failure)
	}
	var envelope struct {
		Docs []getReply `json:"docs"`
	}
	if json.Unmarshal(raw, &envelope) != nil || len(envelope.Docs) != len(works) {
		return failAll(failure)
	}
	for i := range envelope.Docs {
		if envelope.Docs[i].Index != works[i].Backend.(*plan).index || envelope.Docs[i].ID != ids[i] {
			return failAll(failure)
		}
	}
	for i := range envelope.Docs {
		reply := &envelope.Docs[i]
		observation := observedRecord{reply: reply}
		switch {
		case reply.Error != nil:
			observation = observedRecord{failure: failure}
			if reply.Found == nil && reply.Seq == nil && reply.Term == nil && len(reply.Source) == 0 {
				if rejected := a.reject(reply.Error.Type, reply.Status); rejected != nil {
					observation.failure = rejected
				} else if reply.Error.Type == "index_not_found_exception" {
					observation.failure = protocol.Fail(pb.FailureCode_TARGET_NOT_FOUND, "requested index missing")
				}
			}
		case reply.Found == nil || reply.Status != 0:
			observation = observedRecord{failure: failure}
		case !*reply.Found:
			if reply.Seq != nil || reply.Term != nil || len(reply.Source) != 0 {
				observation = observedRecord{failure: failure}
			}
		case reply.Seq == nil || reply.Term == nil || *reply.Seq < 0 || *reply.Term < 1 || !object(reply.Source):
			observation = observedRecord{failure: failure}
		case ctx.Err() != nil:
			observation = observedRecord{failure: protocol.ContextFailure(ctx)}
		}
		observed[i] = observation
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
	entry    []byte
	pending  []recordWrite
	retry    []int
	barriers map[string][]int
	// chains maps the first position of each chain of folding mutations of
	// one record to all its positions, in queue order. applied maps a chain
	// whose write is framed to the positions whose effects that write
	// carries. unfold lists chains whose write was rejected: nothing of them
	// applied, so their mutations run again one at a time.
	chains  map[int][]int
	applied map[int][]int
	unfold  []int
}

// fail records a final result for one work.
func (b *recordBatch) fail(position int, outcome pb.MutationOutcome, failure *pb.Failure) {
	b.results[position] = execution.FailedEvent(b.works[position].Command, outcome, failure)
}

// appendWrite frames one document at a time; a single large entry can exceed
// the batch window.
func (b *recordBatch) appendWrite(work *execution.Plan, position int, current *getReply) {
	entry, err := appendBulkEntry(b.entry[:0], work.Backend.(*plan), current)
	if err != nil {
		if b.applied[position] != nil {
			delete(b.applied, position)
			b.unfold = append(b.unfold, position)
			return
		}
		b.fail(position, pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "Search source cannot be framed as one NDJSON line"))
		return
	}
	b.entry = entry
	settings := b.adapter.options()
	window := min(settings.BatchBytes, settings.ExchangeBytes)
	if b.request.Len()+len(entry) > window {
		b.flush()
	}
	start := b.request.Len()
	b.request.Write(entry)
	pending := recordWrite{work: work, position: position, start: start, end: b.request.Len()}
	b.pending = append(b.pending, pending)
	if b.request.Len() >= window {
		b.flush()
	}
}

// appendBulkEntry frames one bulk action line, with metadata keys in sorted
// order, and its compacted source line.
func appendBulkEntry(dst []byte, native *plan, current *getReply) ([]byte, error) {
	action := native.bulkAction()
	dst = append(dst, `{"`...)
	dst = append(dst, action...)
	dst = append(dst, `":{"_id":`...)
	dst = appendJSONString(dst, native.id)
	dst = append(dst, `,"_index":`...)
	dst = appendJSONString(dst, native.index)
	if current != nil && *current.Found {
		dst = append(dst, `,"if_primary_term":`...)
		dst = strconv.AppendInt(dst, *current.Term, 10)
		dst = append(dst, `,"if_seq_no":`...)
		dst = strconv.AppendInt(dst, *current.Seq, 10)
	}
	if native.action == "expression" {
		dst = append(dst, `,"retry_on_conflict":0`...)
	} else if action != "delete" {
		dst = append(dst, `,"pipeline":"_none"`...)
	}
	dst = append(dst, "}}\n"...)
	if native.source == nil {
		return dst, nil
	}
	// The options are those of json.Compact, so stored sources keep their
	// strings and escapes but lose insignificant whitespace.
	dst, err := jsontext.AppendFormat(dst, native.source, jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true), jsontext.PreserveRawStrings(true))
	if err != nil {
		return nil, err
	}
	dst = append(dst, '\n')
	return dst, nil
}

// appendJSONString quotes s as json.Marshal does, including its HTML-safe
// escapes, without reflection in the common case.
func appendJSONString(dst []byte, s string) []byte {
	if !utf8.ValidString(s) || strings.ContainsAny(s, "<>&\u2028\u2029") {
		encoded, _ := json.Marshal(s)
		return append(dst, encoded...)
	}
	dst, _ = jsontext.AppendQuote(dst, s)
	return dst
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
		if members := b.applied[item.position]; members != nil {
			// A chain's write goes out while any of its callers remains.
			if b.endChain(members) {
				delete(b.applied, item.position)
				continue
			}
		} else if failure := item.work.Ended(b.ctx); failure != nil {
			b.fail(item.position, pb.MutationOutcome_NOT_APPLIED, failure)
			continue
		}
		length += copy(body[length:], body[item.start:item.end])
		works = append(works, item.work)
		positions = append(positions, item.position)
	}
	if len(works) != 0 {
		visible := works[0].Backend.(*plan).waitUntilVisible
		refresh := "false"
		if visible {
			refresh = "wait_for"
		}
		call := exchange{
			path: "/_bulk?pipeline=_none&refresh=" + refresh + "&wait_for_active_shards=1",
			body: body[:length], mutation: true, operation: "bulk", visible: visible, records: len(works),
		}
		status, raw, err := b.adapter.request(b.ctx, call)
		replies, items := b.adapter.bulkResults(works, status, raw, err)
		if visible {
			b.queueBulkVisibilityBarriers(works, positions, replies, items)
		}
		for i, position := range positions {
			if b.applied[position] != nil {
				b.settleChain(position, replies[i])
				continue
			}
			b.results[position] = replies[i]
			conditional := works[i].Backend.(*plan).conditional
			mutation := replies[i].GetMutationResult()
			if !conditional || mutation.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || mutation.GetFailure().GetCode() != pb.FailureCode_CONFLICT {
				continue
			}
			if b.attempts[position] < b.adapter.options().RetryAttempts {
				b.retry = append(b.retry, position)
			} else {
				b.fail(position, pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_CONFLICT, "Search record changed during every transform attempt"))
			}
		}
	}
	b.request.Reset()
	clear(b.pending)
	b.pending = b.pending[:0]
}

// ExecuteRecords resolves each target once and then runs the batch's reads or
// mutations. In-flight reads and conditional writes keep that physical index
// even if an alias changes concurrently.
func (a *Adapter) ExecuteRecords(ctx context.Context, works []*execution.Plan) []*pb.Event {
	batch := &recordBatch{
		adapter:  a,
		ctx:      ctx,
		works:    works,
		results:  make([]*pb.Event, len(works)),
		attempts: make([]int, len(works)),
	}
	positions := batch.qualify()
	if works[0].Command.GetRead() != nil {
		batch.read(positions)
	} else {
		batch.mutate(positions)
	}
	return batch.results
}

// qualify denies works their targets cannot serve and returns the others.
func (b *recordBatch) qualify() []int {
	positions := make([]int, 0, len(b.works))
	for i, work := range b.works {
		if failure := work.Ended(b.ctx); failure != nil {
			b.fail(i, pb.MutationOutcome_NOT_STARTED, failure)
			continue
		}
		positions = append(positions, i)
	}
	type qualification struct {
		caps    capabilities
		failure *pb.Failure
	}
	qualified := make(map[string]qualification)
	return slices.DeleteFunc(positions, func(i int) bool {
		work := b.works[i]
		if failure := work.Ended(b.ctx); failure != nil {
			b.fail(i, pb.MutationOutcome_NOT_APPLIED, failure)
			return true
		}
		native := work.Backend.(*plan)
		target, known := qualified[native.index]
		if !known {
			caps, failure := b.adapter.inspect(b.ctx, native.index)
			target = qualification{caps: caps, failure: failure}
			qualified[native.index] = target
		}
		if failure := work.Ended(b.ctx); failure != nil {
			b.fail(i, pb.MutationOutcome_NOT_APPLIED, failure)
			return true
		}
		denied := target.failure
		if denied == nil {
			denied = target.caps.denies(native.action)
		}
		if denied != nil {
			b.fail(i, pb.MutationOutcome_NOT_APPLIED, denied)
			return true
		}
		native.index = target.caps.index
		return false
	})
}

func (c capabilities) denies(action string) *pb.Failure {
	switch {
	case action != "read" && !c.writable:
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "alias is not a write target")
	case action == "read" && !c.source:
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "stored full source required for this operation")
	case (action == "program" || action == "expression") && (!c.source || !c.nativeWrite):
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "transform requires full stored source and no default or final pipeline")
	case action != "read" && action != "delete" && !c.write:
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "stored full source and no final pipeline required for this operation")
	}
	return nil
}

func (b *recordBatch) read(positions []int) {
	works := make([]*execution.Plan, len(positions))
	for i, position := range positions {
		works[i] = b.works[position]
	}
	for i, record := range b.adapter.mget(b.ctx, works) {
		b.results[positions[i]] = readResult(record.reply, record.failure)
	}
}

// mutate frames each work's write, pre-reading the records its action depends
// on, and sends conflicting transforms around again from a fresh read.
// Folding mutations of one record become one write.
func (b *recordBatch) mutate(positions []int) {
	settings := b.adapter.options()
	// At most this many pre-read sources are retained while Lua runs.
	retained := max(1, settings.ExchangeBytes/settings.BatchBytes)
	positions = b.chain(positions)
	for len(positions) != 0 {
		for start := 0; start < len(positions); {
			end, reads := start, 0
			for end < len(positions) && (reads < retained || !b.preReads(positions[end])) {
				if b.preReads(positions[end]) {
					reads++
				}
				end++
			}
			b.mutateChunk(positions[start:end])
			start = end
		}
		b.flush()
		b.flushVisibilityBarriers()
		positions = b.retry
		b.retry = nil
		for _, i := range positions {
			b.clear(i)
		}
	}
	for len(b.unfold) != 0 {
		head := b.unfold[0]
		b.unfold = b.unfold[1:]
		members := b.chains[head]
		delete(b.chains, head)
		for _, i := range members {
			b.results[i] = nil
			b.attempts[i] = 0
		}
		for _, i := range members {
			single := []int{i}
			b.mutate(single)
		}
	}
}

// chain links folding mutations of one record into chains and returns the
// positions of the other mutations and of each chain's first mutation.
func (b *recordBatch) chain(positions []int) []int {
	starts := make([]int, 0, len(positions))
	first := make(map[string]int, len(positions))
	for _, i := range positions {
		work := b.works[i]
		head, linked := first[work.Key]
		if !work.Folds || !linked {
			if work.Folds {
				first[work.Key] = i
			}
			starts = append(starts, i)
			continue
		}
		if b.chains == nil {
			b.chains = make(map[int][]int)
		}
		if b.chains[head] == nil {
			b.chains[head] = []int{head}
		}
		b.chains[head] = append(b.chains[head], i)
	}
	return starts
}

// clear forgets the results of a work, or of every work of its chain.
func (b *recordBatch) clear(i int) {
	members := b.chains[i]
	if members == nil {
		b.results[i] = nil
		return
	}
	for _, member := range members {
		b.results[member] = nil
	}
}

// preReads reports whether a work, or a chain starting at it, depends on the
// record's current state. A chain does unless its first live mutation
// replaces or deletes the record whatever it holds.
func (b *recordBatch) preReads(i int) bool {
	members := b.chains[i]
	if members == nil {
		return b.works[i].Backend.(*plan).preReads()
	}
	for _, member := range members {
		if b.works[member].Ended(b.ctx) == nil {
			action := b.works[member].Backend.(*plan).action
			return action != "index" && action != "delete"
		}
	}
	return false
}

// preReads reports whether a mutation depends on the record's current state.
func (native *plan) preReads() bool {
	return native.action == "replace" || native.action == "program"
}

func (b *recordBatch) mutateChunk(chunk []int) {
	var readWorks []*execution.Plan
	var readPositions []int
	// readChains holds the chains that asked for a read; one without its
	// read has failed or waits for the next round.
	var readChains map[int]bool
	for _, i := range chunk {
		work := b.works[i]
		if b.chains[i] != nil {
			if b.preReads(i) {
				if readChains == nil {
					readChains = make(map[int]bool)
				}
				readChains[i] = true
				readWorks = append(readWorks, b.reader(i))
				readPositions = append(readPositions, i)
			}
			continue
		}
		if denied := work.Ended(b.ctx); denied != nil {
			b.fail(i, pb.MutationOutcome_NOT_APPLIED, denied)
			continue
		}
		if work.Backend.(*plan).preReads() {
			readWorks = append(readWorks, work)
			readPositions = append(readPositions, i)
		}
	}
	current := make(map[int]*getReply, len(readWorks))
	for offset, record := range b.adapter.mget(b.ctx, readWorks) {
		position := readPositions[offset]
		switch {
		case record.failure == nil:
			current[position] = record.reply
		case b.chains[position] != nil && readWorks[offset].Context.Err() != nil && b.live(position):
			// The reading caller left; another one reads in the next round.
			b.retry = append(b.retry, position)
		case b.chains[position] != nil:
			for _, member := range b.chains[position] {
				failure := record.failure
				if denied := b.works[member].Ended(b.ctx); denied != nil {
					failure = denied
				}
				b.fail(member, pb.MutationOutcome_NOT_APPLIED, failure)
			}
		default:
			b.fail(position, pb.MutationOutcome_NOT_APPLIED, record.failure)
		}
	}
	for _, i := range chunk {
		if b.results[i] != nil {
			continue
		}
		if b.chains[i] != nil {
			if readChains[i] && current[i] == nil {
				continue
			}
			b.mutateChain(i, current[i])
			delete(current, i)
			continue
		}
		if denied := b.works[i].Ended(b.ctx); denied != nil {
			b.fail(i, pb.MutationOutcome_NOT_APPLIED, denied)
			continue
		}
		b.mutateOne(i, current[i])
		delete(current, i)
	}
}

// mutateOne frames one write from the record's current state, if it has one.
func (b *recordBatch) mutateOne(i int, current *getReply) {
	work := b.works[i]
	native := work.Backend.(*plan)
	if native.action == "replace" && !*current.Found {
		b.fail(i, pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "record missing"))
		return
	}
	if native.program != nil {
		b.attempts[i]++
		next, mutation := b.adapter.evaluateProgram(work, current)
		if denied := work.Ended(b.ctx); denied != nil {
			mutation = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, denied)
		}
		if mutation != nil {
			b.fail(i, mutation.Outcome, mutation.Failure)
			if native.waitUntilVisible && mutation.Outcome == pb.MutationOutcome_APPLIED && mutation.Failure == nil {
				b.queueVisibilityBarrier(native.index, i)
			}
			return
		}
		transformed := *work
		transformed.Backend = next
		work = &transformed
	}
	b.appendWrite(work, i, current)
}
