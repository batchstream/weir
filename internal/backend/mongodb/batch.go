package mongodb

import (
	"context"
	"errors"
	"strconv"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
	"google.golang.org/protobuf/proto"
)

// A large scheduled batch becomes multiple native exchanges. The exchange
// budget controls retained framing, and does not reject the whole batch.
func (a *Adapter) executeRecords(ctx context.Context, plans []*execution.Plan) []*pb.Event {
	results := make([]*pb.Event, len(plans))
	memory := &recordMemory{base: recordWorking(plans)}
	if len(plans) != 0 {
		memory.owner = plans[0].Memory
	}
	ctx = context.WithValue(ctx, recordMemoryContextKey, memory)
	active := make([]*execution.Plan, 0, len(plans))
	positions := make([]int, 0, len(plans))
	for i, work := range plans {
		if failure := a.validateRecord(ctx, work); failure != nil {
			results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_STARTED, failure)
			continue
		}
		active = append(active, work)
		positions = append(positions, i)
	}
	if err := reserveRecordWorking(ctx, active, 0); err != nil {
		for i, work := range active {
			results[positions[i]] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_STARTED, recordMemoryFailure())
		}
		return results
	}
	limit := a.options().ExchangeBytes
	for start := 0; start < len(active); {
		end, bytes := start, 0
		for end < len(active) {
			charge := proto.Size(active[end].Command) + execution.EntryOverheadBytes
			if end > start && charge > limit-bytes {
				break
			}
			bytes += charge
			end++
		}
		for i, event := range a.executeRecordChunk(ctx, active[start:end]) {
			results[positions[start+i]] = event
		}
		start = end
	}
	return results
}

func (a *Adapter) executeRecordChunk(ctx context.Context, plans []*execution.Plan) []*pb.Event {
	results := make([]*pb.Event, len(plans))
	type targetBatch struct {
		plans                         []*execution.Plan
		positions                     []int
		reads, writes                 []*execution.Plan
		readPositions, writePositions []int
		qualified                     bool
	}
	groups := make([]targetBatch, 0)
	positions := make(map[namespace]int)
	var programs []*execution.Plan
	var programPositions []int
	for i, p := range plans {
		native := p.Backend.(*plan)
		if native.action == "program" {
			programs = append(programs, p)
			programPositions = append(programPositions, i)
			continue
		}
		position, exists := positions[native.target]
		if !exists {
			position = len(groups)
			positions[native.target] = position
			group := targetBatch{}
			groups = append(groups, group)
		}
		group := &groups[position]
		group.plans = append(group.plans, p)
		group.positions = append(group.positions, i)
		if native.action == "read" {
			group.reads = append(group.reads, p)
			group.readPositions = append(group.readPositions, i)
		} else {
			group.writes = append(group.writes, p)
			group.writePositions = append(group.writePositions, i)
		}
	}

	for position := range groups {
		group := &groups[position]
		if replies := a.qualifyRecordBatch(ctx, group.plans); replies != nil {
			for i, reply := range replies {
				results[group.positions[i]] = reply
			}
			continue
		}
		group.qualified = true
		if len(group.reads) != 0 {
			replies := a.executeReads(ctx, group.reads)
			for i, reply := range replies {
				results[group.readPositions[i]] = reply
			}
		}
	}
	var writes []*execution.Plan
	var writePositions []int
	for _, group := range groups {
		if group.qualified {
			writes = append(writes, group.writes...)
			writePositions = append(writePositions, group.writePositions...)
		}
	}
	if len(writes) != 0 {
		replies := a.executeWrites(ctx, writes)
		for i, reply := range replies {
			results[writePositions[i]] = reply
		}
	}
	if len(programs) != 0 {
		replies := a.executePrograms(ctx, programs)
		for i, reply := range replies {
			results[programPositions[i]] = reply
		}
	}
	return results
}

func unstarted(ctx context.Context, p *execution.Plan) *pb.Event {
	if p.Context != nil && p.Context.Err() != nil {
		return execution.FailedEvent(p.Command, pb.MutationOutcome_NOT_STARTED, protocol.ContextFailure(p.Context))
	}
	if ctx.Err() != nil {
		return execution.FailedEvent(p.Command, pb.MutationOutcome_NOT_STARTED, protocol.ContextFailure(ctx))
	}
	return nil
}

func (a *Adapter) executeReads(ctx context.Context, plans []*execution.Plan) []*pb.Event {
	results := make([]*pb.Event, len(plans))
	skipped := make([]bool, len(plans))
	ids := make(bson.A, 0, len(plans))
	positions := make(map[any][]int, len(plans))
	target := plans[0].Backend.(*plan).target
	for i, p := range plans {
		if result := unstarted(ctx, p); result != nil {
			results[i] = result
			skipped[i] = true
			continue
		}

		id := p.Backend.(*plan).id
		if _, exists := positions[id]; !exists {
			ids = append(ids, id)
		}
		positions[id] = append(positions[id], i)
	}
	if len(ids) == 0 {
		return results
	}

	filter := bson.D{{Key: "_id", Value: ids[0]}}
	if len(ids) > 1 {
		selector := bson.D{{Key: "$in", Value: ids}}
		filter[0].Value = selector
	}
	var command any = bson.D{
		{Key: "find", Value: target.collection},
		{Key: "filter", Value: filter},
		{Key: "limit", Value: int64(len(ids))},
		{Key: "batchSize", Value: int32(len(ids))},
		{Key: "allowPartialResults", Value: false},
	}
	encoded, marshalErr := bson.Marshal(command)
	if marshalErr != nil || len(plans) > 1 && len(encoded) > a.commandBytes() {
		if marshalErr == nil && len(plans) > 1 {
			middle := len(plans) / 2
			copy(results[:middle], a.executeReads(ctx, plans[:middle]))
			copy(results[middle:], a.executeReads(ctx, plans[middle:]))
			return results
		}
		failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "encoded read exceeds MongoDB native BSON command boundary")
		for i, work := range plans {
			if !skipped[i] {
				results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_STARTED, failure)
			}
		}
		return results
	}
	command = bson.Raw(encoded)
	state := &recordCursor{target: target, items: len(ids)}
	session, err := a.client.StartSession()
	valid := err == nil
	received := 0
	stopped := false
	if valid {
		state.session = session
		defer a.closeRecordCursor(ctx, state)
		ctx = mongo.NewSessionContext(ctx, session)
		for page := 0; page < len(ids); page++ {
			attempt := ctx
			var release context.CancelFunc
			if page != 0 {
				attempt, release = nativeAttemptContext(ctx)
			}
			if budgetErr := reserveRecordWorking(ctx, plans, 0); budgetErr != nil {
				err = budgetErr
				valid = false
				break
			}
			raw, readErr := a.client.Database(target.database).RunCommand(attempt, command).Raw()
			if release != nil {
				release()
			}
			err = readErr
			if err != nil {
				// Only a direct initial selection error proves this fresh session
				// sent no command. Wrapped errors and later pages may hide a cursor.
				if _, unsent := readErr.(topology.ServerSelectionError); page == 0 && unsent {
					state.cursorKnown = true
				}
				valid = false
				break
			}
			fields, parseErr := bsonFields(raw)
			valid = parseErr == nil && bsonCommandOK(fields["ok"])
			for key := range fields {
				if key != "ok" && key != "cursor" && key != "operationTime" && key != "$clusterTime" {
					valid = false
				}
			}
			var documents []bson.Raw
			if valid {
				documents, valid = cursorDocuments(fields, state, target.String(), page == 0)
			}
			if !valid || len(documents) > len(ids)-received || len(documents) == 0 && state.cursor != 0 {
				valid = false
				break
			}
			if state.cursorKnown && state.cursor == 0 {
				// The uncompressed driver reply is now one retained backing
				// allocation. cap includes trailing bytes; 36 also covers the
				// wire prefix before the returned OP_MSG or OP_REPLY document.
				// Known frame and parser metadata charges remain in the base.
				extra := cap(raw) + 36 - a.nativeReplyBytes()
				if budgetErr := reserveRecordWorking(ctx, plans, extra); budgetErr != nil {
					err = budgetErr
					valid = false
					break
				}
			}
			for _, raw := range documents {
				id, validID := rawRecordID(raw.Lookup("_id"))
				matches, found := positions[id]
				if !validID || !found || results[matches[0]] != nil {
					valid = false
					break
				}

				if !validBSONDocument(raw, 0) {
					valid = false
					break
				}
				var document *pb.ReadResult
				for _, i := range matches {
					// Duplicate reads share immutable bytes; every RPC reserves its own output.
					if plans[i].Memory != nil {
						if err := plans[i].Memory.ReserveResult(len(raw) + execution.ResultOverheadBytes); err != nil {
							results[i] = execution.FailedEvent(plans[i].Command, pb.MutationOutcome_NOT_STARTED, recordMemoryFailure())
							continue
						}
					}
					if document == nil {
						data := make([]byte, len(raw))
						copy(data, raw)
						d := &pb.Document{ContentType: "application/bson", Data: data}
						document = protocol.ReadDocument(d)
					}
					value := &pb.Event_ReadResult{ReadResult: document}
					event := &pb.Event{Value: value}
					results[i] = event
				}
				received++
			}
			clear(documents)
			documents = nil
			clear(fields)
			fields = nil
			raw = nil
			if !valid || state.cursor == 0 {
				break
			}
			if received == len(ids) {
				valid = false
				break
			}
			interested := false
			for _, p := range plans {
				interested = interested || p.Context == nil || p.Context.Err() == nil
			}
			if !interested {
				stopped = true
				break
			}
			command = bson.D{
				{Key: "getMore", Value: state.cursor},
				{Key: "collection", Value: target.collection},
				{Key: "batchSize", Value: int32(len(ids) - received)},
			}
		}
		valid = valid && state.cursorKnown && state.cursor == 0
	}
	for i, p := range plans {
		if skipped[i] {
			continue
		}
		if results[i] != nil && (valid || err != nil || stopped) {
			continue
		}
		reply := protocol.Missing()
		if !valid {
			failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "point read acknowledgement unavailable or incomplete")
			if stopped && p.Context != nil && p.Context.Err() != nil {
				failure = protocol.ContextFailure(p.Context)
			} else if errors.Is(err, execution.ErrExecutionMemory) {
				failure = recordMemoryFailure()
			} else if err != nil {
				failure = backendFailure(ctx, err)
			}
			reply = protocol.ReadFailure(failure)
		}
		value := &pb.Event_ReadResult{ReadResult: reply}
		event := &pb.Event{Value: value}
		results[i] = event
	}
	return results
}

func rawRecordID(raw bson.RawValue) (any, bool) {
	switch raw.Type {
	case bson.TypeString:
		return raw.StringValueOK()
	case bson.TypeInt32:
		n, ok := raw.Int32OK()
		return int64(n), ok
	case bson.TypeInt64:
		return raw.Int64OK()
	case bson.TypeObjectID:
		return raw.ObjectIDOK()
	}
	return nil, false
}

func (a *Adapter) executeWrites(ctx context.Context, plans []*execution.Plan) []*pb.Event {
	results := make([]*pb.Event, len(plans))
	if err := reserveRecordWorking(ctx, plans, 0); err != nil {
		for i, work := range plans {
			results[i] = unstarted(ctx, work)
			if results[i] == nil {
				results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_STARTED, recordMemoryFailure())
			}
		}
		return results
	}
	active := make([]*execution.Plan, 0, len(plans))
	positions := make([]int, 0, len(plans))
	ops := make(bson.A, 0, len(plans))
	namespaces := make(bson.A, 0)
	namespaceIndexes := make(map[namespace]int32)
	for i, p := range plans {
		if result := unstarted(ctx, p); result != nil {
			results[i] = result
			continue
		}

		n := p.Backend.(*plan)
		namespaceIndex, exists := namespaceIndexes[n.target]
		if !exists {
			namespaceIndex = int32(len(namespaces))
			namespaceIndexes[n.target] = namespaceIndex
			info := bson.D{{Key: "ns", Value: n.target.String()}}
			namespaces = append(namespaces, info)
		}
		filter := bson.D{{Key: "_id", Value: n.id}}
		var op bson.D
		switch n.action {
		case "create":
			op = bson.D{{Key: "insert", Value: namespaceIndex}, {Key: "document", Value: n.document}}
		case "put", "replace", "expression":
			op = bson.D{
				{Key: "update", Value: namespaceIndex},
				{Key: "filter", Value: filter},
				{Key: "updateMods", Value: n.document},
				{Key: "multi", Value: false},
				{Key: "upsert", Value: n.action == "put"},
			}
		case "delete":
			op = bson.D{{Key: "delete", Value: namespaceIndex}, {Key: "filter", Value: filter}, {Key: "multi", Value: false}}
		default:
			results[i] = execution.FailedEvent(p.Command, pb.MutationOutcome_NOT_STARTED, protocol.Fail(pb.FailureCode_INTERNAL, "unsupported prepared operation"))
			continue
		}
		active = append(active, p)
		positions = append(positions, i)
		ops = append(ops, op)
	}
	if len(active) == 0 {
		return results
	}

	concern := bson.D{{Key: "w", Value: "majority"}}
	cursorOpts := bson.D{{Key: "batchSize", Value: int32(len(active))}}
	var command any = bson.D{
		{Key: "bulkWrite", Value: int32(1)},
		{Key: "ops", Value: ops},
		{Key: "nsInfo", Value: namespaces},
		{Key: "ordered", Value: false},
		{Key: "errorsOnly", Value: false},
		{Key: "cursor", Value: cursorOpts},
		{Key: "writeConcern", Value: concern},
	}
	encoded, marshalErr := bson.Marshal(command)
	if marshalErr != nil || len(plans) > 1 && len(encoded) > a.commandBytes() {
		if marshalErr == nil && len(plans) > 1 {
			middle := len(plans) / 2
			copy(results[:middle], a.executeWrites(ctx, plans[:middle]))
			copy(results[middle:], a.executeWrites(ctx, plans[middle:]))
			return results
		}
		failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "encoded write exceeds MongoDB native BSON command boundary")
		for i, work := range active {
			results[positions[i]] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_STARTED, failure)
		}
		return results
	}
	command = bson.Raw(encoded)
	state := &writeBatch{plans: active, results: make([]*pb.MutationResult, len(active))}
	session, err := a.client.StartSession()
	if err != nil {
		for i, p := range active {
			results[positions[i]] = execution.FailedEvent(p.Command, pb.MutationOutcome_NOT_STARTED, backendFailure(ctx, err))
		}
		return results
	}
	state.cursor.session = session
	state.cursor.items = len(active)
	state.cursor.target = namespace{database: "admin", collection: "$cmd.bulkWrite"}
	defer a.closeRecordCursor(ctx, &state.cursor)
	ctx = mongo.NewSessionContext(ctx, session)
	raw, err := a.client.Database("admin").RunCommand(ctx, command).Raw()
	if err != nil && len(raw) == 0 {
		var commandError mongo.CommandError
		if errors.As(err, &commandError) {
			raw = commandError.Raw
		}
	}
	valid := state.reply(raw, true)
	if valid && state.authorizationRejected && len(namespaces) > 1 {
		// Authorization checks every namespace before bulkWrite executes any
		// operation. Only this definite initial rejection permits isolation.
		groups := make([][]*execution.Plan, len(namespaces))
		groupPositions := make([][]int, len(namespaces))
		for i, p := range active {
			index := namespaceIndexes[p.Backend.(*plan).target]
			groups[index] = append(groups[index], p)
			groupPositions[index] = append(groupPositions[index], positions[i])
		}
		for i, group := range groups {
			if len(group) == 0 {
				continue
			}
			replies := a.executeWrites(ctx, group)
			for j, reply := range replies {
				results[groupPositions[i][j]] = reply
			}
		}
		return results
	}
	for page := 1; valid && state.cursor.cursor != 0 && page < len(active); page++ {
		command = bson.D{
			{Key: "getMore", Value: state.cursor.cursor},
			{Key: "collection", Value: "$cmd.bulkWrite"},
			{Key: "batchSize", Value: int32(len(active) - state.received)},
		}
		attempt, release := nativeAttemptContext(ctx)
		raw, err = a.client.Database("admin").RunCommand(attempt, command).Raw()
		release()
		if err != nil {
			break
		}
		valid = state.reply(raw, false)
	}
	if !valid || state.cursor.cursor != 0 && err == nil {
		state.results = make([]*pb.MutationResult, len(active))
	}
	for i := range active {
		reply := state.results[i]
		if reply == nil {
			reply = protocol.Mutation(pb.MutationOutcome_UNKNOWN, protocol.Fail(pb.FailureCode_UNAVAILABLE, "write acknowledgement unavailable or incomplete"))
		}
		value := &pb.Event_MutationResult{MutationResult: reply}
		event := &pb.Event{Value: value}
		results[positions[i]] = event
	}
	return results
}

type writeBatch struct {
	plans                 []*execution.Plan
	results               []*pb.MutationResult
	cursor                recordCursor
	received              int
	concern               bool
	counts                [6]int64
	observed              [6]int64
	uncertainCounts       bool
	authorizationRejected bool
}

func (a *Adapter) closeRecordCursor(ctx context.Context, state *recordCursor) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.options().CleanupTimeout)
	defer cancel()
	_ = a.closeRecordCursorState(ctx, state)
}

func (b *writeBatch) reply(raw bson.Raw, first bool) bool {

	if !validBSONDocument(raw, 0) {
		return false
	}
	fields, err := bsonFields(raw)
	if err != nil {
		return false
	}
	if !bsonCommandOK(fields["ok"]) {
		if !first || !writeZero(fields["ok"]) || fields["cursor"].Type != 0 {
			return false
		}
		for _, count := range []string{"nInserted", "nDeleted", "nMatched", "nModified", "nUpserted"} {
			if raw, exists := fields[count]; exists {
				n, valid := writeCount(raw)
				if !valid || n != 0 {
					return false
				}
			}
		}
		b.cursor.cursorKnown = true
		if fields["writeConcernError"].Type != 0 {
			return true
		}
		b.authorizationRejected = writeAuthorizationRejection(fields)
		for i := range b.results {
			unknown := protocol.Mutation(pb.MutationOutcome_UNKNOWN, protocol.Fail(pb.FailureCode_UNAVAILABLE, "write command rejected without definite evidence"))
			b.results[i] = writeRejection(fields["code"], unknown)
		}
		return true
	}
	for key := range fields {
		switch key {
		case "ok", "cursor", "operationTime", "$clusterTime":
		case "nErrors", "nInserted", "nDeleted", "nMatched", "nModified", "nUpserted", "writeConcernError", "errorLabels", "electionId", "opTime":
			if !first {
				return false
			}
		default:
			return false
		}
	}
	if id, exists := fields["electionId"]; exists {
		if _, valid := id.ObjectIDOK(); !valid {
			return false
		}
	}
	if raw, exists := fields["opTime"]; exists {
		doc, valid := raw.DocumentOK()
		metadata, err := bsonFields(doc)
		_, _, validTimestamp := metadata["ts"].TimestampOK()
		_, validTerm := writeCount(metadata["t"])
		if !valid || err != nil || len(metadata) != 2 || !validTimestamp || !validTerm {
			return false
		}
	}
	if first {
		b.concern = fields["writeConcernError"].Type != 0
		for i, key := range []string{"nErrors", "nInserted", "nDeleted", "nMatched", "nModified", "nUpserted"} {
			n, valid := writeCount(fields[key])
			if !valid || n < 0 || n > int64(len(b.plans)) {
				return false
			}
			b.counts[i] = n
		}
	}
	documents, valid := cursorDocuments(fields, &b.cursor, "admin.$cmd.bulkWrite", first)
	if !valid || len(documents) > len(b.plans)-b.received || len(documents) == 0 && b.cursor.cursor != 0 {
		return false
	}
	for _, raw := range documents {
		item, err := bsonFields(raw)
		index, valid := writeCount(item["idx"])
		if err != nil || !valid || index < 0 || index >= int64(len(b.plans)) || b.results[index] != nil {
			return false
		}
		native := b.plans[index].Backend.(*plan)
		b.results[index] = bulkItemReply(native, item, b.concern)
		count, valid := writeCount(item["n"])
		if writeZero(item["ok"]) {
			b.observed[0]++
		} else if bsonCommandOK(item["ok"]) && valid && count >= 0 && count <= 1 {
			switch native.action {
			case "create":
				b.observed[1] += count
			case "delete":
				b.observed[2] += count
			default:
				modified, valid := writeCount(item["nModified"])
				b.uncertainCounts = b.uncertainCounts || !valid
				b.observed[4] += modified
				if item["upserted"].Type != 0 {
					b.observed[5] += count
				} else {
					b.observed[3] += count
				}
			}
		} else {
			b.uncertainCounts = true
		}
		b.received++
	}
	if b.cursor.cursor == 0 && b.received == len(b.plans) && !b.uncertainCounts && b.counts != b.observed {
		return false
	}
	if b.cursor.cursor != 0 && b.received == len(b.plans) {
		return false
	}
	return true
}

func writeAuthorizationRejection(fields map[string]bson.RawValue) bool {
	code, valid := writeCount(fields["code"])
	if !valid || code != 13 {
		return false
	}
	for key, value := range fields {
		switch key {
		case "ok", "code", "operationTime", "$clusterTime":
		case "errmsg":
			if _, valid := value.StringValueOK(); !valid {
				return false
			}
		case "codeName":
			if name, valid := value.StringValueOK(); !valid || name != "Unauthorized" {
				return false
			}
		case "nErrors", "nInserted", "nDeleted", "nMatched", "nModified", "nUpserted":
			if count, valid := writeCount(value); !valid || count != 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// The array walker allocates at most one raw reference per qualified operation.
// Raw command execution avoids the driver's cursor document materialization.
func cursorDocuments(fields map[string]bson.RawValue, state *recordCursor, namespace string, first bool) ([]bson.Raw, bool) {
	cursor, valid := fields["cursor"].DocumentOK()
	if !valid {
		return nil, false
	}
	values, err := bsonFields(cursor)
	if err != nil {
		return nil, false
	}
	id, valid := values["id"].Int64OK()
	if !valid {
		return nil, false
	}
	state.cursor, state.cursorKnown = id, true
	ns, valid := values["ns"].StringValueOK()
	if !valid || ns != namespace {
		return nil, false
	}
	batchName, other := "nextBatch", "firstBatch"
	if first {
		batchName, other = other, batchName
	}
	for key := range values {
		if key != "id" && key != "ns" && key != batchName || key == other {
			return nil, false
		}
	}
	batch, valid := values[batchName].ArrayOK()
	if !valid || !bsonFraming(batch) {
		return nil, false
	}
	rest := batch[4 : len(batch)-1]
	documents := make([]bson.Raw, 0)
	for len(rest) > 0 {
		if len(documents) >= state.items {
			return nil, false
		}
		element, tail, valid := bsoncore.ReadElement(rest)
		if !valid {
			return nil, false
		}
		rest = tail
		key, err := element.KeyErr()
		if err != nil || key != strconv.Itoa(len(documents)) {
			return nil, false
		}
		value, err := element.ValueErr()
		if err != nil || value.Type != bsoncore.TypeEmbeddedDocument || !bsonFraming(value.Data) {
			return nil, false
		}
		documents = append(documents, value.Data)
	}
	return documents, true
}

func bulkItemReply(n *plan, fields map[string]bson.RawValue, concern bool) *pb.MutationResult {
	unknown := protocol.Mutation(pb.MutationOutcome_UNKNOWN, protocol.Fail(pb.FailureCode_UNAVAILABLE, "write item acknowledgement unavailable or incomplete"))
	count, valid := writeCount(fields["n"])
	if !bsonCommandOK(fields["ok"]) {
		if !writeZero(fields["ok"]) || fields["n"].Type != 0 && (!valid || count != 0) {
			return unknown
		}
		if modified, exists := fields["nModified"]; exists {
			count, valid := writeCount(modified)
			if !valid || count != 0 {
				return unknown
			}
		}
		if fields["upserted"].Type != 0 {
			return unknown
		}
		return writeRejection(fields["code"], unknown)
	}
	if !valid || count < 0 || count > 1 {
		return unknown
	}
	if concern || fields["code"].Type != 0 || fields["errmsg"].Type != 0 {
		return unknown
	}
	for key := range fields {
		if key != "ok" && key != "idx" && key != "n" && key != "nModified" && key != "upserted" {
			return unknown
		}
	}
	switch n.action {
	case "create":
		if count != 1 {
			return unknown
		}
	case "delete":
	case "put", "replace", "expression":
		modified, valid := writeCount(fields["nModified"])
		if !valid || modified < 0 || modified > count {
			return unknown
		}
		if upserted, exists := fields["upserted"]; exists {
			doc, valid := upserted.DocumentOK()
			id, validID := rawRecordID(doc.Lookup("_id"))
			if n.action != "put" || !valid || !validID || id != n.id || count != 1 || modified != 0 {
				return unknown
			}
		} else if count == 0 {
			if n.action == "put" {
				return unknown
			}
			return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "record missing"))
		}
	default:
		return unknown
	}
	return protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
}

func writeCount(raw bson.RawValue) (int64, bool) {
	switch raw.Type {
	case bson.TypeInt32:
		return int64(raw.Int32()), true
	case bson.TypeInt64:
		return raw.Int64(), true
	}
	return 0, false
}

func writeRejection(raw bson.RawValue, unknown *pb.MutationResult) *pb.MutationResult {
	code, ok := writeCount(raw)
	if !ok {
		return unknown
	}
	switch code {
	case 18:
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_UNAUTHENTICATED, "backend authentication required"))
	case 13:
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_PERMISSION_DENIED, "backend permission denied"))
	case 26:
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_TARGET_NOT_FOUND, "target collection does not exist"))
	case 112:
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_CONFLICT, "native mutation conflict"))
	case 2, 14, 28, 40, 66, 121, 11000:
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "native mutation rejected"))
	}
	return unknown
}

func writeZero(raw bson.RawValue) bool {
	switch raw.Type {
	case bson.TypeDouble:
		return raw.Double() == 0
	case bson.TypeInt32:
		return raw.Int32() == 0
	case bson.TypeInt64:
		return raw.Int64() == 0
	}
	return false
}
