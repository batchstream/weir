package mongodb

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"google.golang.org/protobuf/proto"
)

func (a *Adapter) executeRecords(ctx context.Context, plans []*execution.Plan) []*pb.Event {
	results := make([]*pb.Event, len(plans))
	bytes := 0
	for _, p := range plans {
		charge := max(p.Bytes, proto.Size(p.Command)+16)
		if p.Bytes < 0 || charge > execution.BackendBatchBytes || bytes > execution.BackendBatchBytes-charge {
			bytes = execution.BackendBatchBytes + 1
			break
		}
		bytes += charge
	}
	if bytes > execution.BackendBatchBytes {
		failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "MongoDB batch exceeds encoded input byte bound")
		for i, p := range plans {
			results[i] = execution.FailedEvent(p.Command, pb.MutationOutcome_NOT_STARTED, failure)
		}
		return results
	}
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
	for _, group := range groups {
		if group.qualified && len(group.writes) != 0 {
			replies := a.executeWrites(ctx, group.writes)
			for i, reply := range replies {
				results[group.writePositions[i]] = reply
			}
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
	command := bson.D{
		{Key: "find", Value: target.collection},
		{Key: "filter", Value: filter},
		{Key: "limit", Value: int64(len(ids))},
		{Key: "batchSize", Value: int32(len(ids))},
		{Key: "allowPartialResults", Value: false},
	}
	state := &recordCursor{target: target, items: len(ids)}
	session, err := a.client.StartSession()
	valid := err == nil
	received := 0
	stopped := false
	if valid {
		state.session = session
		defer a.closeRecordCursor(state)
		ctx = mongo.NewSessionContext(ctx, session)
		for page := 0; page < len(ids); page++ {
			attempt := ctx
			var release context.CancelFunc
			if page != 0 {
				attempt, release = nativeAttemptContext(ctx)
			}
			raw, readErr := a.client.Database(target.database).RunCommand(attempt, command).Raw()
			if release != nil {
				release()
			}
			err = readErr
			if err != nil {
				valid = false
				break
			}
			fields, parseErr := scanFields(raw)
			valid = parseErr == nil && len(raw) <= scanNativeLimit && scanOK(fields["ok"])
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
			for _, raw := range documents {
				id, validID := rawRecordID(raw.Lookup("_id"))
				matches, found := positions[id]
				if !validID || !found || results[matches[0]] != nil {
					valid = false
					break
				}
				oversized := len(raw) > protocol.MaxDocument
				if !oversized {
					nodes := 65536
					if !validScanBSON(raw, 0, &nodes) {
						valid = false
						break
					}
				}
				var document *pb.ReadResult
				for _, i := range matches {
					var reply *pb.ReadResult
					if oversized {
						reply = protocol.ReadFailure(protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "stored record exceeds read limit"))
					} else if len(raw) > plans[i].ResultBytes-execution.ResultOverheadBytes {
						reply = protocol.ReadFailure(protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "record exceeds result reservation"))
					} else {
						// Duplicate reads can share immutable data, but every RPC
						// pays its own response budget before retaining that data.
						if document == nil {
							d := &pb.Document{ContentType: "application/bson", Data: append([]byte(nil), raw...)}
							document = protocol.ReadDocument(d)
						}
						reply = document
					}
					value := &pb.Event_ReadResult{ReadResult: reply}
					event := &pb.Event{Value: value}
					results[i] = event
				}
				received++
			}
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
	active := make([]*execution.Plan, 0, len(plans))
	positions := make([]int, 0, len(plans))
	ops := make(bson.A, 0, len(plans))
	target := plans[0].Backend.(*plan).target
	for i, p := range plans {
		if result := unstarted(ctx, p); result != nil {
			results[i] = result
			continue
		}

		n := p.Backend.(*plan)
		filter := bson.D{{Key: "_id", Value: n.id}}
		var op bson.D
		switch n.action {
		case "create":
			op = bson.D{{Key: "insert", Value: int32(0)}, {Key: "document", Value: n.document}}
		case "put", "replace", "expression":
			op = bson.D{
				{Key: "update", Value: int32(0)},
				{Key: "filter", Value: filter},
				{Key: "updateMods", Value: n.document},
				{Key: "multi", Value: false},
				{Key: "upsert", Value: n.action == "put"},
			}
		case "delete":
			op = bson.D{{Key: "delete", Value: int32(0)}, {Key: "filter", Value: filter}, {Key: "multi", Value: false}}
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

	namespaceInfo := bson.D{{Key: "ns", Value: target.String()}}
	concern := bson.D{{Key: "w", Value: "majority"}}
	cursorOpts := bson.D{{Key: "batchSize", Value: int32(len(active))}}
	command := bson.D{
		{Key: "bulkWrite", Value: int32(1)},
		{Key: "ops", Value: ops},
		{Key: "nsInfo", Value: bson.A{namespaceInfo}},
		{Key: "ordered", Value: false},
		{Key: "errorsOnly", Value: false},
		{Key: "cursor", Value: cursorOpts},
		{Key: "writeConcern", Value: concern},
	}
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
	defer a.closeRecordCursor(&state.cursor)
	ctx = mongo.NewSessionContext(ctx, session)
	raw, err := a.client.Database("admin").RunCommand(ctx, command).Raw()
	if err != nil && len(raw) == 0 {
		var commandError mongo.CommandError
		if errors.As(err, &commandError) {
			raw = commandError.Raw
		}
	}
	valid := state.reply(raw, true)
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
	plans           []*execution.Plan
	results         []*pb.MutationResult
	cursor          recordCursor
	received        int
	concern         bool
	counts          [6]int64
	observed        [6]int64
	uncertainCounts bool
}

func (a *Adapter) closeRecordCursor(state *recordCursor) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = a.closeRecordCursorState(ctx, state)
}

func (b *writeBatch) reply(raw bson.Raw, first bool) bool {
	if len(raw) > scanNativeLimit {
		return false
	}
	nodes := 65536
	if !validScanBSON(raw, 0, &nodes) {
		return false
	}
	fields, err := scanFields(raw)
	if err != nil {
		return false
	}
	if !scanOK(fields["ok"]) {
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
		metadata, err := scanFields(doc)
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
		item, err := scanFields(raw)
		index, valid := writeCount(item["idx"])
		if err != nil || !valid || index < 0 || index >= int64(len(b.plans)) || b.results[index] != nil {
			return false
		}
		native := b.plans[index].Backend.(*plan)
		b.results[index] = bulkItemReply(native, item, b.concern)
		count, valid := writeCount(item["n"])
		if writeZero(item["ok"]) {
			b.observed[0]++
		} else if scanOK(item["ok"]) && valid && count >= 0 && count <= 1 {
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

// The array walker allocates at most one raw reference per qualified operation.
// Raw command execution avoids the driver's cursor document materialization.
func cursorDocuments(fields map[string]bson.RawValue, state *recordCursor, namespace string, first bool) ([]bson.Raw, bool) {
	cursor, valid := fields["cursor"].DocumentOK()
	if !valid {
		return nil, false
	}
	values, err := scanFields(cursor)
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
	if !valid || !scanFraming(batch) {
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
		if err != nil || value.Type != bsoncore.TypeEmbeddedDocument || !scanFraming(value.Data) {
			return nil, false
		}
		documents = append(documents, value.Data)
	}
	return documents, true
}

func bulkItemReply(n *plan, fields map[string]bson.RawValue, concern bool) *pb.MutationResult {
	unknown := protocol.Mutation(pb.MutationOutcome_UNKNOWN, protocol.Fail(pb.FailureCode_UNAVAILABLE, "write item acknowledgement unavailable or incomplete"))
	count, valid := writeCount(fields["n"])
	if !scanOK(fields["ok"]) {
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
