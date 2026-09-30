package mongodb

import (
	"context"
	"errors"
	"strconv"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"google.golang.org/protobuf/proto"
)

func (a *Adapter) Execute(ctx context.Context, plans []*execution.Plan) ([]*pb.BulkResult, execution.Feedback) {
	results := make([]*pb.BulkResult, len(plans))
	bytes := 0
	for _, p := range plans {
		charge := max(p.Bytes, proto.Size(p.Operation))
		if p.Bytes < 0 || charge > 8<<20 || bytes > (8<<20)-charge {
			bytes = 8<<20 + 1
			break
		}
		bytes += charge
	}
	if len(plans) > 128 || bytes > 8<<20 {
		failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "MongoDB batch exceeds operation or byte bound")
		for i, p := range plans {
			results[i] = protocol.ResultError(p.Operation, pb.MutationOutcome_NOT_STARTED, failure)
		}
		return results, execution.Neutral
	}
	var reads, writes []*execution.Plan
	var readPositions, writePositions, programs []int
	for i, p := range plans {
		switch p.Backend.(*plan).action {
		case "read":
			reads = append(reads, p)
			readPositions = append(readPositions, i)
		case "program":
			programs = append(programs, i)
		default:
			writes = append(writes, p)
			writePositions = append(writePositions, i)
		}
	}
	signal := execution.Healthy
	if len(reads) != 0 {
		replies, sample := a.executeReads(ctx, reads)
		for i, reply := range replies {
			results[readPositions[i]] = reply
		}
		signal = batchFeedback(signal, sample)
	}
	if len(writes) != 0 {
		replies, sample := a.executeWrites(ctx, writes)
		for i, reply := range replies {
			results[writePositions[i]] = reply
		}
		signal = batchFeedback(signal, sample)
	}
	for _, i := range programs {
		p := plans[i]
		if result := unstarted(ctx, p); result != nil {
			results[i] = result
			signal = batchFeedback(signal, execution.Neutral)
			continue
		}
		// Lua evaluates and commits in its own transaction. Combining independent
		// programs into one transaction would change their failure guarantees.
		replies, sample := a.executeProgram(ctx, p)
		results[i] = replies[0]
		signal = batchFeedback(signal, sample)
	}
	if len(plans) == 0 {
		signal = execution.Neutral
	}
	return results, signal
}

func batchFeedback(current, next execution.Feedback) execution.Feedback {
	if current == execution.Congested || next == execution.Congested {
		return execution.Congested
	}
	if current == execution.Neutral || next == execution.Neutral {
		return execution.Neutral
	}
	return execution.Healthy
}

func unstarted(ctx context.Context, p *execution.Plan) *pb.BulkResult {
	if p.Context != nil && p.Context.Err() != nil {
		return protocol.ResultError(p.Operation, pb.MutationOutcome_NOT_STARTED, protocol.ContextFailure(p.Context))
	}
	if ctx.Err() != nil {
		return protocol.ResultError(p.Operation, pb.MutationOutcome_NOT_STARTED, protocol.ContextFailure(ctx))
	}
	return nil
}

func (a *Adapter) executeReads(ctx context.Context, plans []*execution.Plan) ([]*pb.BulkResult, execution.Feedback) {
	results := make([]*pb.BulkResult, len(plans))
	skipped := make([]bool, len(plans))
	ids := make(bson.A, 0, len(plans))
	positions := make(map[any]int, len(plans))
	for i, p := range plans {
		if result := unstarted(ctx, p); result != nil {
			results[i] = result
			skipped[i] = true
			continue
		}
		id := p.Backend.(*plan).id
		positions[id] = i
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return results, execution.Neutral
	}
	selector := bson.D{{Key: "$in", Value: ids}}
	filter := bson.D{{Key: "_id", Value: selector}}
	command := bson.D{{Key: "find", Value: a.config.Collection}, {Key: "filter", Value: filter}, {Key: "limit", Value: int64(len(ids))}, {Key: "batchSize", Value: int32(len(ids))}, {Key: "allowPartialResults", Value: false}}
	state := &scanPlan{items: len(ids)}
	session, err := a.client.StartSession()
	valid := err == nil
	received := 0
	stopped := false
	if valid {
		state.session = session
		defer a.closeRecordCursor(state, a.config.Database, a.config.Collection)
		ctx = mongo.NewSessionContext(ctx, session)
		for page := 0; page < len(ids); page++ {
			attempt := ctx
			var release context.CancelFunc
			if page != 0 {
				attempt, release = nativeAttemptContext(ctx)
			}
			raw, readErr := a.client.Database(a.config.Database).RunCommand(attempt, command).Raw()
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
				documents, valid = cursorDocuments(fields, state, a.config.Database+"."+a.config.Collection, page == 0)
			}
			if !valid || len(documents) > len(ids)-received || len(documents) == 0 && state.cursor != 0 {
				valid = false
				break
			}
			for _, raw := range documents {
				id, validID := rawRecordID(raw.Lookup("_id"))
				i, found := positions[id]
				if !validID || !found || results[i] != nil {
					valid = false
					break
				}
				var reply *pb.ReadResult
				if len(raw) > protocol.MaxDocument {
					reply = protocol.ReadFailure(protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "stored record exceeds read limit"))
				} else {
					nodes := 65536
					if !validScanBSON(raw, 0, &nodes) {
						valid = false
						break
					}
					d := &pb.Document{MediaType: "application/bson", Data: append([]byte(nil), raw...)}
					reply = protocol.ReadDocument(d)
				}
				variant := &pb.BulkResult_Read{Read: reply}
				results[i] = &pb.BulkResult{Index: plans[i].Operation.Index, Result: variant}
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
			command = bson.D{{Key: "getMore", Value: state.cursor}, {Key: "collection", Value: a.config.Collection}, {Key: "batchSize", Value: int32(len(ids) - received)}}
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
		variant := &pb.BulkResult_Read{Read: reply}
		results[i] = &pb.BulkResult{Index: p.Operation.Index, Result: variant}
	}
	signal := feedback(ctx, err)
	if !valid && signal == execution.Healthy {
		signal = execution.Neutral
	}
	return results, signal
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

func (a *Adapter) executeWrites(ctx context.Context, plans []*execution.Plan) ([]*pb.BulkResult, execution.Feedback) {
	results := make([]*pb.BulkResult, len(plans))
	active := make([]*execution.Plan, 0, len(plans))
	positions := make([]int, 0, len(plans))
	ops := make(bson.A, 0, len(plans))
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
			op = bson.D{{Key: "update", Value: int32(0)}, {Key: "filter", Value: filter}, {Key: "updateMods", Value: n.document}, {Key: "multi", Value: false}, {Key: "upsert", Value: n.action == "put"}}
		case "delete":
			op = bson.D{{Key: "delete", Value: int32(0)}, {Key: "filter", Value: filter}, {Key: "multi", Value: false}}
		default:
			results[i] = protocol.ResultError(p.Operation, pb.MutationOutcome_NOT_STARTED, protocol.Fail(pb.FailureCode_INTERNAL, "unsupported prepared operation"))
			continue
		}
		active = append(active, p)
		positions = append(positions, i)
		ops = append(ops, op)
	}
	if len(active) == 0 {
		return results, execution.Neutral
	}
	namespace := bson.D{{Key: "ns", Value: a.config.Database + "." + a.config.Collection}}
	concern := bson.D{{Key: "w", Value: "majority"}}
	cursorOpts := bson.D{{Key: "batchSize", Value: int32(len(active))}}
	command := bson.D{{Key: "bulkWrite", Value: int32(1)}, {Key: "ops", Value: ops}, {Key: "nsInfo", Value: bson.A{namespace}}, {Key: "ordered", Value: false}, {Key: "errorsOnly", Value: false}, {Key: "cursor", Value: cursorOpts}, {Key: "writeConcern", Value: concern}}
	state := &writeBatch{plans: active, results: make([]*pb.MutationResult, len(active))}
	session, err := a.client.StartSession()
	if err != nil {
		for i, p := range active {
			results[positions[i]] = protocol.ResultError(p.Operation, pb.MutationOutcome_NOT_STARTED, backendFailure(ctx, err))
		}
		return results, feedback(ctx, err)
	}
	state.cursor.session = session
	state.cursor.items = len(active)
	defer a.closeRecordCursor(&state.cursor, "admin", "$cmd.bulkWrite")
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
		command = bson.D{{Key: "getMore", Value: state.cursor.cursor}, {Key: "collection", Value: "$cmd.bulkWrite"}, {Key: "batchSize", Value: int32(len(active) - state.received)}}
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
	for i, p := range active {
		reply := state.results[i]
		if reply == nil {
			reply = protocol.Mutation(pb.MutationOutcome_UNKNOWN, protocol.Fail(pb.FailureCode_UNAVAILABLE, "write acknowledgement unavailable or incomplete"))
		}
		variant := &pb.BulkResult_Mutation{Mutation: reply}
		results[positions[i]] = &pb.BulkResult{Index: p.Operation.Index, Result: variant}
	}
	signal := feedback(ctx, err)
	uncertain := false
	for _, result := range state.results {
		uncertain = uncertain || result == nil || result.Outcome == pb.MutationOutcome_UNKNOWN
	}
	if (!valid || uncertain) && signal == execution.Healthy {
		signal = execution.Neutral
	}
	return results, signal
}

type writeBatch struct {
	plans           []*execution.Plan
	results         []*pb.MutationResult
	cursor          scanPlan
	received        int
	concern         bool
	counts          [6]int64
	observed        [6]int64
	uncertainCounts bool
}

func (a *Adapter) closeRecordCursor(state *scanPlan, database, collection string) {
	// Reuse the qualified cursor cleanup path with the returned namespace.
	config := a.config
	config.Database, config.Collection = database, collection
	cleanup := &Adapter{client: a.client, config: config}
	work := &execution.Plan{Backend: state}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = cleanup.CloseScan(ctx, work)
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
func cursorDocuments(fields map[string]bson.RawValue, state *scanPlan, namespace string, first bool) ([]bson.Raw, bool) {
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
		if len(documents) >= min(state.items, 128) {
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
