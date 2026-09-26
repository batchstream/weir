package mongostore

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"google.golang.org/protobuf/proto"
)

// The connection guard caps wire bytes and metadata expansion before the driver
// allocates/decodes its copy. RunCommand.Raw retains the driver buffer without
// cursor batch decoding. Reserve both wire buffers plus bounded metadata/framing;
// this is separate from the one output-frame credit, not a claim about RSS.
const scanPageBudget = 128 << 20
const scanNativeLimit = 48 << 20

type scanPlan struct {
	options        bson.D
	items          int
	session        *mongo.Session
	cursor         int64
	opened, closed bool
	cursorKnown    bool
}

func (a *Adapter) PrepareScan(req *pb.ScanRequest) (*execution.Plan, *pb.Failure) {
	if f := protocol.ValidateScan(req, a.config.Store); f != nil {
		return nil, f
	}
	_, parts, _ := protocol.ParseResource(req.Resource)
	if len(parts) != 2 || parts[0] != a.config.Database || parts[1] != a.config.Collection {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "only the configured collection supports Scan")
	}
	if req.ReadMediaType != "" && req.ReadMediaType != "application/bson" {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Scan outputs native BSON")
	}
	native := &scanPlan{items: protocol.FetchItems(req.FetchItemsHint)}
	if d := req.Selector; d != nil {
		if d.MediaType != "application/bson" {
			return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "find selector requires BSON")
		}
		// Existing bounded codec validates selector structure before any materialized
		// BSON option list. Scalar types have their original native BSON semantics.
		if _, err := Decode(d.Data); err != nil {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid or excessive BSON selector")
		}
		fields, err := scanFields(d.Data)
		if err != nil {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid selector")
		}
		// Maintain selector field order. No caller command, lifecycle or partial flags.
		elements, _ := bson.Raw(d.Data).Elements()
		for _, e := range elements {
			key := e.Key()
			if key != "filter" && key != "sort" && key != "projection" {
				return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "unsupported find selector option")
			}
			if fields[key].Type != bson.TypeEmbeddedDocument {
				return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "find option must be a document")
			}
			option := bson.E{Key: key, Value: e.Value()}
			native.options = append(native.options, option)
		}
	}
	p := &execution.Plan{Scan: true, Key: req.Resource, Token: "scan", Bytes: proto.Size(req) + protocol.EntryOverhead + 4096, ResultBytes: protocol.MaxDocument + protocol.ResultOverhead, PageBytes: scanPageBudget, Backend: native}
	return p, nil
}

func (a *Adapter) FetchScan(ctx context.Context, p *execution.Plan) (*execution.ScanPage, execution.Feedback) {
	n := p.Backend.(*scanPlan)
	page := &execution.ScanPage{}
	if n.closed || ctx.Err() != nil {
		page.Failure = protocol.ContextFailure(ctx)
		return page, execution.Neutral
	}
	first := !n.opened
	var command bson.D
	if first {
		n.opened = true // An unsuccessful first attempt must never be restarted.
		session, err := a.client.StartSession()
		if err != nil {
			page.Failure = backendFailure(ctx, err)
			return page, execution.Neutral
		}
		n.session = session
		command = bson.D{{Key: "find", Value: a.config.Collection}, {Key: "batchSize", Value: int32(n.items)}, {Key: "allowPartialResults", Value: false}}
		command = append(command, n.options...)
	} else {
		if n.cursor == 0 {
			page.Failure = protocol.Fail(pb.FailureCode_INTERNAL, "cursor already exhausted")
			return page, execution.Neutral
		}
		command = bson.D{{Key: "getMore", Value: n.cursor}, {Key: "collection", Value: a.config.Collection}, {Key: "batchSize", Value: int32(n.items)}}
	}
	ctx = mongo.NewSessionContext(ctx, n.session)
	attempt := ctx
	if !first {
		// RunCommand's CSOT adds maxTimeMS to every command, but MongoDB
		// forbids it on non-tailable getMore. Retain the same absolute parent
		// cancellation via the already-qualified socket-cancellation bridge.
		var release context.CancelFunc
		attempt, release = nativeAttemptContext(ctx)
		defer release()
	}
	raw, err := a.client.Database(a.config.Database).RunCommand(attempt, command).Raw()
	if err != nil {
		page.Failure = backendFailure(ctx, err)
		if mongo.IsTimeout(err) {
			page.Failure = protocol.Fail(pb.FailureCode_DEADLINE_EXCEEDED, "cursor fetch timed out")
		}
		return page, feedback(ctx, err)
	}
	page = a.scanReply(raw, n, first)
	if page.Failure != nil {
		return page, execution.Neutral
	}
	return page, execution.Healthy
}

// Parse the bounded command envelope and batch. Copy bounded documents unchanged
// so one output frame cannot pin a much larger driver message after page release.
// Never use Cursor.Next's false result as exhaustion evidence.
func (a *Adapter) scanReply(raw bson.Raw, n *scanPlan, first bool) *execution.ScanPage {
	page := &execution.ScanPage{}
	invalid := protocol.Fail(pb.FailureCode_INTERNAL, "incomplete or malformed cursor response")
	page.Failure = invalid
	if len(raw) > scanNativeLimit {
		page.Failure = protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "cursor response exceeds native bound")
		return page
	}
	fields, err := scanFields(raw)
	if err != nil {
		return page
	}
	cursor, ok := fields["cursor"].DocumentOK()
	if !ok {
		return page
	}
	values, err := scanFields(cursor)
	if err != nil {
		return page
	}
	id, ok := values["id"].Int64OK()
	if !ok {
		return page
	}
	// Retain the actual returned state even if later metadata invalidates the page.
	n.cursor = id
	n.cursorKnown = true
	ns, ok := values["ns"].StringValueOK()
	if !ok || ns != a.config.Database+"."+a.config.Collection {
		return page
	}
	good := fields["ok"]
	if !scanOK(good) {
		return page
	}
	for key := range fields {
		if key != "ok" && key != "cursor" && key != "operationTime" && key != "$clusterTime" && key != "partialResultsReturned" {
			return page
		}
	}
	for _, envelope := range []map[string]bson.RawValue{fields, values} {
		if partial, exists := envelope["partialResultsReturned"]; exists {
			value, valid := partial.BooleanOK()
			if !valid {
				return page
			}
			if value {
				page.Failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "partial cursor results")
				return page
			}
		}
		for _, key := range []string{"errmsg", "code", "writeConcernError", "writeErrors", "errors"} {
			if _, exists := envelope[key]; exists {
				return page
			}
		}
	}
	batchName := "nextBatch"
	if first {
		batchName = "firstBatch"
	}
	other := "firstBatch"
	if first {
		other = "nextBatch"
	}
	if _, exists := values[other]; exists {
		return page
	}
	batch, ok := values[batchName].ArrayOK()
	if !ok || !scanFraming(batch) {
		return page
	}
	rest := batch[4 : len(batch)-1]
	docs := make([]*pb.Document, 0, n.items)
	for len(rest) > 0 {
		if len(docs) >= n.items {
			page.Failure = protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "cursor batch exceeds item bound")
			return page
		}
		element, tail, valid := bsoncore.ReadElement(rest)
		if !valid {
			return page
		}
		rest = tail
		value, err := element.ValueErr()
		if err != nil || value.Type != bsoncore.TypeEmbeddedDocument {
			return page
		}
		key, err := element.KeyErr()
		if err != nil || key != strconv.Itoa(len(docs)) {
			return page
		}
		if len(value.Data) > protocol.MaxDocument {
			page.Failure = protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "stored Scan document exceeds output bound")
			return page
		}
		nodes := 65536
		if !validScanBSON(value.Data, 0, &nodes) {
			return page
		}
		doc := &pb.Document{MediaType: "application/bson", Data: append([]byte(nil), value.Data...)}
		docs = append(docs, doc)
	}
	page.Documents = docs
	page.Exhausted = id == 0
	page.Failure = nil
	return page
}

func scanOK(value bson.RawValue) bool {
	switch value.Type {
	case bson.TypeDouble:
		return value.Double() == 1
	case bson.TypeInt32:
		return value.Int32() == 1
	case bson.TypeInt64:
		return value.Int64() == 1
	}
	return false
}

// Validation walks bounded raw bytes; it never builds a native document tree or
// converts BSON types. MongoDB's native maximum nesting depth is 100.
func validScanBSON(raw []byte, depth int, nodes *int) bool {
	if depth > 100 || !scanFraming(raw) {
		return false
	}
	rest := raw[4 : len(raw)-1]
	for len(rest) > 0 {
		*nodes--
		if *nodes < 0 {
			return false
		}
		element, tail, ok := bsoncore.ReadElement(rest)
		if !ok {
			return false
		}
		rest = tail
		value, err := element.ValueErr()
		if err != nil {
			return false
		}
		switch value.Type {
		case bsoncore.TypeEmbeddedDocument, bsoncore.TypeArray:
			if !validScanBSON(value.Data, depth+1, nodes) {
				return false
			}
		case bsoncore.TypeCodeWithScope:
			_, scope, ok := value.CodeWithScopeOK()
			if !ok || !validScanBSON(scope, depth+1, nodes) {
				return false
			}
		default:
			if value.Validate() != nil {
				return false
			}
		}
	}
	return true
}

func scanFraming(raw []byte) bool {
	return len(raw) >= 5 && int64(binary.LittleEndian.Uint32(raw)) == int64(len(raw)) && raw[len(raw)-1] == 0
}
func scanFields(raw []byte) (map[string]bson.RawValue, error) {
	if !scanFraming(raw) {
		return nil, fmt.Errorf("invalid BSON framing")
	}
	fields := make(map[string]bson.RawValue)
	rest := raw[4 : len(raw)-1]
	for len(rest) > 0 {
		if len(fields) >= 32 {
			return nil, fmt.Errorf("envelope field limit")
		}
		element, tail, ok := bsoncore.ReadElement(rest)
		if !ok {
			return nil, fmt.Errorf("invalid element")
		}
		rest = tail
		key, err := element.KeyErr()
		if err != nil {
			return nil, err
		}
		if _, exists := fields[key]; exists {
			return nil, fmt.Errorf("duplicate field")
		}
		value, err := element.ValueErr()
		if err != nil {
			return nil, err
		}
		fields[key] = bson.RawValue{Type: bson.Type(value.Type), Value: value.Data}
	}
	return fields, nil
}

func (a *Adapter) CloseScan(ctx context.Context, p *execution.Plan) *pb.Failure {
	n := p.Backend.(*scanPlan)
	if n.closed {
		return nil
	}
	n.closed = true
	defer func() {
		n.cursor = 0
		n.options = nil
		if n.session != nil {
			n.session.EndSession(ctx)
			n.session = nil
		}
	}()
	if n.session == nil {
		return nil
	}
	if !n.cursorKnown {
		// A lost find reply can hide a newly allocated cursor. The explicit
		// session is still known and exclusively held, so target only that
		// session instead of silently omitting cleanup of the unknown cursor.
		command := bson.D{{Key: "killSessions", Value: bson.A{n.session.ID()}}}
		raw, err := a.client.Database("admin").RunCommand(ctx, command).Raw()
		if err != nil {
			return protocol.Fail(pb.FailureCode_UNAVAILABLE, "remote Scan session cleanup unconfirmed")
		}
		fields, err := scanFields(raw)
		if err != nil || !scanOK(fields["ok"]) {
			return protocol.Fail(pb.FailureCode_UNAVAILABLE, "remote Scan session cleanup unconfirmed")
		}
		return nil
	}
	if n.cursor == 0 {
		return nil
	}
	command := bson.D{{Key: "killCursors", Value: a.config.Collection}, {Key: "cursors", Value: bson.A{n.cursor}}}
	ctx = mongo.NewSessionContext(ctx, n.session)
	raw, err := a.client.Database(a.config.Database).RunCommand(ctx, command).Raw()
	if err != nil {
		return protocol.Fail(pb.FailureCode_UNAVAILABLE, "remote cursor cleanup unconfirmed")
	}
	fields, err := scanFields(raw)
	if err != nil {
		return protocol.Fail(pb.FailureCode_INTERNAL, "invalid cursor cleanup response")
	}
	if !scanOK(fields["ok"]) {
		return protocol.Fail(pb.FailureCode_INTERNAL, "invalid cursor cleanup status")
	}
	confirmed := false
	for _, key := range []string{"cursorsKilled", "cursorsNotFound", "cursorsAlive", "cursorsUnknown"} {
		array, ok := fields[key].ArrayOK()
		if !ok || !scanFraming(array) {
			return protocol.Fail(pb.FailureCode_INTERNAL, "invalid cursor cleanup envelope")
		}
		rest := array[4 : len(array)-1]
		if len(rest) == 0 {
			continue
		}
		element, tail, ok := bsoncore.ReadElement(rest)
		if !ok || len(tail) != 0 || key == "cursorsAlive" || key == "cursorsUnknown" || confirmed {
			return protocol.Fail(pb.FailureCode_UNAVAILABLE, "remote cursor cleanup unconfirmed")
		}
		value, err := element.ValueErr()
		if err != nil {
			return protocol.Fail(pb.FailureCode_INTERNAL, "invalid cursor cleanup identity")
		}
		id, ok := value.Int64OK()
		if !ok || id != n.cursor {
			return protocol.Fail(pb.FailureCode_INTERNAL, "invalid cursor cleanup identity")
		}
		confirmed = true
	}
	if confirmed {
		return nil
	}
	return protocol.Fail(pb.FailureCode_UNAVAILABLE, "remote cursor cleanup unconfirmed")
}
