package mongodb

import (
	"context"
	"strconv"

	"github.com/batchstream/weir/api/protocol"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

type recordCursor struct {
	target      namespace
	items       int
	session     *mongo.Session
	cursor      int64
	closed      bool
	cursorKnown bool
}

// Parse the bounded command envelope and batch. Copy bounded documents unchanged
// so one output frame cannot pin a much larger driver message after page release.
// Never use Cursor.Next's false result as exhaustion evidence.
func (a *Adapter) recordCursorReply(raw bson.Raw, n *recordCursor, first bool) *execution.ScanPage {
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
	if !ok || ns != n.target.String() {
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

func (a *Adapter) closeRecordCursorState(ctx context.Context, n *recordCursor) *pb.Failure {
	if n.closed {
		return nil
	}
	n.closed = true
	defer func() {
		n.cursor = 0
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
	command := bson.D{{Key: "killCursors", Value: n.target.collection}, {Key: "cursors", Value: bson.A{n.cursor}}}
	ctx = mongo.NewSessionContext(ctx, n.session)
	raw, err := a.client.Database(n.target.database).RunCommand(ctx, command).Raw()
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
