package mongodb

import (
	"context"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
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
	if n.cursorKnown && n.cursor == 0 {
		return nil
	}
	if !n.cursorKnown {
		// A lost find reply can hide a newly allocated cursor. The explicit
		// session is still known and exclusively held, so target only that
		// session instead of silently omitting cleanup of the unknown cursor.
		command := bson.D{{Key: "killSessions", Value: bson.A{n.session.ID()}}}
		raw, err := a.client.Database("admin").RunCommand(ctx, command).Raw()
		if err != nil {
			return protocol.Fail(pb.FailureCode_UNAVAILABLE, "remote record session cleanup unconfirmed")
		}
		fields, err := bsonFields(raw)
		if err != nil || !bsonCommandOK(fields["ok"]) {
			return protocol.Fail(pb.FailureCode_UNAVAILABLE, "remote record session cleanup unconfirmed")
		}
		return nil
	}
	command := bson.D{{Key: "killCursors", Value: n.target.collection}, {Key: "cursors", Value: bson.A{n.cursor}}}
	ctx = mongo.NewSessionContext(ctx, n.session)
	raw, err := a.client.Database(n.target.database).RunCommand(ctx, command).Raw()
	if err != nil {
		return protocol.Fail(pb.FailureCode_UNAVAILABLE, "remote cursor cleanup unconfirmed")
	}
	fields, err := bsonFields(raw)
	if err != nil {
		return protocol.Fail(pb.FailureCode_INTERNAL, "invalid cursor cleanup response")
	}
	if !bsonCommandOK(fields["ok"]) {
		return protocol.Fail(pb.FailureCode_INTERNAL, "invalid cursor cleanup status")
	}
	confirmed := false
	for _, key := range []string{"cursorsKilled", "cursorsNotFound", "cursorsAlive", "cursorsUnknown"} {
		array, ok := fields[key].ArrayOK()
		if !ok || !bsonFraming(array) {
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
