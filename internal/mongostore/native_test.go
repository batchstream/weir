package mongostore

import (
	"bytes"
	"strings"
	"testing"

	pb "github.com/batchstream/weir/api/weir/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type nativeCapture struct {
	head   *pb.NativeHead
	body   bytes.Buffer
	chunks int
}

func (c *nativeCapture) Head(h *pb.NativeHead) error { c.head = h; return nil }
func (c *nativeCapture) Chunk(b []byte) error        { c.chunks++; _, err := c.body.Write(b); return err }
func (c *nativeCapture) Interrupt()                  {}
func TestMongoNativeCommandScope(t *testing.T) {
	cfg := Config{Store: "mongo", Database: "db", Collection: "records"}
	a := &Adapter{config: cfg}
	for _, name := range []string{"find", "aggregate", "getMore", "killCursors", "drop", "insert", "eval", "startSession"} {
		command := bson.D{{Key: name, Value: "records"}}
		raw, _ := bson.Marshal(command)
		if a.nativeCommand(raw) == nil {
			t.Fatal(name)
		}
	}
	for _, key := range []string{"$db", "lsid", "txnNumber", "autocommit", "startTransaction", "writeConcern", "readConcern", "pipeline", "let"} {
		command := bson.D{{Key: "findAndModify", Value: "records"}, {Key: key, Value: 1}}
		raw, _ := bson.Marshal(command)
		if a.nativeCommand(raw) == nil {
			t.Fatal(key)
		}
	}
	for _, command := range []bson.D{
		{{Key: "count", Value: "other"}},
		{{Key: "findAndModify", Value: "records"}, {Key: "update", Value: bson.A{}}},
		{{Key: "count", Value: "records"}, {Key: "count", Value: "other"}},
	} {
		raw, _ := bson.Marshal(command)
		if a.nativeCommand(raw) == nil {
			t.Fatal(command)
		}
	}
}

func TestMongoNativeRejectsCode(t *testing.T) {
	cfg := Config{Collection: "records"}
	a := &Adapter{config: cfg}
	for _, key := range []string{"$where", "$function", "$accumulator"} {
		query := bson.D{{Key: key, Value: "code"}}
		command := bson.D{{Key: "count", Value: "records"}, {Key: "query", Value: query}}
		raw, _ := bson.Marshal(command)
		if a.nativeCommand(raw) == nil {
			t.Fatal(key)
		}
	}
}

func TestMongoNativeExactCommandBound(t *testing.T) {
	cfg := Config{Collection: "records"}
	a := &Adapter{config: cfg}
	query := bson.D{{Key: "pad", Value: ""}}
	command := bson.D{{Key: "count", Value: "records"}, {Key: "query", Value: query}}
	base, _ := bson.Marshal(command)
	for _, extra := range []int{0, 1} {
		query[0].Value = strings.Repeat("x", NativeCommandLimit-len(base)+extra)
		raw, _ := bson.Marshal(command)
		f := a.nativeCommand(raw)
		if extra == 0 && f != nil || extra == 1 && f == nil {
			t.Fatal(extra, len(raw), f)
		}
	}
}
