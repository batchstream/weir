package mongodb

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoScanEnvelopeIntegrity(t *testing.T) {
	for _, first := range []bool{false, true} {
		for _, mode := range []string{"valid", "empty_live", "exhausted", "partial", "partial_top", "partial_type", "missing_id", "missing_ns", "wrong_ns", "missing_batch", "wrong_batch", "error_tail", "oversized", "too_many", "truncated", "duplicate"} {
			t.Run(mode+map[bool]string{true: "_first", false: "_more"}[first], func(t *testing.T) {
				a := &Adapter{config: Config{}}
				target := namespace{database: "db", collection: "records"}
				n := &recordCursor{target: target, items: 1, cursor: 9}
				name := "nextBatch"
				if first {
					name = "firstBatch"
				}
				doc := bson.D{{Key: "_id", Value: int64(9223372036854775807)}}
				docs := bson.A{doc}
				cursor := bson.D{{Key: "id", Value: int64(12)}, {Key: "ns", Value: "db.records"}}
				switch mode {
				case "empty_live":
					docs = nil
				case "exhausted":
					cursor[0].Value = int64(0)
					docs = bson.A{}
				case "partial":
					field := bson.E{Key: "partialResultsReturned", Value: true}
					cursor = append(cursor, field)
				case "partial_type":
					field := bson.E{Key: "partialResultsReturned", Value: "false"}
					cursor = append(cursor, field)
				case "missing_id":
					cursor = cursor[1:]
				case "missing_ns":
					cursor = cursor[:1]
				case "wrong_ns":
					cursor[1].Value = "other.records"
				case "oversized":
					field := bson.E{Key: "pad", Value: strings.Repeat("x", protocol.MaxDocument)}
					doc = append(doc, field)
					docs = bson.A{doc}
				case "too_many":
					docs = append(docs, doc)
				case "wrong_batch":
					name = "unexpectedBatch"
				}
				if mode == "empty_live" {
					docs = bson.A{}
				}
				if mode != "missing_batch" {
					field := bson.E{Key: name, Value: docs}
					cursor = append(cursor, field)
				}
				envelope := bson.D{{Key: "cursor", Value: cursor}, {Key: "ok", Value: 1.0}}
				if mode == "partial_top" {
					field := bson.E{Key: "partialResultsReturned", Value: true}
					envelope = append(envelope, field)
				}
				if mode == "error_tail" {
					field := bson.E{Key: "errmsg", Value: "failed"}
					envelope = append(envelope, field)
				}
				if mode == "duplicate" {
					field := bson.E{Key: "ok", Value: 1.0}
					envelope = append(envelope, field)
				}
				raw, _ := bson.Marshal(envelope)
				if mode == "truncated" {
					raw = raw[:len(raw)-1]
				}
				page := a.recordCursorReply(raw, n, first)
				valid := mode == "valid" || mode == "exhausted" || mode == "empty_live"
				if valid {
					if page.Failure != nil || page.Exhausted != (mode == "exhausted") {
						t.Fatal(page)
					}
				} else if page.Failure == nil || len(page.Documents) != 0 {
					t.Fatal("failed page leaked documents", page)
				}
				if (mode == "partial" || mode == "error_tail" || mode == "oversized") && n.cursor != 12 {
					t.Fatal("latest cursor not retained")
				}
			})
		}
	}
}
func TestMongoScanSelectorControls(t *testing.T) {
	a := &Adapter{config: Config{Store: "mongo"}}
	for _, name := range []string{"find", "aggregate", "allowPartialResults", "tailable", "awaitData", "noCursorTimeout", "batchSize", "limit", "skip", "singleBatch", "maxTimeMS", "getMore", "lsid", "readConcern", "collation"} {
		selector := bson.D{{Key: name, Value: true}}
		raw, _ := bson.Marshal(selector)
		doc := &pb.Document{MediaType: "application/bson", Data: raw}
		req := &pb.ScanRequest{Resource: "weir://mongo/db/records", Selector: doc}
		if _, f := a.prepareScan(req); f == nil {
			t.Fatal("allowed unsafe option", name)
		}
	}
	raw := make([]byte, protocol.MaxSelector+1)
	binary.LittleEndian.PutUint32(raw, uint32(len(raw)))
	doc := &pb.Document{MediaType: "application/bson", Data: raw}
	req := &pb.ScanRequest{Resource: "weir://mongo/db/records", Selector: doc}
	if _, f := a.prepareScan(req); f == nil {
		t.Fatal("oversized selector")
	}
}

func TestMongoScanRejectsUnstableIdentitySelectorsAndTokens(t *testing.T) {
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	cases := []bson.D{
		{{Key: "sort", Value: bson.D{{Key: "n", Value: int32(1)}}}},
		{{Key: "sort", Value: bson.D{{Key: "_id", Value: int32(-1)}}}},
		{{Key: "projection", Value: bson.D{{Key: "_id", Value: int32(0)}}}},
		{{Key: "projection", Value: bson.D{{Key: "_id.n", Value: int32(1)}}}},
		{{Key: "projection", Value: bson.D{{Key: "_id", Value: "$n"}}}},
	}
	for _, selector := range cases {
		raw, _ := bson.Marshal(selector)
		document := &pb.Document{MediaType: "application/bson", Data: raw}
		request := &pb.ScanRequest{Resource: "weir://mongo/db/records", Selector: document}
		if _, failure := adapter.prepareScan(request); failure == nil {
			t.Fatal("unstable identity selector accepted", selector)
		}
	}
	request := &pb.ScanRequest{Resource: "weir://mongo/db/records"}
	fingerprint := protocol.ScanFingerprint(request, "mongodb")
	identities := []bson.D{
		{{Key: "other", Value: "id"}},
		{{Key: "_id", Value: bson.A{int32(1)}}},
		{{Key: "_id", Value: bson.Regex{Pattern: "id"}}},
	}
	for _, identity := range identities {
		raw, _ := bson.Marshal(identity)
		token, err := protocol.EncodeScanToken("mongodb", fingerprint, raw)
		if err != nil {
			t.Fatal(err)
		}
		request.ContinuationToken = token
		if _, failure := adapter.prepareScan(request); failure == nil {
			t.Fatal("invalid continuation identity accepted", identity)
		}
	}
}
