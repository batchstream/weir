package mongodb

import (
	"bytes"
	"strings"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoScanFindUsesRemainingBoundedBatch(t *testing.T) {
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	for _, size := range []uint32{1, 17, 128, 256} {
		request := &pb.ScanRequest{Resource: "db/records", PageSize: size}
		work, failure := adapter.prepareScan(request)
		if failure != nil {
			t.Fatal(failure)
		}
		if work.ResultBytes != execution.ScanResultBytes || work.WorkingBytes != scanWorkingBytes {
			t.Fatal("Scan did not reserve its bounded batch", work.ResultBytes, work.WorkingBytes)
		}
		state := work.Backend.(*scanPlan)
		for _, emitted := range []uint64{0, uint64(size) - 1} {
			state.Count = emitted
			command := scanFindCommand(state)
			raw, err := bson.Marshal(command)
			if err != nil {
				t.Fatal(err)
			}
			want := min(uint64(size)-emitted, uint64(execution.ScanBatchDocuments))
			if bson.Raw(raw).Lookup("limit").Int64() != int64(want) || bson.Raw(raw).Lookup("batchSize").Int32() != int32(want) || !bson.Raw(raw).Lookup("singleBatch").Boolean() {
				t.Fatal("Scan did not request a bounded remaining batch", size, emitted, raw)
			}
		}
	}
}

func TestMongoScanCheckpointRequiresNativeIdentityOnly(t *testing.T) {
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	request := &pb.ScanRequest{Resource: "db/records", PageSize: 1}
	fingerprint := protocol.ScanFingerprint(request, "mongo", scanProfile)
	nested := bson.D{{Key: "ordered", Value: int32(1)}, {Key: "second", Value: int64(2)}}
	identity := bson.D{{Key: "_id", Value: nested}}
	last, err := bson.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"valid", "missing_identity", "internal_capacity", "extra", "duplicate", "invalid_identity", "wrong_profile"} {
		t.Run(mode, func(t *testing.T) {
			checkpoint := append(bson.D(nil), identity...)
			profile := scanProfile
			switch mode {
			case "missing_identity":
				checkpoint = bson.D{}
			case "internal_capacity":
				field := bson.E{Key: "batch_size", Value: int32(3)}
				checkpoint = append(checkpoint, field)
			case "extra":
				field := bson.E{Key: "unknown", Value: int32(1)}
				checkpoint = append(checkpoint, field)
			case "duplicate":
				checkpoint = append(checkpoint, checkpoint[0])
			case "invalid_identity":
				checkpoint[0].Value = bson.A{int32(1)}
			case "wrong_profile":
				profile = "mongodb:v2"
			}
			state, err := bson.Marshal(checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			token, err := protocol.EncodeScanToken(profile, fingerprint, state)
			if err != nil {
				t.Fatal(err)
			}
			request.ContinuationToken = token
			work, failure := adapter.prepareScan(request)
			if mode != "valid" {
				if failure.GetCode() != pb.FailureCode_INVALID_ARGUMENT {
					t.Fatal("invalid checkpoint was accepted before backend work", mode, failure)
				}
				return
			}
			if failure != nil {
				t.Fatal(failure)
			}
			native := work.Backend.(*scanPlan)
			command := scanFindCommand(native)
			raw, err := bson.Marshal(command)
			if err != nil {
				t.Fatal(err)
			}
			if native.batchSize != execution.ScanBatchDocuments || !bytes.Equal(native.last, last) || bson.Raw(raw).Lookup("batchSize").Int32() != 1 {
				t.Fatal("checkpoint changed BSON identity or inherited internal tuning", native.batchSize, native.last, command)
			}
		})
	}
}

func TestMongoScanPrefixValidatesDiscardedTail(t *testing.T) {
	for _, mode := range []string{"valid", "invalid_document", "invalid_index", "too_many", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			target := namespace{database: "db", collection: "records"}
			adapter := &Adapter{}
			first := bson.D{{Key: "_id", Value: int32(1)}, {Key: "pad", Value: strings.Repeat("x", 64)}}
			second := bson.D{{Key: "_id", Value: int32(2)}, {Key: "pad", Value: strings.Repeat("x", 64)}}
			firstRaw, err := bson.Marshal(first)
			if err != nil {
				t.Fatal(err)
			}
			documents := bson.A{first, second, first}
			if mode == "invalid_document" {
				documents[2] = int32(3)
			}
			if mode == "oversized" {
				document := bson.D{{Key: "_id", Value: int32(3)}, {Key: "pad", Value: strings.Repeat("x", protocol.MaxDocument)}}
				documents[2] = document
			}
			cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: target.String()}, {Key: "firstBatch", Value: documents}}
			envelope := bson.D{{Key: "ok", Value: 1.0}, {Key: "cursor", Value: cursor}}
			raw, err := bson.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "invalid_index" {
				batch := bson.Raw(raw).Lookup("cursor", "firstBatch").Array()
				elements, err := bson.Raw(batch).Elements()
				if err != nil {
					t.Fatal(err)
				}
				elements[2][1] = '9'
			}
			items := 3
			if mode == "too_many" {
				items = 2
			}
			state := &recordCursor{target: target, items: items, outputBytes: len(firstRaw)}
			page := adapter.recordCursorReply(raw, state, true)
			if mode == "valid" {
				if page.Failure != nil || !state.limited || len(page.Documents) != 1 || !bytes.Equal(page.Documents[0].Data, firstRaw) {
					t.Fatal("Scan did not retain exactly the accepted prefix", page, state.limited)
				}
				return
			}
			if page.Failure == nil || len(page.Documents) != 0 {
				t.Fatal("invalid discarded tail exposed a prefix", page)
			}
		})
	}
}

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
func TestMongoScanNativeFilterAndProjection(t *testing.T) {
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	filter := bson.D{{Key: "sort", Value: bson.D{{Key: "$exists", Value: true}}}, {Key: "name", Value: bson.Regex{Pattern: "^a"}}}
	raw, _ := bson.Marshal(filter)
	document := &pb.Document{ContentType: "application/bson", Data: raw}
	projection := &pb.Projection{Mode: pb.ProjectionMode_EXCLUDE, Fields: []string{"_id"}}
	request := &pb.ScanRequest{Resource: "db/records", Filter: document, Projection: projection}
	work, failure := adapter.prepareScan(request)
	if failure != nil {
		t.Fatal(failure)
	}
	native := work.Backend.(*scanPlan)
	encoded, _ := bson.Marshal(scanFindCommand(native))
	if !bytes.Equal(bson.Raw(encoded).Lookup("filter").Document(), raw) || native.includeID {
		t.Fatal("native condition changed or identity published")
	}
	projection.Fields = []string{"_id.n"}
	if _, failure := adapter.prepareScan(request); failure.GetCode() != pb.FailureCode_UNSUPPORTED {
		t.Fatal("partial identity projection", failure)
	}
	document.Data = make([]byte, protocol.MaxScanFilterBytes+1)
	if _, failure := adapter.prepareScan(request); failure == nil {
		t.Fatal("oversized filter accepted")
	}
}

func TestMongoScanRejectsInvalidIdentityTokens(t *testing.T) {
	config := Config{Store: "mongo"}
	adapter := &Adapter{config: config}
	request := &pb.ScanRequest{Resource: "db/records"}
	fingerprint := protocol.ScanFingerprint(request, "mongo", scanProfile)
	identities := []bson.D{
		{{Key: "other", Value: "id"}},
		{{Key: "_id", Value: bson.A{int32(1)}}},
		{{Key: "_id", Value: bson.Regex{Pattern: "id"}}},
	}
	for _, identity := range identities {
		raw, _ := bson.Marshal(identity)
		token, err := protocol.EncodeScanToken(scanProfile, fingerprint, raw)
		if err != nil {
			t.Fatal(err)
		}
		request.ContinuationToken = token
		if _, failure := adapter.prepareScan(request); failure == nil {
			t.Fatal("invalid continuation identity accepted", identity)
		}
	}
}
