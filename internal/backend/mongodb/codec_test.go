package mongodb

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/batchstream/weir/internal/value"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCodecRoundTrip(t *testing.T) {
	decimal, err := bson.ParseDecimal128("123456789.0123456789")
	if err != nil {
		t.Fatal(err)
	}
	oid := bson.NewObjectID()
	binary := bson.Binary{Subtype: 0x80, Data: []byte{0, 1, 255}}
	doc := bson.D{{Key: "_id", Value: oid}, {Key: "small", Value: int32(3)}, {Key: "wide", Value: int64(3)}, {Key: "big", Value: int64(9007199254740993)}, {Key: "decimal", Value: decimal}, {Key: "binary", Value: binary}, {Key: "null", Value: nil}, {Key: "float", Value: math.Float64frombits(0x7ff8000000000042)}}
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := Encode(t.Context(), decoded)
	if err != nil || !bytes.Equal(encoded, raw) {
		t.Fatalf("lossy round trip: %v", err)
	}
	missing, _ := decoded.Lookup("absent")
	null, _ := decoded.Lookup("null")
	if missing.Kind != value.Missing || null.Kind != value.Null {
		t.Fatal("missing/null")
	}
	edited := value.Field{Name: "edited", Value: value.Value{Kind: value.Bool, Boolean: true}}
	decoded.Fields = append(decoded.Fields, edited)
	encoded, err = Encode(t.Context(), decoded)
	if err != nil {
		t.Fatal(err)
	}
	if encoded.Lookup("small").Type != bson.TypeInt32 || encoded.Lookup("wide").Type != bson.TypeInt64 {
		t.Fatal("integer width changed")
	}
	fields, err := encoded.Elements()
	if err != nil || fields[1].Key() != "small" || fields[4].Key() != "decimal" {
		t.Fatal("field order")
	}
	duplicate := bson.D{{Key: "a", Value: 1}, {Key: "a", Value: 2}}
	raw, _ = bson.Marshal(duplicate)
	decoded, err = Decode(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decoded.Lookup("a"); err == nil {
		t.Fatal("ambiguous lookup")
	}
	encoded, err = Encode(t.Context(), decoded)
	if err != nil || !bytes.Equal(encoded, raw) {
		t.Fatal("duplicate round trip")
	}
}
func TestCodecNativeDepthAndMalformed(t *testing.T) {
	deep := bson.D{{Key: "v", Value: 1}}
	for i := 0; i < 101; i++ {
		deep = bson.D{{Key: "v", Value: deep}}
	}
	raw, _ := bson.Marshal(deep)
	if _, err := Decode(t.Context(), raw); err == nil {
		t.Fatal("deep")
	}
	wide := bson.D{}
	for i := 0; i < 5000; i++ {
		field := bson.E{Key: "x", Value: 1}
		wide = append(wide, field)
	}
	raw, _ = bson.Marshal(wide)
	if _, err := Decode(t.Context(), raw); err != nil {
		t.Fatal("node quota remained", err)
	}
	large := bson.D{{Key: "x", Value: strings.Repeat("x", 3<<20)}}
	raw, _ = bson.Marshal(large)
	if _, err := Decode(t.Context(), raw); err != nil {
		t.Fatal("document quota remained", err)
	}
	code := bson.D{{Key: "x", Value: bson.JavaScript("return 1")}}
	raw, _ = bson.Marshal(code)
	if _, err := Decode(t.Context(), raw); err == nil {
		t.Fatal("unsupported code")
	}
	for _, raw := range [][]byte{nil, {1, 0, 0, 0, 0}, {5, 0, 0, 0, 2}, {9, 0, 0, 0, 8, 'b', 0, 2, 0}, {12, 0, 0, 0, 2, 's', 0, 0, 0, 0, 0, 0}, {14, 0, 0, 0, 2, 's', 0, 2, 0, 0, 0, 'a', 255, 0}} {
		if _, err := Decode(t.Context(), raw); err == nil {
			t.Fatalf("accepted %x", raw)
		}
	}
}
func FuzzCodec(f *testing.F) {
	f.Add([]byte{5, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, raw []byte) {
		v, err := Decode(t.Context(), raw)
		if err == nil {
			encoded, e := Encode(t.Context(), v)
			if e != nil {
				t.Fatal(e)
			}
			if !bytes.Equal(encoded, raw) {
				t.Fatal("accepted BSON must round-trip byte-for-byte")
			}
		}
	})
}

func TestCodecCancellationAndLargeRoundTrip(t *testing.T) {
	document := bson.D{{Key: "text", Value: strings.Repeat("x", 5<<20)}}
	raw, err := bson.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(t.Context(), raw)
	if err != nil {
		t.Fatal("large BSON retained a conversion quota", err)
	}
	encoded, err := Encode(t.Context(), decoded)
	if err != nil || !bytes.Equal(encoded, raw) {
		t.Fatal("large BSON did not round-trip", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Decode(ctx, raw); !errors.Is(err, context.Canceled) {
		t.Fatal("BSON decode ignored caller cancellation", err)
	}
	if _, err := Encode(ctx, decoded); !errors.Is(err, context.Canceled) {
		t.Fatal("BSON encode ignored caller cancellation", err)
	}
}

func TestCodecSingleBufferNestedValuesMatchNativeBSON(t *testing.T) {
	decimal, err := bson.ParseDecimal128("1234567890.123456789012345")
	if err != nil {
		t.Fatal(err)
	}
	binaryValue := bson.Binary{Subtype: 0x80, Data: []byte{0, 128, 255}}
	regex := bson.Regex{Pattern: "a.*b", Options: "im"}
	timestamp := bson.Timestamp{T: 1234, I: 99}
	leaf := bson.D{
		{Key: "text", Value: strings.Repeat("nested payload", 4096)},
		{Key: "small", Value: int32(7)}, {Key: "wide", Value: int64(7)},
		{Key: "float", Value: math.Float64frombits(0x7ff8000000000042)},
		{Key: "decimal", Value: decimal}, {Key: "binary", Value: binaryValue},
		{Key: "regex", Value: regex}, {Key: "timestamp", Value: timestamp},
		{Key: "date", Value: bson.DateTime(-123456789)},
		{Key: "duplicate", Value: true}, {Key: "duplicate", Value: nil},
	}
	var nested any = leaf
	for depth := 0; depth < 98; depth++ {
		if depth%2 == 0 {
			items := bson.A{nested}
			nested = items
		} else {
			object := bson.D{{Key: "child", Value: nested}}
			nested = object
		}
	}
	document := bson.D{{Key: "root", Value: nested}}
	raw, err := bson.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := Encode(t.Context(), decoded)
	if err != nil || !bytes.Equal(raw, encoded) {
		t.Fatal("single-buffer encoding changed nested native BSON bytes", len(raw), len(encoded), err)
	}
}

func TestCodecEncodingPreservesNativeDepthBoundary(t *testing.T) {
	leaf := value.Value{Kind: value.Object}
	for depth := 0; depth < 100; depth++ {
		field := value.Field{Name: "child", Value: leaf}
		leaf = value.Value{Kind: value.Object, Fields: []value.Field{field}}
	}
	if _, err := Encode(t.Context(), leaf); err != nil {
		t.Fatal("native depth 100 was rejected", err)
	}
	field := value.Field{Name: "child", Value: leaf}
	tooDeep := value.Value{Kind: value.Object, Fields: []value.Field{field}}
	if _, err := Encode(t.Context(), tooDeep); err == nil {
		t.Fatal("native depth 101 was accepted")
	}
}
