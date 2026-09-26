package mongostore

import (
	"bytes"
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
	decoded, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := Encode(decoded)
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
	encoded, err = Encode(decoded)
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
	decoded, err = Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decoded.Lookup("a"); err == nil {
		t.Fatal("ambiguous lookup")
	}
	encoded, err = Encode(decoded)
	if err != nil || !bytes.Equal(encoded, raw) {
		t.Fatal("duplicate round trip")
	}
}
func TestCodecLimitsAndMalformed(t *testing.T) {
	deep := bson.D{{Key: "v", Value: 1}}
	for i := 0; i < 40; i++ {
		deep = bson.D{{Key: "v", Value: deep}}
	}
	raw, _ := bson.Marshal(deep)
	if _, err := Decode(raw); err == nil {
		t.Fatal("deep")
	}
	wide := bson.D{}
	for i := 0; i < 5000; i++ {
		field := bson.E{Key: "x", Value: 1}
		wide = append(wide, field)
	}
	raw, _ = bson.Marshal(wide)
	if _, err := Decode(raw); err == nil {
		t.Fatal("node count")
	}
	large := bson.D{{Key: "x", Value: strings.Repeat("x", 300<<10)}}
	raw, _ = bson.Marshal(large)
	if _, err := Decode(raw); err == nil {
		t.Fatal("large")
	}
	code := bson.D{{Key: "x", Value: bson.JavaScript("return 1")}}
	raw, _ = bson.Marshal(code)
	if _, err := Decode(raw); err == nil {
		t.Fatal("unsupported code")
	}
	for _, raw := range [][]byte{nil, {1, 0, 0, 0, 0}, {5, 0, 0, 0, 2}, {9, 0, 0, 0, 8, 'b', 0, 2, 0}, {12, 0, 0, 0, 2, 's', 0, 0, 0, 0, 0, 0}, {14, 0, 0, 0, 2, 's', 0, 2, 0, 0, 0, 'a', 255, 0}} {
		if _, err := Decode(raw); err == nil {
			t.Fatalf("accepted %x", raw)
		}
	}
}
func FuzzCodec(f *testing.F) {
	f.Add([]byte{5, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, raw []byte) {
		v, err := Decode(raw)
		if err == nil {
			encoded, e := Encode(v)
			if e != nil {
				t.Fatal(e)
			}
			if !bytes.Equal(encoded, raw) {
				t.Fatal("accepted BSON must round-trip byte-for-byte")
			}
		}
	})
}
