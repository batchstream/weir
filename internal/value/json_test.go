package value

import (
	"bytes"
	"testing"
)

func TestJSONCodecPreservesOrderAndIntegerWidth(t *testing.T) {
	raw := []byte(`{"n32":2147483647,"n64":2147483648,"large":9223372036854775807,"decimal":9007199254740993.125e-3,"beyond":9223372036854775808,"negative_zero":-0,"array":[null,true,1.25]}`)
	decoded, err := DecodeJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Fields[0].Name != "n32" || decoded.Fields[0].Value.Kind != Int32 || decoded.Fields[1].Value.Kind != Int64 || decoded.Fields[2].Value.Integer != 9223372036854775807 || !IsJSONNumber(decoded.Fields[3].Value) || !IsJSONNumber(decoded.Fields[4].Value) || !IsJSONNumber(decoded.Fields[5].Value) || decoded.Fields[6].Value.Kind != Array {
		t.Fatalf("unexpected decoded JSON: %#v", decoded)
	}
	encoded, err := EncodeJSON(decoded)
	if err != nil {
		t.Fatal(err)
	}
	redecoded, err := DecodeJSON(encoded)
	if err != nil || len(redecoded.Fields) != len(decoded.Fields) || redecodeMismatch(redecoded, decoded) {
		t.Fatalf("round trip mismatch: %s %v", encoded, err)
	}
	for _, number := range [][]byte{[]byte(`9223372036854775807`), []byte(`9007199254740993.125e-3`), []byte(`9223372036854775808`), []byte(`-0`)} {
		if !bytes.Contains(encoded, number) {
			t.Fatalf("JSON number was not emitted exactly: wanted %s in %s", number, encoded)
		}
	}
}

func TestJSONCodecRejectsAmbiguousOrUnrepresentableInput(t *testing.T) {
	for _, raw := range []string{`{"a":1,"a":2}`, `[]`, `null`, `{"n":01}`, `{"n":1e}`, `{} {}`} {
		if _, err := DecodeJSON([]byte(raw)); err == nil {
			t.Fatalf("accepted invalid JSON source %s", raw)
		}
	}
}

func redecodeMismatch(got, want Value) bool {
	if len(got.Fields) != len(want.Fields) {
		return true
	}
	for i := range got.Fields {
		if got.Fields[i].Name != want.Fields[i].Name || got.Fields[i].Value.Kind != want.Fields[i].Value.Kind {
			return true
		}
	}
	return false
}
