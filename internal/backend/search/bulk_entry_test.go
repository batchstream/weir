package search

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestBulkEntryMatchesEncodedMetadataAndCompactSource(t *testing.T) {
	found := true
	seq, term := int64(41), int64(3)
	current := &getReply{Found: &found, Seq: &seq, Term: &term}
	source := []byte("{\"a\": 1,\n \"b\": [true]}")
	spaced := []byte(` { "a" :	"x\u0026<y>",  "b": [ 1 , 2 ] } `)
	cases := []struct {
		native  *plan
		current *getReply
	}{
		{native: &plan{index: "records", id: "a", action: "create", source: source}},
		{native: &plan{index: "records", id: "b", action: "replace", source: source}, current: current},
		{native: &plan{index: "records", id: "c", action: "expression", source: []byte(`{"script":{"source":"ctx._source.n++"}}`)}, current: current},
		{native: &plan{index: "records", id: "d\"\\é", action: "delete"}, current: current},
		{native: &plan{index: "a&b", id: "<x>\u2028", action: "put", source: spaced}},
	}
	for _, c := range cases {
		want := referenceBulkEntry(t, c.native, c.current)
		got := appendBulkEntry(nil, c.native, c.current)
		if !bytes.Equal(got, want) {
			t.Fatalf("bulk entry differs\n got %q\nwant %q", got, want)
		}
	}
}

// referenceBulkEntry frames an entry through maps, reflection and
// json.Compact as an independent oracle.
func referenceBulkEntry(t *testing.T, native *plan, current *getReply) []byte {
	t.Helper()
	metadata := map[string]any{"_index": native.index, "_id": native.id}
	action := native.bulkAction()
	switch {
	case native.action == "expression":
		metadata["retry_on_conflict"] = 0
	case action != "delete":
		metadata["pipeline"] = "_none"
	}
	if current != nil && *current.Found {
		metadata["if_seq_no"] = *current.Seq
		metadata["if_primary_term"] = *current.Term
	}
	header := map[string]any{action: metadata}
	encoded, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	var entry bytes.Buffer
	entry.Write(encoded)
	entry.WriteByte('\n')
	if native.source != nil {
		if err := json.Compact(&entry, native.source); err != nil {
			t.Fatal(err)
		}
		entry.WriteByte('\n')
	}
	return entry.Bytes()
}
