package main

import (
	"bytes"
	"testing"
)

func TestMongoPayloadMatchesResourceAndSize(t *testing.T) {
	for _, id := range []string{"read-0001", "r000-000001", "prewarm-10-weir-000001"} {
		raw := mongoPayload(id)
		if len(raw) != 1024 || raw.Validate() != nil {
			t.Fatal(id, len(raw), raw.Validate())
		}
		elements, err := raw.Elements()
		if err != nil || len(elements) != 2 || elements[0].Key() != "_id" || elements[0].Value().StringValue() != id {
			t.Fatal(id, elements, err)
		}
		if !bytes.Equal(raw, mongoPayload(id)) || bytes.Equal(raw, mongoPayload(id+"x")) {
			t.Fatal("payload determinism or identity")
		}
	}
}
