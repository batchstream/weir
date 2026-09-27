package main

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestDocumentExactAndDistinct(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("read-%04d", i)
		p := payload(id)
		if len(p) != 1024 || !json.Valid(p) || seen[string(p)] || !validPayload(p, id) {
			t.Fatal(i)
		}
		seen[string(p)] = true
	}
}
