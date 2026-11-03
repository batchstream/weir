package search

import (
	"bytes"
	"encoding/json/jsontext"
)

// validJSON accepts RFC 7493 JSON: valid UTF-8 and escapes, and unique object
// names.
func validJSON(raw []byte) bool {
	return jsontext.Value(raw).IsValid()
}

func object(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 1 && trimmed[0] == '{'
}
