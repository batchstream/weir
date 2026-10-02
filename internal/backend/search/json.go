package search

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"unicode/utf8"
)

var errJSON = errors.New("invalid or excessive JSON")

// The standard library validates syntax without materializing tokens. The
// bounded walk then checks duplicate keys, depth and value count, decoding only
// escaped object keys. Scalar strings and numbers retain their wire spelling.
func validateJSON(raw []byte, nodes int) error {
	if !utf8.Valid(raw) {
		return errJSON
	}
	// encoding/json accepts unpaired surrogate escapes by replacing them. Reject
	// them before either the backend or a decoder can silently change a string.
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return errJSON
		}
		if raw[i] != 'u' {
			continue
		}
		if i+5 > len(raw) {
			return errJSON
		}
		code, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			return errJSON
		}
		i += 4
		if code >= 0xdc00 && code <= 0xdfff {
			return errJSON
		}
		if code >= 0xd800 && code <= 0xdbff {
			if i+7 > len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return errJSON
			}
			low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return errJSON
			}
			i += 6
		}
	}
	if !json.Valid(raw) {
		return errJSON
	}
	walk := jsonWalk{raw: raw, remaining: nodes}
	if !walk.value(0) {
		return errJSON
	}
	return nil
}

type jsonWalk struct {
	raw       []byte
	position  int
	remaining int
}

// Syntax has already been checked by json.Valid. This walk cannot encounter a
// missing delimiter or unterminated string and never converts numeric values.
func (w *jsonWalk) value(depth int) bool {
	w.remaining--
	if depth > 32 || w.remaining < 0 {
		return false
	}
	w.space()
	switch w.raw[w.position] {
	case '{':
		w.position++
		w.space()
		keys := make(map[string]struct{})
		for w.raw[w.position] != '}' {
			start := w.position
			escaped := w.string()
			name := string(w.raw[start+1 : w.position-1])
			if escaped {
				var decoded string
				if json.Unmarshal(w.raw[start:w.position], &decoded) != nil {
					return false
				}
				name = decoded
			}
			if _, exists := keys[name]; exists {
				return false
			}
			keys[name] = struct{}{}
			w.space()
			w.position++ // colon
			if !w.value(depth + 1) {
				return false
			}
			w.space()
			if w.raw[w.position] == ',' {
				w.position++
				w.space()
			}
		}
		w.position++
	case '[':
		w.position++
		w.space()
		for w.raw[w.position] != ']' {
			if !w.value(depth + 1) {
				return false
			}
			w.space()
			if w.raw[w.position] == ',' {
				w.position++
				w.space()
			}
		}
		w.position++
	case '"':
		w.string()
	default:
		for w.position < len(w.raw) {
			switch w.raw[w.position] {
			case ',', '}', ']', ' ', '\t', '\n', '\r':
				return true
			default:
				w.position++
			}
		}
	}
	return true
}

func (w *jsonWalk) string() bool {
	w.position++
	escaped := false
	for w.raw[w.position] != '"' {
		if w.raw[w.position] == '\\' {
			escaped = true
			w.position++
		}
		w.position++
	}
	w.position++
	return escaped
}

func (w *jsonWalk) space() {
	for w.position < len(w.raw) {
		switch w.raw[w.position] {
		case ' ', '\t', '\n', '\r':
			w.position++
		default:
			return
		}
	}
}
func object(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 1 && trimmed[0] == '{'
}
