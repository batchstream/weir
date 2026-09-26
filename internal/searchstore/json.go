package searchstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"
)

var errJSON = errors.New("invalid or excessive JSON")

// Validate structure before materializing a response. Token strings, including
// ignored native errors, cannot exceed the already capped wire body.
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
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	remaining := nodes
	if err := walkJSON(decoder, 0, &remaining); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errJSON
	}
	return nil
}
func walkJSON(d *json.Decoder, depth int, remaining *int) error {
	*remaining -= 1
	if depth > 32 || *remaining < 0 {
		return errJSON
	}
	token, err := d.Token()
	if err != nil {
		return errJSON
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		keys := make(map[string]bool)
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return errJSON
			}
			name, ok := key.(string)
			if !ok || keys[name] {
				return errJSON
			}
			keys[name] = true
			if err := walkJSON(d, depth+1, remaining); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := walkJSON(d, depth+1, remaining); err != nil {
				return err
			}
		}
	default:
		return errJSON
	}
	close, err := d.Token()
	if err != nil || delimiter == '{' && close != json.Delim('}') || delimiter == '[' && close != json.Delim(']') {
		return errJSON
	}
	return nil
}
func object(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 1 && trimmed[0] == '{'
}
