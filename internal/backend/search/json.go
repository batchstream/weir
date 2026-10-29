package search

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"
)

var errJSON = errors.New("invalid JSON")

const smallJSONBytes = 64 << 10

// The standard library validates syntax without materializing tokens. The
// walk then checks duplicate keys, decoding only
// escaped object keys. Scalar strings and numbers retain their wire spelling.
func validateJSON(ctx context.Context, raw []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Records and replies are usually small. One RFC 7493 pass checks syntax,
	// UTF-8, surrogate escapes and unique names without allocating; larger
	// values take the chunked path below, which observes cancellation.
	if len(raw) <= smallJSONBytes {
		if !jsontext.Value(raw).IsValid() {
			return errJSON
		}
		return nil
	}
	// encoding/json accepts unpaired surrogate escapes by replacing them. Reject
	// them before either the backend or a decoder can silently change a string.
	nextCheck := 0
	for i := 0; i < len(raw); i++ {
		if i >= nextCheck {
			nextCheck = i + 4096
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if raw[i] >= utf8.RuneSelf {
			_, width := utf8.DecodeRune(raw[i:])
			if width == 1 {
				return errJSON
			}
			i += width - 1
			continue
		}
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
	reader := jsonContextReader{ctx: ctx, raw: bytes.NewReader(raw)}
	decoder := json.NewDecoder(reader)
	var checked json.RawMessage
	if err := decoder.Decode(&checked); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errJSON
	}
	if err := decoder.Decode(&checked); err != io.EOF {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errJSON
	}
	walk := jsonWalk{ctx: ctx, raw: raw}
	if !walk.value() {
		if walk.err != nil {
			return walk.err
		}
		return errJSON
	}
	return ctx.Err()
}

// A read chunk lets the standard JSON syntax/depth parser observe cancellation
// while processing one long scalar. It does not bound the complete document.
type jsonContextReader struct {
	ctx context.Context
	raw *bytes.Reader
}

func (r jsonContextReader) Read(data []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.raw.Read(data[:min(len(data), 16<<10)])
}

type jsonWalk struct {
	ctx       context.Context
	err       error
	nextCheck int
	raw       []byte
	position  int
}

// Syntax has already been checked by the standard decoder. This walk cannot encounter a
// missing delimiter or unterminated string and never converts numeric values.
func (w *jsonWalk) value() bool {
	w.space()
	if !w.check() {
		return false
	}
	switch w.raw[w.position] {
	case '{':
		w.position++
		w.space()
		keys := make(map[string]struct{})
		for w.raw[w.position] != '}' {
			start := w.position
			escaped := w.string()
			if w.err != nil {
				return false
			}
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
			if !w.value() {
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
			if !w.value() {
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
			if !w.check() {
				return false
			}
			switch w.raw[w.position] {
			case ',', '}', ']', ' ', '\t', '\n', '\r':
				return true
			default:
				w.position++
			}
		}
	}
	return w.err == nil
}

func (w *jsonWalk) string() bool {
	w.position++
	escaped := false
	for w.raw[w.position] != '"' {
		if !w.check() {
			return false
		}
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
		if !w.check() {
			return
		}
		switch w.raw[w.position] {
		case ' ', '\t', '\n', '\r':
			w.position++
		default:
			return
		}
	}
}
func (w *jsonWalk) check() bool {
	if w.err != nil {
		return false
	}
	if w.position >= w.nextCheck {
		w.err = w.ctx.Err()
		w.nextCheck = w.position + 4096
	}
	return w.err == nil
}

func object(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 1 && trimmed[0] == '{'
}
