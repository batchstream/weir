package search

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

// Keep a token decoder as an independent oracle for the syntax
// validator. It also measures parsing cost on the same response bodies.
func validateJSONTokens(raw []byte) error {
	if !utf8.Valid(raw) || !json.Valid(raw) {
		return errJSON
	}
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
	if err := walkJSONTokens(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errJSON
	}
	return nil
}

func walkJSONTokens(decoder *json.Decoder) error {
	token, err := decoder.Token()
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
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return errJSON
			}
			name, ok := key.(string)
			if !ok || keys[name] {
				return errJSON
			}
			keys[name] = true
			if err := walkJSONTokens(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := walkJSONTokens(decoder); err != nil {
				return err
			}
		}
	default:
		return errJSON
	}
	close, err := decoder.Token()
	if err != nil || delimiter == '{' && close != json.Delim('}') || delimiter == '[' && close != json.Delim(']') {
		return errJSON
	}
	return nil
}

func FuzzJSONValidationMatchesTokenDecoder(f *testing.F) {
	for _, raw := range []string{
		`null`, `1e400`, `-0.01e+30`, `{"a":[true,null,{},[]]}`,
		`{"a":1,"\u0061":2}`, `{"\ud83d\ude00":1,"😀":2}`,
		`{"unicode":"\ud800"}`, `{"literal":"\\ud800"}`,
		`{"path":"\\\"","other":"\u0000"}`, `{"a":1} {}`,
		strings.Repeat("[", 32) + "0" + strings.Repeat("]", 32),
		strings.Repeat("[", 33) + "0" + strings.Repeat("]", 33),
		`{"bad":"` + string([]byte{255}) + `"}`, testIndexReply,
	} {
		f.Add([]byte(raw))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		got := validateJSON(context.Background(), raw) == nil
		want := validateJSONTokens(raw) == nil
		if got != want {
			t.Fatalf("validation mismatch: got %v, want %v, body %q", got, want, raw)
		}
	})
}

func BenchmarkJSONValidation(b *testing.B) {
	source := `{"name":"record","flags":[true,false,null],"payload":"` + strings.Repeat("x", 960) + `"}`
	readItem := `{"_index":"records","_id":"id","found":true,"_seq_no":1,"_primary_term":1,"_source":` + source + `}`
	bulkItem := `{"index":{"_index":"records","_id":"id","status":201,"result":"created","_version":1,"_seq_no":1,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}`
	bodies := map[string][]byte{
		"source_1KiB":   []byte(source),
		"mget_32":       []byte(`{"docs":[` + strings.TrimSuffix(strings.Repeat(readItem+",", 32), ",") + `]}`),
		"bulk_32":       []byte(`{"errors":false,"took":1,"items":[` + strings.TrimSuffix(strings.Repeat(bulkItem+",", 32), ",") + `]}`),
		"qualification": []byte(testIndexReply),
	}
	for name, raw := range bodies {
		b.Run(name, func(b *testing.B) {
			b.Run("syntax_scan", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				for b.Loop() {
					if validateJSON(context.Background(), raw) != nil {
						b.Fatal("invalid benchmark body")
					}
				}
			})
			b.Run("token_oracle", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				for b.Loop() {
					if validateJSONTokens(raw) != nil {
						b.Fatal("invalid benchmark body")
					}
				}
			})
		})
	}
}
