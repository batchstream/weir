// Package luaworker defines the bounded worker protocol and client runner.
package luaworker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/batchstream/weir/internal/value"
)

const (
	MaxSourceBytes   = 16 << 10
	MaxRequestBytes  = 512 << 10
	MaxResponseBytes = 384 << 10
	MaxMessageBytes  = 1024
	MaxTimeout       = time.Second
	DefaultTimeout   = 500 * time.Millisecond
	maxConcurrent    = 4
	maxJSONDepth     = 4*value.MaxDepth + 16
)

type Action string

const (
	Keep    Action = "keep"
	Replace Action = "replace"
	Delete  Action = "delete"
	Reject  Action = "reject"
)

type Program struct {
	Source  string      `json:"source"`
	Current value.Value `json:"current"`
	Input   value.Value `json:"input"`
}

type Result struct {
	Action  Action      `json:"action"`
	Value   value.Value `json:"value"`
	Message string      `json:"message"`
}

type Request struct {
	Program       Program `json:"program"`
	TimeoutMillis int64   `json:"timeout_millis"`
}

type Response struct {
	Result Result `json:"result"`
	Error  string `json:"error"`
}

func ValidateProgram(program Program) error {
	if len(program.Source) == 0 || len(program.Source) > MaxSourceBytes || !utf8.ValidString(program.Source) || strings.IndexByte(program.Source, 0) >= 0 || strings.HasPrefix(program.Source, "\x1bLua") {
		return fmt.Errorf("invalid or oversized Lua source")
	}
	if err := value.Validate(program.Current); err != nil {
		return fmt.Errorf("invalid current value: %w", err)
	}
	if err := value.Validate(program.Input); err != nil {
		return fmt.Errorf("invalid input value: %w", err)
	}
	return nil
}

func ValidateResult(result Result) error {
	switch result.Action {
	case Keep, Delete:
		if result.Value.Kind != value.Missing || result.Message != "" {
			return fmt.Errorf("unexpected action payload")
		}
	case Replace:
		if result.Value.Kind != value.Object || result.Message != "" {
			return fmt.Errorf("replacement must be an object")
		}
	case Reject:
		if result.Value.Kind != value.Missing || len(result.Message) > MaxMessageBytes || !utf8.ValidString(result.Message) {
			return fmt.Errorf("invalid rejection")
		}
	default:
		return fmt.Errorf("invalid action")
	}
	if err := value.Validate(result.Value); err != nil {
		return err
	}
	if result.Action == Replace {
		return validateDocumentValue(result.Value, 0)
	}
	return nil
}

func validateDocumentValue(v value.Value, depth int) error {
	if depth > value.MaxDepth || v.Kind == value.Missing {
		return fmt.Errorf("replacement contains missing value or exceeds depth")
	}
	for _, field := range v.Fields {
		if err := validateDocumentValue(field.Value, depth+1); err != nil {
			return err
		}
	}
	for _, item := range v.Items {
		if err := validateDocumentValue(item, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func DecodeEnvelope(raw []byte, target any) error {
	if len(raw) == 0 || jsonDepth(raw) > maxJSONDepth {
		return fmt.Errorf("invalid worker envelope")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid worker envelope: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("trailing worker data")
	}
	return nil
}

func jsonDepth(raw []byte) int {
	depth, largest := 0, 0
	inString, escaped := false, false
	for _, ch := range raw {
		if inString {
			if escaped {
				escaped = false
			} else if ch == '\\' {
				escaped = true
			} else if ch == '"' {
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
		case '{', '[':
			depth++
			if depth > largest {
				largest = depth
			}
		case '}', ']':
			depth--
			if depth < 0 {
				return maxJSONDepth + 1
			}
		}
	}
	if inString || depth != 0 {
		return maxJSONDepth + 1
	}
	return largest
}
