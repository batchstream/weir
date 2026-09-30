package app

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

const maxConfigBytes = 128 << 10

// DecodeBasic reads process settings without opening the routing document.
func DecodeBasic(input io.Reader) (BasicConfig, error) {
	cfg := DefaultConfig().Basic
	if err := decodeConfigJSON(input, &cfg, false); err != nil {
		return cfg, fmt.Errorf("basic %w", err)
	}
	if strings.TrimSpace(cfg.Routing.File) == "" {
		return cfg, errors.New("basic configuration requires routing.file")
	}
	return cfg, cfg.Validate()
}

// DecodeRouting validates the service graph without accessing any backend.
func DecodeRouting(input io.Reader) (RoutingConfig, error) {
	cfg := RoutingConfig{}
	if err := decodeConfigJSON(input, &cfg, true); err != nil {
		return cfg, fmt.Errorf("routing %w", err)
	}
	return cfg, cfg.Validate()
}

// Load reads both documents and validates the full configuration before startup.
// Relative routing paths are resolved from the basic document's directory.
func Load(filename string) (Config, error) {
	cfg := Config{}
	file, err := os.Open(filename)
	if err != nil {
		return cfg, errors.New("basic configuration unavailable")
	}
	basic, decodeErr := DecodeBasic(file)
	closeErr := file.Close()
	if decodeErr != nil {
		return cfg, decodeErr
	}
	if closeErr != nil {
		return cfg, errors.New("basic configuration unavailable")
	}
	routingFilename := basic.Routing.File
	if !filepath.IsAbs(routingFilename) {
		routingFilename = filepath.Join(filepath.Dir(filename), routingFilename)
	}
	file, err = os.Open(routingFilename)
	if err != nil {
		return cfg, errors.New("routing configuration unavailable")
	}
	routing, decodeErr := DecodeRouting(file)
	closeErr = file.Close()
	if decodeErr != nil {
		return cfg, decodeErr
	}
	if closeErr != nil {
		return cfg, errors.New("routing configuration unavailable")
	}
	cfg.Basic, cfg.Routing = basic, routing
	return cfg, cfg.Validate()
}

func decodeConfigJSON(input io.Reader, target any, allowNull bool) error {
	raw, err := io.ReadAll(io.LimitReader(input, maxConfigBytes+1))
	if err != nil {
		return errors.New("configuration unavailable")
	}
	if len(raw) > maxConfigBytes {
		return errors.New("configuration exceeds bound")
	}
	// encoding/json accepts duplicate keys and case-insensitive field matches.
	// Check the complete document before decoding so neither can overwrite data.
	tokens := json.NewDecoder(bytes.NewReader(raw))
	if err := uniqueJSON(tokens, 0, allowNull); err != nil {
		return err
	}
	if _, err := tokens.Token(); err != io.EOF {
		return errors.New("trailing configuration data")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		// Decoder errors can include unknown field names or invalid input values.
		return errors.New("invalid configuration JSON or unknown field")
	}
	return nil
}

func uniqueJSON(d *json.Decoder, depth int, allowNull bool) error {
	if depth > 12 {
		return errors.New("configuration nesting limit")
	}
	token, err := d.Token()
	if err != nil {
		return errors.New("invalid configuration JSON")
	}
	delimiter, delimited := token.(json.Delim)
	if depth == 0 && (!delimited || delimiter != '{') {
		return errors.New("configuration must be an object")
	}
	if !delimited {
		if token == nil && !allowNull {
			return errors.New("configuration field cannot be null")
		}
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]bool)
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return errors.New("invalid configuration JSON")
			}
			text, ok := key.(string)
			keyName := strings.Map(func(char rune) rune {
				// Use the same Unicode folding as encoding/json field lookup.
				for {
					next := unicode.SimpleFold(char)
					if next <= char {
						return next
					}
					char = next
				}
			}, text)
			if !ok || seen[keyName] {
				return errors.New("duplicate configuration field")
			}
			seen[keyName] = true
			if err := uniqueJSON(d, depth+1, allowNull); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := uniqueJSON(d, depth+1, allowNull); err != nil {
				return err
			}
		}
	default:
		return errors.New("invalid configuration structure")
	}
	if _, err := d.Token(); err != nil {
		return errors.New("invalid configuration JSON")
	}
	return nil
}
