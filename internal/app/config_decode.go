package app

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/batchstream/weir/api/protocol"
	"go.yaml.in/yaml/v3"
)

const maxConfigBytes = 128 << 10

// DecodeBasic reads process settings without opening the routing document.
func DecodeBasic(input io.Reader) (BasicConfig, error) {
	cfg := DefaultConfig().Basic
	if err := decodeConfigYAML(input, &cfg, false); err != nil {
		return cfg, fmt.Errorf("basic %w", err)
	}
	return cfg, cfg.Validate()
}

// DecodeRouting validates resolved, inline configuration without performing IO.
func DecodeRouting(input io.Reader) (RoutingConfig, error) {
	cfg := RoutingConfig{}
	if err := decodeConfigYAML(input, &cfg, true); err != nil {
		return cfg, fmt.Errorf("routing %w", err)
	}
	return cfg, cfg.Validate()
}

// Load resolves every configuration file and validates local Store definitions before startup.
// Basic and routing paths are interpreted from the process working directory.
// Relative credential paths are resolved from the routing document's directory.
// An empty routing path selects a node without local Stores.
func Load(basicFilename, routingFilename string) (Config, error) {
	cfg := Config{}
	file, err := os.Open(basicFilename)
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
	if basic.Discovery.PeerAddressEnv != "" {
		value, exists := os.LookupEnv(basic.Discovery.PeerAddressEnv)
		if !exists {
			return cfg, errors.New("advertised peer address environment value unavailable")
		}
		canonical, err := protocol.CanonicalEndpoint(value)
		if err != nil {
			return cfg, errors.New("invalid advertised peer address environment value")
		}
		basic.Discovery.PeerAddress, basic.Discovery.PeerAddressEnv = canonical, ""
	}
	if routingFilename == "" {
		cfg.Basic = basic
		return cfg, cfg.Validate()
	}
	file, err = os.Open(routingFilename)
	if err != nil {
		return cfg, errors.New("routing configuration unavailable")
	}
	routing := RoutingConfig{}
	decodeErr = decodeConfigYAML(file, &routing, true)
	closeErr = file.Close()
	if decodeErr != nil {
		return cfg, fmt.Errorf("routing %w", decodeErr)
	}
	if closeErr != nil {
		return cfg, errors.New("routing configuration unavailable")
	}
	if err := routing.resolveCredentials(filepath.Dir(routingFilename)); err != nil {
		return cfg, err
	}
	cfg.Basic, cfg.Routing = basic, routing
	return cfg, cfg.Validate()
}

func decodeConfigYAML(input io.Reader, target any, allowNull bool) error {
	raw, err := io.ReadAll(io.LimitReader(input, maxConfigBytes+1))
	if err != nil {
		return errors.New("configuration unavailable")
	}
	if len(raw) > maxConfigBytes {
		return errors.New("configuration exceeds bound")
	}

	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return errors.New("invalid configuration YAML")
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return errors.New("configuration must be a mapping")
	}
	err = validateConfigYAML(document.Content[0], 0, allowNull, "")
	if err != nil {
		return err
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("trailing configuration document or data")
	}

	// Node.Decode does not support KnownFields. Decode the checked document
	// into its defaults with strict field matching, using the same YAML parser.
	decoder = yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(target); err != nil {
		// Parser errors may contain field names, input values or sensitive paths.
		return errors.New("invalid configuration YAML or unknown field")
	}
	return nil
}

func validateConfigYAML(node *yaml.Node, depth int, allowNull bool, field string) error {
	if depth > 12 {
		return errors.New("configuration nesting limit")
	}
	if node.Anchor != "" || node.Kind == yaml.AliasNode {
		return errors.New("configuration anchors and aliases are unsupported")
	}
	if node.Style&yaml.TaggedStyle != 0 {
		return errors.New("configuration explicit tags are unsupported")
	}
	if node.Tag == "!!null" {
		if allowNull {
			switch field {
			case "mongodb", "search", "connection", "stores", "seeds", "advertise":
				return nil
			}
		}
		return errors.New("configuration field cannot be null")
	}

	switch node.Kind {
	case yaml.MappingNode:
		seen := make(map[string]bool)
		for i := 0; i < len(node.Content); i += 2 {
			key := node.Content[i]
			if key.Kind != yaml.ScalarNode ||
				key.Tag != "!!str" ||
				key.Anchor != "" ||
				key.Style&yaml.TaggedStyle != 0 ||
				key.Value == "<<" {
				return errors.New("configuration requires plain string fields without merge keys")
			}
			keyName := strings.Map(func(char rune) rune {
				for {
					next := unicode.SimpleFold(char)
					if next <= char {
						return next
					}
					char = next
				}
			}, key.Value)
			if seen[keyName] {
				return errors.New("duplicate configuration field")
			}
			seen[keyName] = true
			err := validateConfigYAML(node.Content[i+1], depth+1, allowNull, key.Value)
			if err != nil {
				return err
			}
		}
	case yaml.SequenceNode:
		for _, child := range node.Content {
			if child.Tag == "!!null" {
				return errors.New("configuration sequence item cannot be null")
			}
			err := validateConfigYAML(child, depth+1, allowNull, field)
			if err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		expectedTag := "!!str"
		switch field {
		case "max_connections", "max_sessions", "max_concurrency", "max_batch_operations":
			expectedTag = "!!int"
		case "allow_intranet":
			expectedTag = "!!bool"
		}
		if node.Tag != expectedTag {
			return errors.New("invalid configuration scalar type")
		}
	default:
		return errors.New("invalid configuration structure")
	}
	return nil
}
