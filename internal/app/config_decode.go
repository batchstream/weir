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

	"github.com/batchstream/weir-protocol/api/protocol"
	"go.yaml.in/yaml/v3"
)

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
	basic := DefaultConfig().Basic
	if err := loadConfigYAML(basicFilename, &basic, false); err != nil {
		return cfg, fmt.Errorf("basic %w", err)
	}
	if err := basic.Validate(); err != nil {
		return cfg, err
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
	routing := RoutingConfig{}
	if routingFilename != "" {
		if err := loadConfigYAML(routingFilename, &routing, true); err != nil {
			return cfg, fmt.Errorf("routing %w", err)
		}
		if err := routing.resolveCredentials(filepath.Dir(routingFilename)); err != nil {
			return cfg, err
		}
	}
	cfg.Basic, cfg.Routing = basic, routing
	return cfg, cfg.Validate()
}

func loadConfigYAML(filename string, target any, allowNull bool) error {
	file, err := os.Open(filename)
	if err != nil {
		return errors.New("configuration unavailable")
	}
	decodeErr := decodeConfigYAML(file, target, allowNull)
	closeErr := file.Close()
	if decodeErr != nil {
		return decodeErr
	}
	if closeErr != nil {
		return errors.New("configuration unavailable")
	}
	return nil
}

func decodeConfigYAML(input io.Reader, target any, allowNull bool) error {
	raw, err := io.ReadAll(input)
	if err != nil {
		return errors.New("configuration unavailable")
	}

	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return errors.New("invalid configuration YAML")
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return errors.New("configuration must be a mapping")
	}
	err = validateConfigYAML(document.Content[0], allowNull, "")
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

type configYAMLFrame struct {
	node  *yaml.Node
	field string
	exit  bool
}

func validateConfigYAML(node *yaml.Node, allowNull bool, field string) error {
	root := configYAMLFrame{node: node, field: field}
	stack := []configYAMLFrame{root}
	active := make(map[*yaml.Node]bool)
	for len(stack) != 0 {
		frame := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		node := frame.node
		if frame.exit {
			delete(active, node)
			continue
		}
		if node == nil || active[node] {
			return errors.New("invalid or cyclic configuration structure")
		}
		active[node] = true
		exit := configYAMLFrame{node: node, exit: true}
		stack = append(stack, exit)
		if node.Anchor != "" || node.Kind == yaml.AliasNode {
			return errors.New("configuration anchors and aliases are unsupported")
		}
		if node.Style&yaml.TaggedStyle != 0 {
			return errors.New("configuration explicit tags are unsupported")
		}
		if node.Tag == "!!null" {
			if allowNull {
				switch frame.field {
				case "mongodb", "search", "authentication", "tls", "stores", "seeds", "advertise":
					continue
				}
			}
			return errors.New("configuration field cannot be null")
		}
		switch node.Kind {
		case yaml.MappingNode:
			if len(node.Content)%2 != 0 {
				return errors.New("invalid configuration structure")
			}
			seen := make(map[string]bool)
			for i := 0; i < len(node.Content); i += 2 {
				key := node.Content[i]
				if key == nil || key.Kind != yaml.ScalarNode || key.Tag != "!!str" || key.Anchor != "" || key.Style&yaml.TaggedStyle != 0 || key.Value == "<<" {
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
			}
			for i := len(node.Content) - 2; i >= 0; i -= 2 {
				child := configYAMLFrame{node: node.Content[i+1], field: node.Content[i].Value}
				stack = append(stack, child)
			}
		case yaml.SequenceNode:
			for i := len(node.Content) - 1; i >= 0; i-- {
				child := node.Content[i]
				if child == nil || child.Tag == "!!null" {
					return errors.New("configuration sequence item cannot be null")
				}
				item := configYAMLFrame{node: child, field: frame.field}
				stack = append(stack, item)
			}
		case yaml.ScalarNode:
			expectedTag := "!!str"
			switch frame.field {
			case "max_connecting", "max_operations", "max_pending_records", "max_batch_documents", "max_instructions", "max_call_depth", "max_stack_slots", "max_depth", "max_nodes", "metadata_cache_entries", "high_watermark", "low_watermark":
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
	}
	return nil
}
