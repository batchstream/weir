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

	"go.yaml.in/yaml/v3"
)

const maxConfigBytes = 128 << 10

type serviceFileConfig struct {
	Local  *Local  `yaml:"local"`
	Remote *Remote `yaml:"remote"`
}

// DecodeBasic reads process settings without opening the routing document.
func DecodeBasic(input io.Reader) (BasicConfig, error) {
	cfg := DefaultConfig().Basic
	if err := decodeConfigYAML(input, &cfg, false); err != nil {
		return cfg, fmt.Errorf("basic %w", err)
	}
	if strings.TrimSpace(cfg.Routing.File) == "" {
		return cfg, errors.New("basic configuration requires routing.file")
	}
	return cfg, cfg.Validate()
}

// DecodeRouting validates declarations without opening service files or backends.
func DecodeRouting(input io.Reader) (RoutingConfig, error) {
	cfg := RoutingConfig{}
	if err := decodeConfigYAML(input, &cfg, true); err != nil {
		return cfg, fmt.Errorf("routing %w", err)
	}
	return cfg, cfg.Validate()
}

// Load resolves every configuration file and validates the graph before startup.
// Relative routing paths are resolved from the basic document's directory.
// Relative service paths are resolved from the routing document's directory.
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
	for i := range routing.Services {
		service := &routing.Services[i]
		if service.File == "" {
			continue
		}
		serviceFilename := service.File
		if !filepath.IsAbs(serviceFilename) {
			serviceFilename = filepath.Join(filepath.Dir(routingFilename), serviceFilename)
		}
		definition, err := loadServiceFile(serviceFilename)
		if err != nil {
			return cfg, err
		}
		service.Local, service.Remote = definition.Local, definition.Remote
		service.File = ""
	}
	cfg.Basic, cfg.Routing = basic, routing
	return cfg, cfg.Validate()
}

func loadServiceFile(filename string) (serviceFileConfig, error) {
	cfg := serviceFileConfig{}
	info, err := os.Stat(filename)
	if err != nil || !info.Mode().IsRegular() {
		return cfg, errors.New("service configuration unavailable")
	}
	file, err := os.Open(filename)
	if err != nil {
		return cfg, errors.New("service configuration unavailable")
	}
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = file.Close()
		return cfg, errors.New("service configuration unavailable")
	}
	decodeErr := decodeConfigYAML(file, &cfg, true)
	closeErr := file.Close()
	if decodeErr != nil {
		return cfg, fmt.Errorf("service %w", decodeErr)
	}
	if closeErr != nil {
		return cfg, errors.New("service configuration unavailable")
	}
	if (cfg.Local == nil) == (cfg.Remote == nil) {
		return cfg, errors.New("service configuration requires exactly one of local or remote")
	}
	return cfg, nil
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
			case "local", "remote", "mongodb", "search", "connection", "services", "routes", "endpoints":
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
			err := validateConfigYAML(child, depth+1, allowNull, field)
			if err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		expectedTag := "!!str"
		switch field {
		case "max_connections", "max_sessions", "hop_limit", "max_concurrency", "max_batch_operations":
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
