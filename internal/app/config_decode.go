package app

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"

	"go.yaml.in/yaml/v3"
)

// Load reads the configuration file, resolves its relative paths from the
// file's directory, reads credential files and validates the result. CA files
// are read at startup.
func Load(filename string) (Config, error) {
	var empty Config
	file, err := os.Open(filename)
	if err != nil {
		return empty, errors.New("configuration unavailable")
	}
	cfg, decodeErr := decode(file)
	closeErr := file.Close()
	if decodeErr != nil {
		return empty, decodeErr
	}
	if closeErr != nil {
		return empty, errors.New("configuration unavailable")
	}
	// Validate the whole file first, so a later error cannot trigger earlier IO.
	if err := cfg.validateShape(); err != nil {
		return empty, err
	}
	directory := filepath.Dir(filename)
	for _, definition := range cfg.Stores {
		if credentials := definition.Authentication; credentials != nil {
			if err := credentials.resolve(directory); err != nil {
				return empty, err
			}
		}
		if trust := definition.TLS; trust != nil && trust.CAFile != "" && !filepath.IsAbs(trust.CAFile) {
			trust.CAFile = filepath.Join(directory, trust.CAFile)
		}
	}
	return cfg, cfg.Validate()
}

func decode(input io.Reader) (Config, error) {
	var empty Config
	raw, err := io.ReadAll(input)
	if err != nil {
		return empty, errors.New("configuration unavailable")
	}
	cfg := Config{}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		// Parser errors may contain field names, input values or sensitive paths.
		return empty, errors.New("invalid configuration YAML or unknown field")
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); err != io.EOF {
		return empty, errors.New("configuration must be one YAML document")
	}
	// The decoder skips null sequence items, so count Store entries separately.
	var entries struct {
		Stores []yaml.Node `yaml:"stores"`
	}
	if err := yaml.Unmarshal(raw, &entries); err != nil || len(entries.Stores) != len(cfg.Stores) {
		return empty, errors.New("Store entries must not be null")
	}
	return cfg, nil
}
