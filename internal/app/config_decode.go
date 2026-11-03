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
	raw, err := os.ReadFile(filename)
	if err != nil {
		return empty, errors.New("configuration unavailable")
	}
	cfg, err := decode(raw)
	if err != nil {
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

func decode(raw []byte) (Config, error) {
	var empty Config
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
	return cfg, nil
}
