package app

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"

	"go.yaml.in/yaml/v3"
)

// Decode reads one configuration document and validates it without IO.
// Credential files are not read, so a document naming them fails validation;
// use Load to resolve them.
func Decode(input io.Reader) (Config, error) {
	cfg, err := decode(input)
	if err != nil {
		return cfg, err
	}
	return cfg, cfg.Validate()
}

// Load reads the configuration file, resolves its relative paths from the
// file's directory, reads credential files and validates the result. CA files
// are read at startup.
func Load(filename string) (Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return Config{}, errors.New("configuration unavailable")
	}
	cfg, decodeErr := decode(file)
	closeErr := file.Close()
	if decodeErr != nil {
		return Config{}, decodeErr
	}
	if closeErr != nil {
		return Config{}, errors.New("configuration unavailable")
	}
	// Validate every source first, so a later conflict cannot trigger earlier IO.
	if err := cfg.validateSources(); err != nil {
		return Config{}, err
	}
	directory := filepath.Dir(filename)
	for _, definition := range cfg.Stores {
		if credentials := definition.Authentication; credentials != nil {
			if err := credentials.resolve(directory); err != nil {
				return Config{}, err
			}
		}
		if trust := definition.TLS; trust != nil && trust.CAFile != "" && !filepath.IsAbs(trust.CAFile) {
			trust.CAFile = filepath.Join(directory, trust.CAFile)
		}
	}
	return cfg, cfg.Validate()
}

func decode(input io.Reader) (Config, error) {
	cfg := Config{}
	raw, err := io.ReadAll(input)
	if err != nil {
		return cfg, errors.New("configuration unavailable")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		// Parser errors may contain field names, input values or sensitive paths.
		return Config{}, errors.New("invalid configuration YAML or unknown field")
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Config{}, errors.New("configuration must be one YAML document")
	}
	return cfg, nil
}
