package app

import (
	"io"
	"net/url"
	"os"
	"testing"

	"go.yaml.in/yaml/v3"
)

// Backend fixtures expose standard MongoDB URIs. Application configuration keeps
// credentials in explicit fields, so split the URI before writing configuration.
func mongoFixtureStore(t *testing.T, name, uri string) StoreConfig {
	t.Helper()
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatal("invalid owned MongoDB fixture URI")
	}
	mongo := &MongoDB{}
	definition := StoreConfig{Name: name, MongoDB: mongo}
	if parsed.User != nil {
		credentials := &Credentials{Username: parsed.User.Username()}
		credentials.Password, _ = parsed.User.Password()
		definition.Authentication = credentials
		parsed.User = nil
	}
	query := parsed.Query()
	if ca := query.Get("tlsCAFile"); ca != "" {
		definition.TLS = &TLS{CAFile: ca}
		query.Del("tlsCAFile")
	}
	parsed.RawQuery = query.Encode()
	mongo.URI = parsed.String()
	return definition
}

// searchFixtureStore hosts one Store on a Search URL without credentials.
func searchFixtureStore(name, uri string) StoreConfig {
	search := &Search{URL: uri}
	definition := StoreConfig{Name: name, Search: search}
	return definition
}

// writeConfigFile writes a test-owned configuration document, including
// container mounts.
func writeConfigFile(t *testing.T, filename string, cfg Config, mode os.FileMode) {
	t.Helper()
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, raw, mode); err != nil {
		t.Fatal(err)
	}
}

// Decode reads one configuration document and validates it without IO, so a
// document naming credential files fails validation.
func Decode(input io.Reader) (Config, error) {
	cfg, err := decode(input)
	if err != nil {
		return cfg, err
	}
	return cfg, cfg.Validate()
}
