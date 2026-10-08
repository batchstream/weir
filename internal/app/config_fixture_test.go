package app

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// Backend fixtures expose standard MongoDB URIs. Application configuration keeps
// credentials in explicit fields, so split the URI before writing configuration.
func mongoFixtureConfig(t *testing.T, uri string) BackendConfig {
	t.Helper()
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatal("invalid owned MongoDB fixture URI")
	}
	mongo := &Mongo{}
	backend := BackendConfig{MongoDB: mongo}
	if parsed.User != nil {
		credentials := &Credentials{Username: parsed.User.Username()}
		credentials.Password, _ = parsed.User.Password()
		backend.Authentication = credentials
		parsed.User = nil
	}
	query := parsed.Query()
	if ca := query.Get("tlsCAFile"); ca != "" {
		backend.TLS = &BackendTLS{CAFile: ca}
		query.Del("tlsCAFile")
	}
	parsed.RawQuery = query.Encode()
	mongo.URI = parsed.String()
	return backend
}

// Write only test-owned basic and routing documents, including container mounts.
func writeConfigFiles(t *testing.T, filename string, cfg Config, mode os.FileMode) string {
	t.Helper()
	base := strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename))
	routingFilename := filepath.Join(filepath.Dir(filename), base+"-routing.yaml")
	routing, err := yaml.Marshal(cfg.Routing)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(routingFilename, routing, mode); err != nil {
		t.Fatal(err)
	}
	basic, err := yaml.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, basic, mode); err != nil {
		t.Fatal(err)
	}
	return routingFilename
}
