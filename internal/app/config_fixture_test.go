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
func mongoFixtureConfig(t *testing.T, uri, database string) *Mongo {
	t.Helper()
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatal("invalid owned MongoDB fixture URI")
	}
	backend := &Mongo{Database: database, Collection: "records"}
	if parsed.User != nil {
		backend.Username = parsed.User.Username()
		backend.Password, _ = parsed.User.Password()
		parsed.User = nil
	}
	backend.URI = parsed.String()
	return backend
}

// Write only test-owned basic and routing documents, including container mounts.
func writeConfigFiles(t *testing.T, filename string, cfg Config, mode os.FileMode) {
	t.Helper()
	base := strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename))
	routingFilename := filepath.Join(filepath.Dir(filename), base+"-routing.yaml")
	cfg.Basic.Routing.File = filepath.Base(routingFilename)
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
}
