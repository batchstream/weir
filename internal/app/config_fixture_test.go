package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

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
