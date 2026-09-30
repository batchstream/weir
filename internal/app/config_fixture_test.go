package app

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Write only test-owned basic and routing documents, including container mounts.
func writeConfigFiles(t *testing.T, filename string, cfg Config, mode os.FileMode) {
	t.Helper()
	base := strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename))
	routingFilename := filepath.Join(filepath.Dir(filename), base+"-routing.json")
	cfg.Basic.Routing.File = filepath.Base(routingFilename)
	routing, err := json.Marshal(cfg.Routing)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(routingFilename, routing, mode); err != nil {
		t.Fatal(err)
	}
	basic, err := json.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, basic, mode); err != nil {
		t.Fatal(err)
	}
}
