// Package testutil locates repository assets for owned test fixtures and process tests.
// Production packages must not import it.
package testutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Root finds this module from any package working directory, without assuming a
// fixed directory depth or relying on a developer-specific source path.
func Root(t testing.TB) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal("cannot locate test working directory")
	}
	for {
		module, err := os.ReadFile(filepath.Join(directory, "go.mod"))
		if err == nil && strings.HasPrefix(string(module), "module github.com/batchstream/weir\n") {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("test must run inside the Weir module")
		}
		directory = parent
	}
}
