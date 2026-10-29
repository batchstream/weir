//go:build integration

package testmongo

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil"
)

// mongodBinary returns WEIR_MONGOD or the MongoDB 8.0.32 server that
// scripts/mongod.sh installs.
func mongodBinary(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("WEIR_MONGOD")
	if binary == "" {
		binary = filepath.Join(".tools", "mongodb-8.0.32", "bin", "mongod")
	}
	// Like scripts/mongo-local.sh, resolve relative paths from the repository root.
	if strings.Contains(binary, "/") && !filepath.IsAbs(binary) {
		binary = filepath.Join(testutil.Root(t), binary)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, binary, "--version").Output()
	if err != nil {
		t.Fatalf("cannot run %s: %v; run scripts/mongod.sh or set WEIR_MONGOD", binary, err)
	}
	if !strings.HasPrefix(string(output), "db version v8.0.32\n") {
		t.Fatalf("%s is not MongoDB 8.0.32", binary)
	}
	return binary
}
