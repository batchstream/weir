//go:build integration

package testmongo

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testutil"
)

// mongodBinary returns the MongoDB 8.0.32 server that scripts/mongod.sh installs.
func mongodBinary(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(testutil.Root(t), ".tools", "mongodb-8.0.32", "bin", "mongod")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, binary, "--version").Output()
	if err != nil || !strings.HasPrefix(string(output), "db version v8.0.32\n") {
		t.Fatalf("MongoDB 8.0.32 is not installed at %s; run scripts/mongod.sh", binary)
	}
	return binary
}
