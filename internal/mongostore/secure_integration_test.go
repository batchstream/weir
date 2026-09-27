//go:build integration

package mongostore

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/testmongo"
)

func TestMongoSCRAMTLSProfileFailsClosedAtBoundedReader(t *testing.T) {
	if os.Getenv("WEIR_M10_INTEGRATION") != "1" {
		t.Skip("secure MongoDB profile is explicit opt-in")
	}
	fixture := testmongo.OpenSecure(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	config := Config{URI: fixture.URI, Store: "mongo", Database: fixture.DB, Collection: "records", Pool: 4}
	adapter, err := Open(ctx, config)
	if err == nil || adapter != nil || !strings.Contains(err.Error(), "not qualified") || strings.Contains(err.Error(), "weir_app") {
		t.Fatal("unproven TLS/SCRAM transport must be rejected before connecting")
	}
}
