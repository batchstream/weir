//go:build integration

package search

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testsearch"
)

// TestOpenSearchComponentEvidence records loaded classes in a fresh, owned TLS
// fixture. Absence from this finite observation is not a reachability proof.
func TestOpenSearchComponentEvidence(t *testing.T) {
	if os.Getenv("WEIR_SEARCH_COMPONENT_EVIDENCE") != "1" {
		t.Skip("component evidence requires WEIR_SEARCH_COMPONENT_EVIDENCE=1")
	}
	if os.Getenv("WEIR_SEARCH_SECURE_INTEGRATION") != "opensearch" {
		t.Fatal("component evidence requires the explicit OpenSearch secure fixture")
	}
	f := testsearch.OpenSecure(t)
	a := secureAdapter(t, f.Backend)
	assertOutcome(t, runSearch(t, a, searchPlan(t, a, "put", searchResource(f.Backend.Index, "component-evidence"))), pb.MutationOutcome_APPLIED, 0)
	result := runSearch(t, a, searchPlan(t, a, "read", searchResource(f.Backend.Index, "component-evidence")))
	if result.Read.GetDocument() == nil {
		t.Fatal("component evidence read failed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	name := filepath.Base(f.Root)
	list := exec.CommandContext(ctx, "docker", "exec", name, "/usr/share/opensearch/jdk/bin/jcmd", "-l")
	raw, err := list.Output()
	if err != nil {
		t.Fatal("cannot identify fixture JVM", err)
	}
	var pid string
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[1] == "org.opensearch.bootstrap.OpenSearch" {
			if pid != "" {
				t.Fatal("ambiguous fixture JVM")
			}
			pid = fields[0]
		}
	}
	if pid == "" {
		t.Fatal("fixture JVM not found")
	}
	command := exec.CommandContext(ctx, "docker", "exec", name, "/usr/share/opensearch/jdk/bin/jcmd", pid, "VM.classloaders", "show-classes=true", "verbose=true", "fold=false")
	output, err := command.Output()
	if err != nil {
		t.Fatal("cannot record loaded fixture classes", err)
	}
	if !strings.Contains(string(output), "org.opensearch.bootstrap.OpenSearch") {
		t.Fatal("incomplete classloader output")
	}
	destination := filepath.Join(f.Root, "loaded-classes.log")
	if err := os.WriteFile(destination, output, 0600); err != nil {
		t.Fatal("cannot retain classloader evidence", err)
	}
	t.Log("read-only classloader snapshot", destination)
}
