package kubernetes

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/batchstream/weir/internal/app"
)

func TestDeploymentConfiguration(t *testing.T) {
	file, err := os.Open("node.example.json")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	cfg, err := app.Decode(file)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Application != "0.0.0.0:7447" || cfg.Diagnostics != "127.0.0.1:7449" || cfg.MemoryMiB != 768 {
		t.Fatal("Pod bind or memory budget changed")
	}
	raw, err := os.ReadFile("weir.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Items []struct {
			Kind string
			Spec struct {
				Type     string
				Replicas int
			}
		}
	}
	// Kubernetes validates the full schema server-side in the opt-in fixture.
	if json.Unmarshal(raw, &manifest) != nil || len(manifest.Items) != 2 || manifest.Items[0].Spec.Replicas != 1 || manifest.Items[1].Spec.Type != "ClusterIP" {
		t.Fatal("invalid minimal manifest")
	}
}
