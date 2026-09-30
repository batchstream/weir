package kubernetes

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/batchstream/weir/internal/app"
)

func TestDeploymentConfiguration(t *testing.T) {
	cfg, err := app.Load("node.example.json")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Basic.Listeners.Application != "0.0.0.0:7447" || cfg.Basic.Diagnostics.Address != "127.0.0.1:7449" || cfg.Basic.Memory != app.ByteSize(768<<20) {
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
