package kubernetes

import (
	"encoding/json"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/app"
)

func TestDeploymentConfiguration(t *testing.T) {
	cfg, err := app.Load("weir.yaml", "routes.yaml")
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Basic.Listeners.Application != "0.0.0.0:7447" || cfg.Basic.Diagnostics.Address != "127.0.0.1:7449" || cfg.Basic.Memory != app.ByteSize(768<<20) {
		t.Fatal("Pod bind or memory budget changed")
	}
	local := cfg.Routing.Services[0].Local
	if local.MaxConcurrency != 2 || local.MaxBatchOperations != 32 || local.BatchCollect == nil || time.Duration(*local.BatchCollect) != 5*time.Millisecond || local.MaxReadSize == nil || *local.MaxReadSize != app.ByteSize(16<<10) {
		t.Fatal("Pod routing does not use the tuned small-document defaults")
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
				Template struct {
					Spec struct {
						Containers []struct {
							Args []string
						}
					}
				}
			}
		}
	}
	// Kubernetes validates the full schema server-side in the opt-in fixture.
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Items) != 2 || manifest.Items[0].Spec.Replicas != 1 || manifest.Items[1].Spec.Type != "ClusterIP" {
		t.Fatal("invalid minimal manifest")
	}

	containers := manifest.Items[0].Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Fatal("deployment requires one Weir container")
	}
	expectedArgs := []string{"serve", "--config", "/etc/weir/node.yaml", "--routes", "/etc/weir/routes.yaml"}
	if !slices.Equal(containers[0].Args, expectedArgs) {
		t.Fatal("deployment must load the mounted YAML configuration")
	}
}

func TestOptionalHPARetainsExplicitReplicaAndRateBounds(t *testing.T) {
	raw, err := os.ReadFile("hpa.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		APIVersion string `json:"apiVersion"`
		Kind       string
		Spec       struct {
			MinReplicas    int
			MaxReplicas    int
			ScaleTargetRef struct {
				Kind string
				Name string
			}
			Behavior map[string]struct {
				StabilizationWindowSeconds int
				Policies                   []struct {
					Type          string
					Value         int
					PeriodSeconds int
				}
			}
		}
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.APIVersion != "autoscaling/v2" || manifest.Kind != "HorizontalPodAutoscaler" || manifest.Spec.MinReplicas != 1 || manifest.Spec.MaxReplicas != 4 || manifest.Spec.ScaleTargetRef.Kind != "Deployment" || manifest.Spec.ScaleTargetRef.Name != "weir" {
		t.Fatal("unbounded or unrelated HPA target")
	}
	for _, direction := range []string{"scaleUp", "scaleDown"} {
		behavior, found := manifest.Spec.Behavior[direction]
		if !found || behavior.StabilizationWindowSeconds < 60 || len(behavior.Policies) != 1 {
			t.Fatal("HPA requires bounded scale changes", direction)
		}
		policy := behavior.Policies[0]
		if policy.Type != "Pods" || policy.Value != 1 || policy.PeriodSeconds < 60 {
			t.Fatal("HPA scale rate exceeds one Pod per minute", direction)
		}
	}
}
