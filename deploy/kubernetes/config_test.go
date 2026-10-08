package kubernetes

import (
	"encoding/json"
	"os"
	"slices"
	"testing"

	"github.com/batchstream/weir/internal/app"
)

func TestDeploymentConfiguration(t *testing.T) {
	t.Setenv("WEIR_PEER_ADDRESS", "192.0.2.1:7448")
	cfg, err := app.Load("weir.yaml", "routes.yaml")
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Basic.Listeners.Application != "0.0.0.0:7447" || cfg.Basic.Diagnostics.Address != "127.0.0.1:7449" {
		t.Fatal("Pod bind changed")
	}
	local := cfg.Routing.Stores[0].Local
	if local.Batching.MaxOperations != 32 {
		t.Fatal("Pod routing has unexpected batching defaults")
	}

	raw, err := os.ReadFile("weir.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Items []struct {
			Kind string
			Spec struct {
				Type      string
				ClusterIP string
				Ports     []struct {
					Name       string
					Port       int
					TargetPort string
				}
				Replicas int
				Template struct {
					Spec struct {
						Containers []struct {
							Args  []string
							Ports []struct {
								Name          string
								ContainerPort int
							}
							Env []struct {
								Name      string
								Value     string
								ValueFrom struct{ FieldRef struct{ FieldPath string } }
							}
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
	if len(manifest.Items) != 3 || manifest.Items[0].Spec.Replicas != 1 || manifest.Items[1].Spec.Type != "ClusterIP" {
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
	if cfg.Basic.Listeners.Peer != "0.0.0.0:7448" || cfg.Basic.Discovery.PeerAddress != "192.0.2.1:7448" || cfg.Basic.Discovery.Group != "records" || !slices.Equal(cfg.Basic.Discovery.Seeds, []string{"weir:7448"}) || !slices.Equal(cfg.Basic.Discovery.Advertise, []string{"weir-headless:7447"}) {
		t.Fatal("directory seed or group target is not wired")
	}
	if manifest.Items[2].Kind != "Service" || manifest.Items[2].Spec.ClusterIP != "None" || len(manifest.Items[2].Spec.Ports) != 1 || manifest.Items[2].Spec.Ports[0].Port != 7447 {
		t.Fatal("Store DNS requires headless business Service")
	}
	container := containers[0]
	if len(container.Ports) != 2 || container.Ports[1].Name != "peer" || container.Ports[1].ContainerPort != 7448 || len(manifest.Items[1].Spec.Ports) != 2 || manifest.Items[1].Spec.Ports[1].Port != 7448 || manifest.Items[1].Spec.Ports[1].TargetPort != "peer" {
		t.Fatal("ordinary initialization Service must expose peer directory port")
	}
	if len(container.Env) != 2 || container.Env[0].Name != "POD_IP" || container.Env[0].ValueFrom.FieldRef.FieldPath != "status.podIP" || container.Env[1].Name != "WEIR_PEER_ADDRESS" || container.Env[1].Value != "$(POD_IP):7448" {
		t.Fatal("Pod address must be advertised through explicit public environment source")
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
