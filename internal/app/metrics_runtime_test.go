package app

import (
	"testing"
)

func TestStandardRuntimeCollectors(t *testing.T) {
	node := diagnosticNode(t)
	families, err := node.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, family := range families {
		name := family.GetName()
		found[name] = true
		if name == "go_goroutines" || name == "go_memstats_heap_alloc_bytes" {
			if len(family.Metric) != 1 || len(family.Metric[0].Label) != 0 {
				t.Fatal("runtime collector duplicated/labeled")
			}
		}
	}
	for _, name := range []string{
		"go_goroutines",
		"go_memstats_heap_alloc_bytes",
		"go_memstats_alloc_bytes_total",
		"go_gc_duration_seconds",
	} {
		if !found[name] {
			t.Fatal("missing", name)
		}
	}
}
