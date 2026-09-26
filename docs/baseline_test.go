package docs

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// Section/reference parity catches structural drift; semantic review remains required.
func TestArchitectureTranslationParity(t *testing.T) {
	english, err := os.ReadFile("architecture.md")
	if err != nil {
		t.Fatal(err)
	}
	chinese, err := os.ReadFile("architecture.zh-CN.md")
	if err != nil {
		t.Fatal(err)
	}
	pattern := regexp.MustCompile(`(?m)^#{2,3} (\d+(?:\.\d+)?)[. ]`)
	en, zh := pattern.FindAllSubmatch(english, -1), pattern.FindAllSubmatch(chinese, -1)
	if len(en) != len(zh) {
		t.Fatalf("section counts: en=%d zh=%d", len(en), len(zh))
	}
	for i := range en {
		if string(en[i][1]) != string(zh[i][1]) {
			t.Fatalf("section mismatch %q %q", en[i][1], zh[i][1])
		}
	}
	for _, id := range []string{"UNSUPPORTED", "NOT_STARTED", "NOT_APPLIED", "APPLIED", "UNKNOWN", "Cmin=1"} {
		if !strings.Contains(string(english), id) || !strings.Contains(string(chinese), id) {
			t.Error("contract missing", id)
		}
	}
	refs := regexp.MustCompile(`(?m)(?:\*\*D\d+ -|\| S\d+ \|)`)
	if len(refs.FindAll(english, -1)) != len(refs.FindAll(chinese, -1)) {
		t.Fatal("reference registry mismatch")
	}
	scenarios := regexp.MustCompile(`(?ms)^### 20\.1 .*?\n(.*?)^### 20\.2 `)
	enScenarios := scenarios.FindSubmatch(english)
	zhScenarios := scenarios.FindSubmatch(chinese)
	if len(enScenarios) != 2 || len(zhScenarios) != 2 || strings.Count(string(enScenarios[1]), "\n|") != strings.Count(string(zhScenarios[1]), "\n|") {
		t.Fatal("conformance scenario table mismatch")
	}
}

func TestArchitectureExcludesMilestoneReports(t *testing.T) {
	for _, name := range []string{"architecture.md", "architecture.zh-CN.md"} {
		t.Run(name, func(t *testing.T) {
			content, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			text := strings.ToLower(string(content))
			for _, marker := range []string{"milestone", "里程碑"} {
				if strings.Contains(text, marker) {
					t.Errorf("architecture must not contain milestone reports: found %q", marker)
				}
			}
		})
	}
}
