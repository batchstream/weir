package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/testutil"
)

func TestBasicConfigurationReference(t *testing.T) {
	filename := filepath.Join(testutil.Root(t), "config.example.yaml")
	cfg, err := Load(filename)
	if err != nil {
		t.Fatal("basic reference and its adjacent routing example must load", err)
	}

	expected := DefaultConfig().Basic
	expected.Listeners.Application = "127.0.0.1:7447"
	expected.Diagnostics.Address = "127.0.0.1:7449"
	expected.Routing.File = "routes.example.yaml"
	if cfg.Basic != expected {
		t.Fatal("basic reference differs from its documented defaults and addresses")
	}
	if len(cfg.Routing.Services) != 1 || len(cfg.Routing.Routes) != 1 || cfg.Routing.Services[0].Local.MongoDB == nil {
		t.Fatal("minimal routing example must declare one MongoDB service and route")
	}
}

func TestRoutingConfigurationReference(t *testing.T) {
	filename := filepath.Join(testutil.Root(t), "config.example.yaml")
	raw, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	_, block, found := strings.Cut(string(raw), "# routing-reference-begin\n")
	if !found {
		t.Fatal("routing reference start marker is missing")
	}
	block, _, found = strings.Cut(block, "# routing-reference-end")
	if !found {
		t.Fatal("routing reference end marker is missing")
	}

	var document strings.Builder
	for _, line := range strings.Split(strings.TrimSuffix(block, "\n"), "\n") {
		if line == "#" {
			line = ""
		} else {
			var commented bool
			line, commented = strings.CutPrefix(line, "# ")
			if !commented {
				t.Fatal("routing reference must become YAML after removing one comment prefix")
			}
		}
		document.WriteString(line)
		document.WriteByte('\n')
	}
	input := document.String()
	cfg, err := DecodeRouting(strings.NewReader(input))
	if err != nil {
		t.Fatal("complete commented routing reference must validate", err)
	}
	if len(cfg.Services) != 3 || len(cfg.Routes) != 3 ||
		cfg.Services[0].Local.MongoDB == nil ||
		cfg.Services[1].Local.Search == nil ||
		cfg.Services[2].Remote == nil {
		t.Fatal("complete reference must cover MongoDB, Search and remote services")
	}

	for _, profile := range []string{search.ElasticsearchProfile, search.OpenSearchProfile} {
		t.Run(profile, func(t *testing.T) {
			secured := strings.Replace(input, "http://127.0.0.1:9200", "https://127.0.0.1:9200", 1)
			secured = strings.Replace(secured, "profile: \""+search.ElasticsearchProfile+"\"", "profile: \""+profile+"\"", 1)
			for _, field := range []string{"connection:", "  username:", "  password:", "  ca_file:"} {
				if !strings.Contains(secured, "# "+field) {
					t.Fatal("optional HTTPS reference field is missing", field)
				}
				secured = strings.Replace(secured, "# "+field, field, 1)
			}
			cfg, err := DecodeRouting(strings.NewReader(secured))
			if err != nil {
				t.Fatal("uncommented HTTPS reference must validate without opening its CA path", err)
			}
			backend := cfg.Services[1].Local.Search
			if backend.Profile != profile || backend.Connection == nil ||
				backend.Connection.Username != "weir" ||
				backend.Connection.Password != "change-me" ||
				backend.Connection.CAFile != "/etc/weir/ca.pem" {
				t.Fatal("HTTPS reference did not populate its documented connection fields")
			}
		})
	}
}
