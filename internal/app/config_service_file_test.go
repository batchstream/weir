package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

const remoteServiceDocument = `remote:
  endpoints: [127.0.0.1:7448]
  max_concurrency: 2
`

const mongoServiceDocument = `local:
  mongodb:
    uri: mongodb://unresolved.invalid:27017
    database: example
    collection: records
`

const searchServiceDocument = `local:
  search:
    url: https://unresolved.invalid:9200
    index: records
    profile: elasticsearch-8.19.22
    connection:
      username: user-secret-sentinel
      password: password-secret-sentinel
      ca_file: /missing/ca-secret-sentinel.pem
`

func writeServiceFileTestDocuments(t *testing.T, cfg Config) (string, string) {
	t.Helper()
	root := t.TempDir()
	basicDirectory := filepath.Join(root, "basic")
	routingDirectory := filepath.Join(root, "routing")
	for _, directory := range []string{basicDirectory, routingDirectory} {
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
	}

	cfg.Basic.Routing.File = "../routing/routes.yaml"
	basic, err := yaml.Marshal(cfg.Basic)
	if err != nil {
		t.Fatal(err)
	}
	routing, err := yaml.Marshal(cfg.Routing)
	if err != nil {
		t.Fatal(err)
	}
	basicFilename := filepath.Join(basicDirectory, "weir.yaml")
	routingFilename := filepath.Join(routingDirectory, "routes.yaml")
	if err := os.WriteFile(basicFilename, basic, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(routingFilename, routing, 0600); err != nil {
		t.Fatal(err)
	}
	return basicFilename, routingFilename
}

func TestLoadServiceFilePathsAndBackends(t *testing.T) {
	cases := []struct {
		name     string
		document string
	}{
		{"remote", remoteServiceDocument},
		{"mongodb", mongoServiceDocument},
		{"search", searchServiceDocument},
	}
	for _, tc := range cases {
		for _, pathMode := range []string{"relative", "absolute"} {
			t.Run(tc.name+"/"+pathMode, func(t *testing.T) {
				cfg := remoteConfig(t)
				service := &cfg.Routing.Services[0]
				service.Name, service.Remote = tc.name, nil
				service.File = "service.yaml"
				cfg.Routing.Routes[0].Service = tc.name
				if pathMode == "absolute" {
					service.File = filepath.Join(t.TempDir(), "service.yaml")
				}
				basicFilename, routingFilename := writeServiceFileTestDocuments(t, cfg)
				serviceFilename := service.File
				if pathMode == "relative" {
					serviceFilename = filepath.Join(filepath.Dir(routingFilename), service.File)
					decoy := filepath.Join(filepath.Dir(basicFilename), service.File)
					if err := os.WriteFile(decoy, []byte("invalid-secret-sentinel"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(serviceFilename, []byte(tc.document), 0600); err != nil {
					t.Fatal(err)
				}

				loaded, err := Load(basicFilename)
				if err != nil {
					t.Fatal("service configuration must resolve without backend, DNS or CA-file IO", err)
				}
				resolved := loaded.Routing.Services[0]
				if resolved.File != "" || resolved.Name != tc.name || service.File == "" {
					t.Fatal("service resolution lost the name, retained a file or mutated the caller's configuration")
				}
				switch tc.name {
				case "remote":
					if resolved.Remote == nil || resolved.Local != nil || resolved.Remote.MaxConcurrency != 2 {
						t.Fatal("remote service file was not resolved")
					}
				case "mongodb":
					if resolved.Local == nil || resolved.Local.MongoDB == nil || resolved.Remote != nil || resolved.Local.MongoDB.Database != "example" {
						t.Fatal("MongoDB service file was not resolved")
					}
				case "search":
					if resolved.Local == nil || resolved.Local.Search == nil || resolved.Remote != nil || resolved.Local.Search.Connection.CAFile != "/missing/ca-secret-sentinel.pem" {
						t.Fatal("Search service file was not resolved")
					}
				}
				if err := loaded.Validate(); err != nil {
					t.Fatal("resolved configuration must satisfy runtime validation", err)
				}
			})
		}
	}
}

func TestLoadServiceFileSymlinks(t *testing.T) {
	for _, mode := range []string{"file", "directory", "routing-file"} {
		t.Run(mode, func(t *testing.T) {
			cfg := remoteConfig(t)
			cfg.Routing.Services[0].Remote = nil
			cfg.Routing.Services[0].File = "service.yaml"
			if mode == "directory" {
				cfg.Routing.Services[0].File = "current/service.yaml"
			}
			basicFilename, routingFilename := writeServiceFileTestDocuments(t, cfg)
			directory := filepath.Dir(routingFilename)
			targetDirectory := filepath.Join(directory, "revision")
			if err := os.Mkdir(targetDirectory, 0700); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(targetDirectory, "service.yaml")
			if mode == "routing-file" {
				// The service path stays relative to the public routing filename,
				// rather than the directory containing its symlink target.
				target = filepath.Join(directory, "service.yaml")
				if err := os.Rename(routingFilename, filepath.Join(targetDirectory, "routes.yaml")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("revision/routes.yaml", routingFilename); err != nil {
					t.Fatal(err)
				}
			} else if mode == "directory" {
				if err := os.Symlink("revision", filepath.Join(directory, "current")); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Symlink("revision/service.yaml", filepath.Join(directory, "service.yaml")); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(target, []byte(remoteServiceDocument), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(basicFilename); err != nil {
				t.Fatal("ordinary file and directory symlinks must be followed", err)
			}
		})
	}
}

func TestLoadServiceFileRejectsInvalidDocuments(t *testing.T) {
	cases := []struct {
		name     string
		document string
	}{
		{"empty", ""},
		{"whitespace", " \n\t\n"},
		{"null", "null\n"},
		{"sequence", "[]\n"},
		{"malformed", "remote: [secret-sentinel\n"},
		{"name", "name: secret-sentinel\n" + remoteServiceDocument},
		{"empty-name", "name: ''\n" + remoteServiceDocument},
		{"recursive-file", "file: secret-sentinel.yaml\n" + remoteServiceDocument},
		{"empty-file", "file: ''\n" + remoteServiceDocument},
		{"routes", "routes: []\n" + remoteServiceDocument},
		{"services", "services: []\n" + remoteServiceDocument},
		{"both-adapters", mongoServiceDocument + remoteServiceDocument},
		{"neither-adapter", "{}\n"},
		{"null-adapters", "local: null\nremote: null\n"},
		{"unknown-field", "secret-sentinel: value\n" + remoteServiceDocument},
		{"nested-unknown-field", strings.Replace(remoteServiceDocument, "remote:\n", "remote:\n  secret-sentinel: value\n", 1)},
		{"scalar-type", strings.Replace(remoteServiceDocument, "max_concurrency: 2", "max_concurrency: '2'", 1)},
		{"duplicate", remoteServiceDocument + remoteServiceDocument},
		{"case-duplicate", remoteServiceDocument + "REMOTE: null\n"},
		{"nested-duplicate", strings.Replace(remoteServiceDocument, "max_concurrency: 2", "max_concurrency: 2\n  MAX_CONCURRENCY: 3", 1)},
		{"anchor", strings.Replace(remoteServiceDocument, "remote:", "remote: &secret-sentinel", 1)},
		{"alias", "local: &a null\nremote: *a\n"},
		{"merge", "<<: {}\n" + remoteServiceDocument},
		{"quoted-merge", "'<<': {}\n" + remoteServiceDocument},
		{"explicit-tag", "!!map\n" + remoteServiceDocument},
		{"multiple-documents", remoteServiceDocument + "---\n{}\n"},
		{"depth", "secret-sentinel: " + strings.Repeat("[", 12) + "value" + strings.Repeat("]", 12) + "\n"},
		{"oversized", remoteServiceDocument + strings.Repeat(" ", maxConfigBytes)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := remoteConfig(t)
			cfg.Routing.Services[0].Remote = nil
			cfg.Routing.Services[0].File = "service-secret-sentinel.yaml"
			basicFilename, routingFilename := writeServiceFileTestDocuments(t, cfg)
			filename := filepath.Join(filepath.Dir(routingFilename), cfg.Routing.Services[0].File)
			if err := os.WriteFile(filename, []byte(tc.document), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(basicFilename)
			if err == nil || strings.Contains(err.Error(), "sentinel") || strings.Contains(err.Error(), filepath.Dir(filename)) {
				t.Fatal("invalid service file accepted or error exposed its path/content", err)
			}
		})
	}
}

func TestLoadServiceFileSizeBound(t *testing.T) {
	cfg := remoteConfig(t)
	cfg.Routing.Services[0].Remote = nil
	cfg.Routing.Services[0].File = "service.yaml"
	basicFilename, routingFilename := writeServiceFileTestDocuments(t, cfg)
	filename := filepath.Join(filepath.Dir(routingFilename), cfg.Routing.Services[0].File)
	bounded := remoteServiceDocument + strings.Repeat(" ", maxConfigBytes-len(remoteServiceDocument))
	if err := os.WriteFile(filename, []byte(bounded), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(basicFilename); err != nil {
		t.Fatal("exact service file size bound must be accepted", err)
	}
	if err := os.WriteFile(filename, []byte(bounded+" "), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(basicFilename); err == nil || err.Error() != "service configuration exceeds bound" {
		t.Fatal("oversized service file must be rejected", err)
	}
}

func TestLoadServiceFileUnavailable(t *testing.T) {
	for _, mode := range []string{"missing", "directory", "dangling-symlink", "directory-symlink"} {
		t.Run(mode, func(t *testing.T) {
			cfg := remoteConfig(t)
			cfg.Routing.Services[0].Remote = nil
			cfg.Routing.Services[0].File = "service-secret-sentinel.yaml"
			basicFilename, routingFilename := writeServiceFileTestDocuments(t, cfg)
			filename := filepath.Join(filepath.Dir(routingFilename), cfg.Routing.Services[0].File)
			switch mode {
			case "directory":
				if err := os.Mkdir(filename, 0700); err != nil {
					t.Fatal(err)
				}
			case "dangling-symlink":
				if err := os.Symlink("missing-secret-sentinel.yaml", filename); err != nil {
					t.Fatal(err)
				}
			case "directory-symlink":
				if err := os.Symlink(".", filename); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := Load(basicFilename); err == nil || err.Error() != "service configuration unavailable" {
				t.Fatal("unavailable or non-regular service files must be rejected without disclosing paths", err)
			}
		})
	}
}

func TestServiceFileDeclarationChoices(t *testing.T) {
	for _, mode := range []string{"file", "empty-file-inline", "file-and-remote", "file-and-local", "all-three", "neither", "blank-file"} {
		t.Run(mode, func(t *testing.T) {
			cfg := remoteConfig(t)
			service := &cfg.Routing.Services[0]
			service.File = "service-secret-sentinel.yaml"
			mongo := &Mongo{URI: "mongodb://127.0.0.1:27017", Database: "example", Collection: "records"}
			local := &Local{MongoDB: mongo}
			switch mode {
			case "file":
				service.Remote = nil
			case "empty-file-inline":
				service.File = ""
			case "file-and-local":
				service.Remote, service.Local = nil, local
			case "all-three":
				service.Local = local
			case "neither":
				service.Remote, service.File = nil, ""
			case "blank-file":
				service.Remote, service.File = nil, " \t "
			}
			raw, err := yaml.Marshal(cfg.Routing)
			if err != nil {
				t.Fatal(err)
			}
			_, err = DecodeRouting(strings.NewReader(string(raw)))
			if mode == "file" || mode == "empty-file-inline" {
				if err != nil {
					t.Fatal("declaration validation must not open a service file", err)
				}
			} else if err == nil || strings.Contains(err.Error(), "sentinel") {
				t.Fatal("invalid service source selection accepted or leaked a path", err)
			}
		})
	}
}

func TestUnresolvedServiceFileCannotOpen(t *testing.T) {
	cfg := remoteConfig(t)
	cfg.Routing.Services[0].Remote = nil
	cfg.Routing.Services[0].File = "unopened-service-secret-sentinel.yaml"
	if err := cfg.Routing.Validate(); err != nil {
		t.Fatal("file declaration must pass pure graph validation", err)
	}
	if err := cfg.Validate(); err == nil || err.Error() != "unresolved Service configuration" {
		t.Fatal("runtime validation must reject an unresolved file", err)
	}
	node, err := Open(context.Background(), cfg)
	if node != nil || err == nil || err.Error() != "unresolved Service configuration" {
		t.Fatal("unresolved service must fail before backend or listener startup", err)
	}
}

func TestLoadServiceFileFinalGraphValidation(t *testing.T) {
	for _, mode := range []string{"local-alias", "remote-alias", "invalid-remote", "invalid-mongodb", "invalid-search"} {
		t.Run(mode, func(t *testing.T) {
			cfg := remoteConfig(t)
			cfg.Routing.Services[0].Remote = nil
			cfg.Routing.Services[0].File = "service.yaml"
			document := remoteServiceDocument
			if mode == "local-alias" || mode == "remote-alias" {
				route := Route{Store: "another", Service: cfg.Routing.Services[0].Name}
				cfg.Routing.Routes = append(cfg.Routing.Routes, route)
			}
			switch mode {
			case "local-alias":
				document = mongoServiceDocument
			case "invalid-remote":
				document = strings.Replace(remoteServiceDocument, "max_concurrency: 2", "max_concurrency: 0", 1)
			case "invalid-mongodb":
				document = strings.Replace(mongoServiceDocument, "mongodb://unresolved.invalid:27017", "mongodb://user:password-secret-sentinel@unresolved.invalid:27017", 1)
			case "invalid-search":
				document = strings.Replace(searchServiceDocument, "elasticsearch-8.19.22", "profile-secret-sentinel", 1)
			}
			if err := cfg.Routing.Validate(); err != nil {
				t.Fatal("valid unresolved graph should defer backend-specific validation", err)
			}
			basicFilename, routingFilename := writeServiceFileTestDocuments(t, cfg)
			filename := filepath.Join(filepath.Dir(routingFilename), cfg.Routing.Services[0].File)
			if err := os.WriteFile(filename, []byte(document), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(basicFilename)
			if mode == "remote-alias" {
				if err != nil {
					t.Fatal("remote service may have multiple Store routes", err)
				}
			} else if err == nil || strings.Contains(err.Error(), "sentinel") {
				t.Fatal("resolved graph or backend settings must be validated without leaking credentials", err)
			} else if mode == "local-alias" && err.Error() != "unused or aliased LocalStore" {
				t.Fatal("resolved local service must retain the single-route rule", err)
			}
		})
	}
}

func TestLoadValidatesLaterServiceFileBeforeStartup(t *testing.T) {
	cfg := remoteConfig(t)
	first := Service{Name: "search", File: "search.yaml"}
	second := Service{Name: "remote", File: "invalid-secret-sentinel.yaml"}
	searchRoute := Route{Store: "search", Service: "search"}
	remoteRoute := Route{Store: "remote", Service: "remote"}
	cfg.Routing.Services = []Service{first, second}
	cfg.Routing.Routes = []Route{searchRoute, remoteRoute}
	basicFilename, routingFilename := writeServiceFileTestDocuments(t, cfg)
	directory := filepath.Dir(routingFilename)
	if err := os.WriteFile(filepath.Join(directory, first.File), []byte(searchServiceDocument), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, second.File), []byte("remote: [secret-sentinel\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(basicFilename); err == nil || err.Error() != "service invalid configuration YAML" {
		t.Fatal("later service file must be parsed before opening the first service's missing CA or backend", err)
	}
}
