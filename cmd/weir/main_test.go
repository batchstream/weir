package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestVersionWithoutConfiguration(t *testing.T) {
	t.Chdir(t.TempDir())

	var output bytes.Buffer
	if err := run([]string{"version"}, &output); err != nil {
		t.Fatal(err)
	}

	var identity map[string]string
	if err := json.Unmarshal(output.Bytes(), &identity); err != nil {
		t.Fatal(err)
	}

	if identity["product"] != "weir" ||
		identity["go"] != runtime.Version() ||
		identity["target"] != runtime.GOOS+"/"+runtime.GOARCH ||
		identity["state"] != "dev" {
		t.Fatal(identity)
	}
}

func TestCheckConfigWithCredentialFilesWithoutBackendOrCAAccess(t *testing.T) {
	directory := t.TempDir()
	file := filepath.Join(directory, "weir.yaml")
	routingDirectory := filepath.Join(directory, "nested")
	valueDirectory := filepath.Join(routingDirectory, "values")
	if err := os.MkdirAll(valueDirectory, 0700); err != nil {
		t.Fatal(err)
	}

	routingFile := filepath.Join(routingDirectory, "routes.yaml")
	usernameFile := filepath.Join(valueDirectory, "username")
	passwordFile := filepath.Join(valueDirectory, "password")
	basic := `listeners:
  application: 192.0.2.1:7447
`
	routing := `services:
  - name: search
    local:
      search:
        url: https://unresolved.invalid:443
        connection:
          username_file: values/username
          password_file: values/password
          ca_file: /missing/ca-sentinel.pem
routes:
  - store: records
    service: search
`
	if err := os.WriteFile(file, []byte(basic), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(routingFile, []byte(routing), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(usernameFile, []byte("user"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(passwordFile, []byte("password-sentinel"), 0600); err != nil {
		t.Fatal(err)
	}

	t.Chdir(directory)
	var output bytes.Buffer
	if err := run([]string{"check", "--routes", "nested/routes.yaml"}, &output); err != nil || output.String() != "configuration valid\n" {
		t.Fatal("explicit routes must resolve nested credential values without startup IO", err, output.String())
	}

	// Value paths belong to the routing file, even from a different working directory.
	t.Chdir(t.TempDir())
	for _, flag := range []string{"--config", "-c"} {
		output.Reset()
		if err := run([]string{"check", flag, file, "--routes", routingFile}, &output); err != nil {
			t.Fatal(err)
		}
		if output.String() != "configuration valid\n" {
			t.Fatal("validation output must not reveal configuration", output.String())
		}
	}

	t.Chdir(directory)
	invalidCases := []struct {
		document string
		reason   string
	}{
		{
			strings.Replace(routing, "          username_file:", "          username: user-sentinel\n          username_file:", 1),
			"credential value and file are mutually exclusive",
		},
		{
			strings.Replace(routing, "values/password", "values/missing-secret-sentinel", 1),
			"credential file unavailable",
		},
		{
			strings.Replace(routing, "    local:", "    file: service-sentinel.yaml\n    local:", 1),
			"routing invalid configuration YAML or unknown field",
		},
	}
	for _, tc := range invalidCases {
		if err := os.WriteFile(routingFile, []byte(tc.document), 0600); err != nil {
			t.Fatal(err)
		}
		for _, command := range []string{"check", "serve"} {
			output.Reset()
			err := run([]string{command, "--routes", "nested/routes.yaml"}, &output)
			if err == nil || err.Error() != tc.reason || output.Len() != 0 {
				t.Fatal("invalid credential source must fail before startup without exposing values", command, err, output.String())
			}
		}
	}
}

func TestHelpWithoutConfiguration(t *testing.T) {
	t.Chdir(t.TempDir())
	cases := [][]string{
		nil,
		{"--help"},
		{"-h"},
		{"help"},
		{"help", "serve"},
		{"serve", "--help"},
		{"check", "-h"},
		{"version", "--help"},
		{"probe", "--help"},
	}
	for _, args := range cases {
		var output bytes.Buffer
		if err := run(args, &output); err != nil || !strings.Contains(output.String(), "Usage:") {
			t.Fatal("help must short circuit server startup", args, err, output.String())
		}
		for _, legacy := range []string{
			"--diagnostics",
			"--listen",
			"--mongo-uri",
			"--database",
			"--collection",
			"--batch",
			"--memory-mib",
			"--search-url",
			"--search-index",
			"--search-profile",
			"--check-config",
			"--probe-address",
		} {
			if strings.Contains(output.String(), legacy) {
				t.Fatal("retained removed flag", legacy, output.String())
			}
		}
	}

	var output bytes.Buffer
	if err := run(nil, &output); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"serve", "check", "version", "probe"} {
		if !strings.Contains(output.String(), "  "+command+" ") {
			t.Fatal("missing supported command", command, output.String())
		}
	}
	if strings.Contains(output.String(), "completion") ||
		strings.Contains(output.String(), "--config") ||
		strings.Contains(output.String(), "--routes") ||
		strings.Contains(output.String(), "--address") {
		t.Fatal("root must show only command help and root flags", output.String())
	}

	for _, command := range []string{"serve", "check"} {
		output.Reset()
		if err := run([]string{command, "--help"}, &output); err != nil ||
			!strings.Contains(output.String(), `-c, --config string`) ||
			!strings.Contains(output.String(), `(default "weir.yaml")`) ||
			!strings.Contains(output.String(), "basic YAML configuration file") ||
			!strings.Contains(output.String(), "--routes string") ||
			!strings.Contains(output.String(), "optional routing YAML configuration file") {
			t.Fatal("command help must document basic configuration and optional routes", command, err, output.String())
		}
	}
}

func TestCLIRequiresBasicConfigurationAndAllowsOptionalRoutes(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, command := range []string{"serve", "check"} {
		var output bytes.Buffer
		if err := run([]string{command}, &output); err == nil || err.Error() != "basic configuration unavailable" || output.Len() != 0 {
			t.Fatal("command must require default weir.yaml before opening a backend", command, err, output.String())
		}
	}

	basic := `listeners:
  application: 127.0.0.1:0
`
	if err := os.WriteFile("weir.yaml", []byte(basic), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("routes.yaml", []byte("unknown: ignored-sentinel\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"check"}, {"check", "--routes", ""}, {"check", "--routes="}} {
		var output bytes.Buffer
		if err := run(args, &output); err != nil || output.String() != "configuration valid\n" {
			t.Fatal("basic-only check must ignore a default routes.yaml", args, err, output.String())
		}
	}

	for _, command := range []string{"serve", "check"} {
		var output bytes.Buffer
		if err := run([]string{command, "--routes", "missing.yaml"}, &output); err == nil || err.Error() != "routing configuration unavailable" || output.Len() != 0 {
			t.Fatal("an explicit routing file must exist", command, err, output.String())
		}
		if err := run([]string{command, "--routes", "routes.yaml"}, &output); err == nil || err.Error() != "routing invalid configuration YAML or unknown field" || output.Len() != 0 {
			t.Fatal("an explicit routing file must be validated before startup", command, err, output.String())
		}
	}
}

func TestCLIRejectsRoutingReferenceInBasicConfiguration(t *testing.T) {
	t.Chdir(t.TempDir())
	basic := `listeners:
  application: 127.0.0.1:0
routing:
  file: routes-sentinel.yaml
`
	if err := os.WriteFile("weir.yaml", []byte(basic), 0600); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"serve", "check"} {
		var output bytes.Buffer
		err := run([]string{command}, &output)
		if err == nil || err.Error() != "basic invalid configuration YAML or unknown field" || output.Len() != 0 {
			t.Fatal("removed basic routing field must fail before startup", command, err, output.String())
		}
	}
}

func TestCLIConfigurationPathsAreRelativeToWorkingDirectory(t *testing.T) {
	directory := t.TempDir()
	basicDirectory := filepath.Join(directory, "nodes")
	routingDirectory := filepath.Join(directory, "routing")
	for _, path := range []string{basicDirectory, routingDirectory} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	basic := `listeners:
  application: 192.0.2.1:7447
`
	routing := `services:
  - name: remote
    remote:
      endpoints:
        - unresolved.invalid:7448
      max_concurrency: 1
routes:
  - store: records
    service: remote
`
	if err := os.WriteFile(filepath.Join(basicDirectory, "weir.yaml"), []byte(basic), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(routingDirectory, "routes.yaml"), []byte(routing), 0600); err != nil {
		t.Fatal(err)
	}

	t.Chdir(directory)
	var output bytes.Buffer
	err := run([]string{"check", "--config", "nodes/weir.yaml", "--routes", "routing/routes.yaml"}, &output)
	if err != nil || output.String() != "configuration valid\n" {
		t.Fatal("both CLI file paths must resolve independently from the working directory", err, output.String())
	}

	t.Chdir(basicDirectory)
	output.Reset()
	err = run([]string{"check", "--routes", "routing/routes.yaml"}, &output)
	if err == nil || err.Error() != "routing configuration unavailable" || output.Len() != 0 {
		t.Fatal("relative routes must follow the current working directory", err, output.String())
	}

	output.Reset()
	err = run([]string{"check", "--routes", "../routing/routes.yaml"}, &output)
	if err != nil || output.String() != "configuration valid\n" {
		t.Fatal("relative routes outside the basic directory must be accepted", err, output.String())
	}
}

func TestCLIHasNoConfigurationFallback(t *testing.T) {
	for _, filename := range []string{filepath.Join("config", "weir.yaml"), "weir.json"} {
		t.Run(filename, func(t *testing.T) {
			t.Chdir(t.TempDir())
			basic := `listeners:
  application: 127.0.0.1:0
`
			if filename == "weir.json" {
				basic = `{"listeners":{"application":"127.0.0.1:0"}}`
			}
			routing := `services:
  - name: remote
    remote:
      endpoints:
        - unresolved.invalid:7448
      max_concurrency: 1
routes:
  - store: records
    service: remote
`
			if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filename, []byte(basic), 0600); err != nil {
				t.Fatal(err)
			}
			routingFile := filepath.Join(filepath.Dir(filename), "routes.yaml")
			if err := os.WriteFile(routingFile, []byte(routing), 0600); err != nil {
				t.Fatal(err)
			}

			for _, command := range []string{"serve", "check"} {
				var output bytes.Buffer
				err := run([]string{command}, &output)
				if err == nil || err.Error() != "basic configuration unavailable" || output.Len() != 0 {
					t.Fatal("default configuration must not discover files other than ./weir.yaml", command, filename, err, output.String())
				}
			}
		})
	}
}

func TestCLIRejectsArgumentsBeforeConfiguration(t *testing.T) {
	t.Chdir(t.TempDir())
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	cases := [][]string{
		{"unknown"}, {"completion"}, {"extra"}, {"version", "extra"},
		{"serve", "--config", ""}, {"check", "-c", ""},
		{"serve", "extra"}, {"check", "extra"},
		{"--config", missing}, {"--address", "127.0.0.1:1"},
		{"--routes", missing},
		{"version", "--config", missing}, {"version", "-c", missing},
		{"version", "--routes", missing},
		{"version", "--address", "127.0.0.1:1"},
		{"serve", "--address", "127.0.0.1:1"}, {"check", "--address", "127.0.0.1:1"},
		{"probe", "live", "--config", missing}, {"probe", "live", "-c", missing},
		{"probe", "live", "--routes", missing},
		{"serve", "-r", missing}, {"check", "-r", missing},
		{"serve", "--config", missing, "--database", "unexpected"},
		{"-version"}, {"-config", missing}, {"-check-config", missing}, {"-probe", "live"},
		{"serve", "-config", missing}, {"check", "-check-config", missing},
		{"version", "-version"}, {"probe", "-probe", "live"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var output bytes.Buffer
			err := run(args, &output)
			if err == nil || strings.Contains(err.Error(), "configuration unavailable") || output.Len() != 0 {
				t.Fatal("expected quiet argument rejection before config access", err, output.String())
			}
		})
	}
}

func TestCLICommandStateIsFresh(t *testing.T) {
	directory := t.TempDir()
	t.Chdir(directory)
	basic := `listeners:
  application: 192.0.2.1:7447
`
	routing := `services:
  - name: remote
    remote:
      endpoints:
        - unresolved.invalid:7448
      max_concurrency: 1
routes:
  - store: records
    service: remote
`
	if err := os.WriteFile("weir.yaml", []byte(basic), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("routes.yaml", []byte(routing), 0600); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := run([]string{"check", "--config", "missing.yaml"}, &output); err == nil {
		t.Fatal("explicit missing file accepted")
	}
	output.Reset()
	if err := run([]string{"check", "--routes", "routes.yaml"}, &output); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("routes.yaml", []byte("unknown: ignored-sentinel\n"), 0600); err != nil {
		t.Fatal(err)
	}

	for range 2 {
		output.Reset()
		if err := run([]string{"check"}, &output); err != nil || output.String() != "configuration valid\n" {
			t.Fatal("configuration or routing flag state leaked between executions", err, output.String())
		}
		output.Reset()
		if err := run([]string{"version"}, &output); err != nil || !json.Valid(output.Bytes()) {
			t.Fatal("previous command state affected version", err, output.String())
		}
	}
}
