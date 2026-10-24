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
		identity["version"] == "" {
		t.Fatal(identity)
	}
}

func TestVersionReportsInjectedRevision(t *testing.T) {
	injected := "0123456789abcdef0123456789abcdef01234567"
	previous := revision
	revision = injected
	t.Cleanup(func() { revision = previous })

	var output bytes.Buffer
	if err := run([]string{"version"}, &output); err != nil {
		t.Fatal(err)
	}

	var identity map[string]string
	if err := json.Unmarshal(output.Bytes(), &identity); err != nil {
		t.Fatal(err)
	}
	if identity["revision"] != injected {
		t.Fatal(identity)
	}
}

func TestCheckConfigWithCredentialFilesWithoutBackendOrCAAccess(t *testing.T) {
	directory := t.TempDir()
	configDirectory := filepath.Join(directory, "nested")
	valueDirectory := filepath.Join(configDirectory, "values")
	if err := os.MkdirAll(valueDirectory, 0700); err != nil {
		t.Fatal(err)
	}

	file := filepath.Join(configDirectory, "weir.yaml")
	usernameFile := filepath.Join(valueDirectory, "username")
	passwordFile := filepath.Join(valueDirectory, "password")
	document := `listeners:
  application: 192.0.2.1:7447
stores:
  - name: search
    search:
      url: https://unresolved.invalid:443
    authentication:
      username_file: values/username
      password_file: values/password
    tls:
      ca_file: /missing/ca-sentinel.pem
`
	if err := os.WriteFile(file, []byte(document), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(usernameFile, []byte("user"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(passwordFile, []byte("password-sentinel"), 0600); err != nil {
		t.Fatal(err)
	}

	// Credential paths belong to the configuration file, whatever the working directory.
	for _, working := range []string{directory, t.TempDir()} {
		t.Chdir(working)
		for _, flag := range []string{"--config", "-c"} {
			var output bytes.Buffer
			if err := run([]string{"check", flag, file}, &output); err != nil {
				t.Fatal(err)
			}
			if output.String() != "configuration valid\n" {
				t.Fatal("validation output must not reveal configuration", output.String())
			}
		}
	}

	t.Chdir(directory)
	invalidCases := []struct {
		document string
		reason   string
	}{
		{
			strings.Replace(document, "      username_file:", "      username: user-sentinel\n      username_file:", 1),
			"credential value and file are mutually exclusive",
		},
		{
			strings.Replace(document, "values/password", "values/missing-secret-sentinel", 1),
			"credential file unavailable",
		},
		{
			strings.Replace(document, "    search:", "    file: service-sentinel.yaml\n    search:", 1),
			"invalid configuration YAML or unknown field",
		},
	}
	for _, tc := range invalidCases {
		if err := os.WriteFile(file, []byte(tc.document), 0600); err != nil {
			t.Fatal(err)
		}
		for _, command := range []string{"check", "serve"} {
			var output bytes.Buffer
			err := run([]string{command, "--config", "nested/weir.yaml"}, &output)
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
	}
	for _, args := range cases {
		var output bytes.Buffer
		if err := run(args, &output); err != nil || !strings.Contains(output.String(), "Usage:") {
			t.Fatal("help must short circuit server startup", args, err, output.String())
		}
	}

	var output bytes.Buffer
	if err := run(nil, &output); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"serve", "check", "version"} {
		if !strings.Contains(output.String(), "  "+command+" ") {
			t.Fatal("missing supported command", command, output.String())
		}
	}
	if strings.Contains(output.String(), "completion") ||
		strings.Contains(output.String(), "--config") {
		t.Fatal("root must show only command help and root flags", output.String())
	}

	for _, command := range []string{"serve", "check"} {
		output.Reset()
		if err := run([]string{command, "--help"}, &output); err != nil ||
			!strings.Contains(output.String(), `-c, --config string`) ||
			!strings.Contains(output.String(), `(default "weir.yaml")`) ||
			!strings.Contains(output.String(), "YAML configuration file") {
			t.Fatal("command help must document the configuration file", command, err, output.String())
		}
	}
}

func TestCLIRequiresConfiguration(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, command := range []string{"serve", "check"} {
		var output bytes.Buffer
		if err := run([]string{command}, &output); err == nil || err.Error() != "configuration unavailable" || output.Len() != 0 {
			t.Fatal("command must require default weir.yaml before opening a backend", command, err, output.String())
		}
	}

	if err := os.WriteFile("weir.yaml", []byte("unknown: ignored-sentinel\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"serve", "check"} {
		var output bytes.Buffer
		if err := run([]string{command}, &output); err == nil || err.Error() != "invalid configuration YAML or unknown field" || output.Len() != 0 {
			t.Fatal("configuration must be validated before startup", command, err, output.String())
		}
	}

	if err := os.WriteFile("weir.yaml", []byte("listeners:\n  application: 127.0.0.1:0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run([]string{"check"}, &output); err != nil || output.String() != "configuration valid\n" {
		t.Fatal("a Store-less configuration must be valid", err, output.String())
	}
}

func TestCLIConfigurationPathIsRelativeToWorkingDirectory(t *testing.T) {
	directory := t.TempDir()
	nodeDirectory := filepath.Join(directory, "nodes")
	if err := os.Mkdir(nodeDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	document := `listeners:
  application: 192.0.2.1:7447
stores: []
`
	if err := os.WriteFile(filepath.Join(nodeDirectory, "weir.yaml"), []byte(document), 0600); err != nil {
		t.Fatal(err)
	}

	t.Chdir(directory)
	var output bytes.Buffer
	err := run([]string{"check", "--config", "nodes/weir.yaml"}, &output)
	if err != nil || output.String() != "configuration valid\n" {
		t.Fatal("the configuration path must resolve from the working directory", err, output.String())
	}

	t.Chdir(nodeDirectory)
	output.Reset()
	err = run([]string{"check", "--config", "nodes/weir.yaml"}, &output)
	if err == nil || err.Error() != "configuration unavailable" || output.Len() != 0 {
		t.Fatal("a relative path must follow the current working directory", err, output.String())
	}
}

func TestCLIRejectsArgumentsBeforeConfiguration(t *testing.T) {
	t.Chdir(t.TempDir())
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	cases := [][]string{
		{"unknown"}, {"completion"}, {"version", "extra"},
		{"serve", "extra"}, {"check", "extra"},
		{"serve", "--config", ""}, {"check", "--config", ""},
		{"--config", missing},
		{"version", "--config", missing},
		{"check", "--config", missing, "--unknown-file", missing},
		{"serve", "--config", missing, "--unknown", "value"},
		{"-version"}, {"serve", "-config", missing},
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
	t.Chdir(t.TempDir())
	document := `listeners:
  application: 192.0.2.1:7447
`
	if err := os.WriteFile("weir.yaml", []byte(document), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("other.yaml", []byte("unknown: ignored-sentinel\n"), 0600); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := run([]string{"check", "--config", "other.yaml"}, &output); err == nil {
		t.Fatal("invalid explicit file accepted")
	}
	for range 2 {
		output.Reset()
		if err := run([]string{"check"}, &output); err != nil || output.String() != "configuration valid\n" {
			t.Fatal("configuration flag state leaked between executions", err, output.String())
		}
		output.Reset()
		if err := run([]string{"version"}, &output); err != nil || !json.Valid(output.Bytes()) {
			t.Fatal("previous command state affected version", err, output.String())
		}
	}
}
