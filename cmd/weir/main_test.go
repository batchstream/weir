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

func TestCheckConfigWithoutBackendOrCAAccess(t *testing.T) {
	directory := t.TempDir()
	file := filepath.Join(directory, "node.json")
	routingDirectory := filepath.Join(directory, "routing")
	if err := os.Mkdir(routingDirectory, 0700); err != nil {
		t.Fatal(err)
	}

	routingFile := filepath.Join(routingDirectory, "routes.json")
	basic := `{"listeners":{"application":"192.0.2.1:7447"},"routing":{"file":"routing/routes.json"}}`
	routing := `{"services":[{"name":"database","local":{"mongodb":{"uri":"mongodb://user:password-sentinel@unresolved.invalid:27017/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tlsCAFile=%2Fmissing%2Fca.pem","database":"catalog","collection":"records"}}}],"routes":[{"store":"mongo","service":"database"}]}`
	if err := os.WriteFile(file, []byte(basic), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(routingFile, []byte(routing), 0600); err != nil {
		t.Fatal(err)
	}

	// Routing paths belong to the basic file, even from a different working directory.
	t.Chdir(t.TempDir())

	var output bytes.Buffer
	for _, flag := range []string{"--config", "-c"} {
		output.Reset()
		if err := run([]string{"check", flag, file}, &output); err != nil {
			t.Fatal(err)
		}
		if output.String() != "configuration valid\n" {
			t.Fatal("validation output must not reveal configuration", output.String())
		}
	}

	invalid := strings.Replace(routing, `"collection":"records"`, `"collection":"invalid name"`, 1)
	if err := os.WriteFile(routingFile, []byte(invalid), 0600); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"check", "serve"} {
		output.Reset()
		err := run([]string{command, "--config", file}, &output)
		if err == nil || strings.Contains(err.Error(), "password-sentinel") || output.Len() != 0 {
			t.Fatal("invalid routing configuration must fail without exposing values", command, err)
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
		strings.Contains(output.String(), "--address") {
		t.Fatal("root must show only command help and root flags", output.String())
	}

	output.Reset()
	if err := run([]string{"serve", "--help"}, &output); err != nil ||
		!strings.Contains(output.String(), `-c, --config string`) ||
		!strings.Contains(output.String(), `(default "weir.json")`) {
		t.Fatal("serve help must document the local configuration flag", err, output.String())
	}
}

func TestCLIRequiresBasicAndRoutingFiles(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, command := range []string{"serve", "check"} {
		var output bytes.Buffer
		if err := run([]string{command}, &output); err == nil || err.Error() != "basic configuration unavailable" || output.Len() != 0 {
			t.Fatal("command must require default weir.json before opening a backend", command, err, output.String())
		}
	}

	basic := `{"listeners":{"application":"127.0.0.1:0"},"routing":{"file":"routes.json"}}`
	if err := os.WriteFile("weir.json", []byte(basic), 0600); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"serve", "check"} {
		var output bytes.Buffer
		if err := run([]string{command}, &output); err == nil || err.Error() != "routing configuration unavailable" || output.Len() != 0 {
			t.Fatal("command must load weir.json and its referenced routing file", command, err, output.String())
		}
	}
}

func TestCLIRejectsArgumentsBeforeConfiguration(t *testing.T) {
	t.Chdir(t.TempDir())
	missing := filepath.Join(t.TempDir(), "missing.json")
	cases := [][]string{
		{"unknown"}, {"completion"}, {"extra"}, {"version", "extra"},
		{"serve", "--config", ""}, {"check", "-c", ""},
		{"serve", "extra"}, {"check", "extra"},
		{"--config", missing}, {"--address", "127.0.0.1:1"},
		{"version", "--config", missing}, {"version", "-c", missing},
		{"version", "--address", "127.0.0.1:1"},
		{"serve", "--address", "127.0.0.1:1"}, {"check", "--address", "127.0.0.1:1"},
		{"probe", "live", "--config", missing}, {"probe", "live", "-c", missing},
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
	basic := `{"listeners":{"application":"192.0.2.1:7447"},"routing":{"file":"routes.json"}}`
	routing := `{"services":[{"name":"remote","remote":{"endpoints":["unresolved.invalid:7448"],"max_concurrency":1}}],"routes":[{"store":"records","service":"remote"}]}`
	if err := os.WriteFile("weir.json", []byte(basic), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("routes.json", []byte(routing), 0600); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := run([]string{"check", "--config", "missing.json"}, &output); err == nil {
		t.Fatal("explicit missing file accepted")
	}

	for range 2 {
		output.Reset()
		if err := run([]string{"check"}, &output); err != nil || output.String() != "configuration valid\n" {
			t.Fatal("configuration flag or command state leaked between executions", err, output.String())
		}
		output.Reset()
		if err := run([]string{"version"}, &output); err != nil || !json.Valid(output.Bytes()) {
			t.Fatal("previous command state affected version", err, output.String())
		}
	}
}
