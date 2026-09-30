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
	if err := run([]string{"-version"}, &output); err != nil {
		t.Fatal(err)
	}
	var identity map[string]string
	if err := json.Unmarshal(output.Bytes(), &identity); err != nil {
		t.Fatal(err)
	}
	if identity["product"] != "weir" || identity["go"] != runtime.Version() || identity["target"] != runtime.GOOS+"/"+runtime.GOARCH || identity["state"] != "dev" {
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
	basic := `{"application":"192.0.2.1:7447","routing_file":"routing/routes.json"}`
	routing := `{"services":[{"name":"database","local":{"mongo":{"uri":"mongodb://user:password-sentinel@unresolved.invalid:27017/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tlsCAFile=%2Fmissing%2Fca.pem","database":"catalog","collection":"records"}}}],"routes":[{"store":"mongo","service":"database"}]}`
	if err := os.WriteFile(file, []byte(basic), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(routingFile, []byte(routing), 0600); err != nil {
		t.Fatal(err)
	}
	// Routing paths belong to the basic file, even from a different working directory.
	t.Chdir(t.TempDir())
	var output bytes.Buffer
	if err := run([]string{"-check-config", file}, &output); err != nil {
		t.Fatal(err)
	}
	if output.String() != "configuration valid\n" {
		t.Fatal("validation output must not reveal configuration", output.String())
	}
	invalid := strings.Replace(routing, `"collection":"records"`, `"collection":"invalid name"`, 1)
	if err := os.WriteFile(routingFile, []byte(invalid), 0600); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"-check-config", "-config"} {
		output.Reset()
		err := run([]string{mode, file}, &output)
		if err == nil || strings.Contains(err.Error(), "password-sentinel") || output.Len() != 0 {
			t.Fatal("invalid routing configuration must fail without exposing values", mode, err)
		}
	}
}

func TestHelpWithoutConfiguration(t *testing.T) {
	t.Chdir(t.TempDir())
	var output bytes.Buffer
	if err := run([]string{"-help"}, &output); err != nil || !strings.Contains(output.String(), "Usage of weir") {
		t.Fatal("help must short circuit server startup", err, output.String())
	}
	for _, name := range []string{"config", "check-config", "version", "probe", "probe-address"} {
		if !strings.Contains(output.String(), "\n  -"+name) {
			t.Fatal("missing supported flag", name, output.String())
		}
	}
	for _, name := range []string{"diagnostics", "listen", "mongo-uri", "database", "collection", "batch", "memory-mib", "search-url", "search-index", "search-profile"} {
		if strings.Contains(output.String(), "\n  -"+name) {
			t.Fatal("retained removed flag", name, output.String())
		}
	}
	if !strings.Contains(output.String(), `(default "weir.json")`) {
		t.Fatal("help must document the default basic configuration", output.String())
	}
}

func TestCLIRequiresBasicAndRoutingFiles(t *testing.T) {
	t.Chdir(t.TempDir())
	var output bytes.Buffer
	if err := run(nil, &output); err == nil || err.Error() != "basic configuration unavailable" || output.Len() != 0 {
		t.Fatal("default startup must require weir.json before opening a backend", err, output.String())
	}
	basic := `{"application":"127.0.0.1:0","routing_file":"routes.json"}`
	if err := os.WriteFile("weir.json", []byte(basic), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(nil, &output); err == nil || err.Error() != "routing configuration unavailable" || output.Len() != 0 {
		t.Fatal("default startup must load weir.json and its referenced routing file", err, output.String())
	}
}

func TestCLIRejectsArgumentsBeforeConfiguration(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.json")
	cases := [][]string{
		{"-unknown"}, {"-version=invalid"}, {"extra"}, {"-version", "extra"},
		{"-config", ""}, {"-version=false"}, {"-check-config", ""},
		{"-version", "-config", missing}, {"-config", missing, "-version=false"},
		{"-version", "-check-config", missing}, {"-check-config", missing, "-version=false"},
		{"-version", "-probe", "live"}, {"-version=false", "-probe-address", "127.0.0.1:1"},
		{"-check-config", missing, "-config", missing},
		{"-check-config", missing, "-probe", "live"},
		{"-check-config", missing, "-probe-address", "127.0.0.1:1"},
		{"-config", missing, "-probe", "live"},
		{"-config", missing, "-probe-address", "127.0.0.1:1"},
		{"-config", missing, "-database", "unexpected"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var output bytes.Buffer
			err := run(args, &output)
			if err == nil || strings.Contains(err.Error(), "configuration unavailable") {
				t.Fatal("expected argument rejection before config access", err)
			}
		})
	}
}
