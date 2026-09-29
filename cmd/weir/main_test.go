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
	file := filepath.Join(t.TempDir(), "node.json")
	config := `{"application":"192.0.2.1:7447","services":[{"name":"database","local":{"mongo":{"uri":"mongodb://user:password-sentinel@unresolved.invalid:27017/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tlsCAFile=%2Fmissing%2Fca.pem","database":"catalog","collection":"records"}}}],"routes":[{"store":"mongo","service":"database"}]}`
	if err := os.WriteFile(file, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run([]string{"-check-config", file}, &output); err != nil {
		t.Fatal(err)
	}
	if output.String() != "configuration valid\n" {
		t.Fatal("validation output must not reveal configuration", output.String())
	}
	invalid := strings.Replace(config, `"collection":"records"`, `"collection":"invalid name"`, 1)
	if err := os.WriteFile(file, []byte(invalid), 0600); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	err := run([]string{"-check-config", file}, &output)
	if err == nil || strings.Contains(err.Error(), "password-sentinel") || output.Len() != 0 {
		t.Fatal("invalid configuration must fail without exposing values", err)
	}
	for _, args := range [][]string{{"-check-config", ""}, {"-check-config", file, "-config", file}, {"-check-config", file, "-version=false"}, {"-check-config", file, "-probe", "live"}} {
		if err := run(args, &output); err == nil {
			t.Fatal("accepted mixed or empty check mode", args)
		}
	}
}

func TestHelpWithoutConfiguration(t *testing.T) {
	t.Chdir(t.TempDir())
	var output bytes.Buffer
	if err := run([]string{"-help"}, &output); err != nil || !strings.Contains(output.String(), "Usage of weir") {
		t.Fatal("help must short circuit server startup", err, output.String())
	}
}

func TestCLIRejectsArgumentsBeforeConfiguration(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.json")
	cases := [][]string{{"-unknown"}, {"-version=invalid"}, {"extra"}, {"-version", "extra"}, {"-version", "-config", missing}, {"-config", missing, "-database", "unexpected"}}
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
