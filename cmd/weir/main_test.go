package main

import (
	"bytes"
	"encoding/json"
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
