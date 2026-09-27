package testutil

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRootLineEndingsAndDepth(t *testing.T) {
	endings := []struct {
		name    string
		newline string
	}{
		{name: "LF", newline: "\n"},
		{name: "CRLF", newline: "\r\n"},
		{name: "NoFinalNewline"},
	}
	for _, ending := range endings {
		for _, depth := range []int{0, 1, 6} {
			t.Run(fmt.Sprintf("%s/depth%d", ending.name, depth), func(t *testing.T) {
				root, err := filepath.EvalSymlinks(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				module := "module github.com/batchstream/weir" + ending.newline
				if ending.newline != "" {
					module += ending.newline + "go 1.27.0" + ending.newline
				}
				if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte(module), 0600); err != nil {
					t.Fatal(err)
				}
				nested := root
				for range depth {
					nested = filepath.Join(nested, "nested")
				}
				if err := os.MkdirAll(nested, 0700); err != nil {
					t.Fatal(err)
				}
				if depth > 0 {
					otherModule := "module github.com/batchstream/weir/nested" + ending.newline
					err := os.WriteFile(filepath.Join(nested, "go.mod"), []byte(otherModule), 0600)
					if err != nil {
						t.Fatal(err)
					}
				}
				t.Chdir(nested)
				if got := Root(t); got != root {
					t.Fatalf("Root() = %q, want %q", got, root)
				}
			})
		}
	}
}

func TestRootNotFound(t *testing.T) {
	// Use a subprocess to exercise Root's real Fatal path and bound the walk.
	if os.Getenv("WEIR_TEST_ROOT_NOT_FOUND") == "1" {
		Root(t)
		return
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		module string
	}{
		{name: "Missing"},
		{name: "Unrelated", module: "module example.com/other\n"},
		{name: "Prefix", module: "module github.com/batchstream/wei\n"},
		{name: "Suffix", module: "module github.com/batchstream/weir-extra\r\n"},
		{name: "Submodule", module: "module github.com/batchstream/weir/v2\r\n"},
		{name: "Embedded", module: "module example.com/github.com/batchstream/weir\n"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if test.module != "" {
				err := os.WriteFile(filepath.Join(root, "go.mod"), []byte(test.module), 0600)
				if err != nil {
					t.Fatal(err)
				}
			}
			nested := filepath.Join(root, "internal", "nested")
			if err := os.MkdirAll(nested, 0700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, binary, "-test.run=^TestRootNotFound$", "-test.v")
			command.Dir = nested
			command.Env = append(os.Environ(), "WEIR_TEST_ROOT_NOT_FOUND=1")
			output, err := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatal("Root did not stop at the filesystem root", ctx.Err())
			}
			var exitError *exec.ExitError
			if !errors.As(err, &exitError) || exitError.ExitCode() != 1 {
				t.Fatalf("expected test failure, got %v: %s", err, output)
			}
			if !strings.Contains(string(output), "test must run inside the Weir module") {
				t.Fatalf("missing Root failure diagnostic: %s", output)
			}
		})
	}
}
