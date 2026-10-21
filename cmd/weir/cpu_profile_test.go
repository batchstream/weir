package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/app"
)

func TestCPUProfileDisabledAndInvalid(t *testing.T) {
	disabled := cpuProfileOptions{}
	profile, err := startCPUProfile(disabled)
	if err != nil || profile != nil || profile.close() != nil {
		t.Fatal("disabled profiling must do nothing", err)
	}
	path := filepath.Join(t.TempDir(), "cpu.pprof")
	cases := []cpuProfileOptions{
		{path: path},
		{duration: time.Second},
		{path: path, duration: -time.Second},
		{path: path, duration: 61 * time.Second},
	}
	for _, options := range cases {
		if _, err := startCPUProfile(options); err == nil {
			t.Fatal("accepted invalid options", options)
		}
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("invalid options created output", err)
	}
	for _, args := range [][]string{
		{"serve", "--cpu-profile", path},
		{"serve", "--cpu-profile-duration", "1s"},
		{"serve", "--cpu-profile", path, "--cpu-profile-duration", "61s"},
	} {
		var output bytes.Buffer
		err := run(args, &output)
		if err == nil || !strings.Contains(err.Error(), "--cpu-profile") {
			t.Fatal("profile flags must fail before configuration load", err)
		}
	}
}

func TestCPUProfileNeverOverwrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu.pprof")
	original := []byte("preserve existing data")
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	options := cpuProfileOptions{path: path, duration: time.Second}
	if _, err := startCPUProfile(options); err == nil {
		t.Fatal("accepted existing file")
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, original) {
		t.Fatal("changed existing file", err)
	}
	link := filepath.Join(t.TempDir(), "link.pprof")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	options.path = link
	if _, err := startCPUProfile(options); err == nil {
		t.Fatal("accepted existing symlink")
	}
}

func TestCPUProfileServeExit(t *testing.T) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "weir.yaml")
	if err := os.WriteFile(configPath, []byte("listeners:\n  application: 127.0.0.1:0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := app.Load(configPath, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := filepath.Join(directory, "cpu.pprof")
	options := cpuProfileOptions{path: path, duration: 60 * time.Second}
	started := time.Now()
	_ = serve(ctx, cfg, io.Discard, options)
	if time.Since(started) > 5*time.Second {
		t.Fatal("serve exit waited for capture duration")
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() == 0 {
		t.Fatal("serve exit did not flush profile", err)
	}
	// A new session proves serve exit stopped the process-global profiler.
	options.path = filepath.Join(directory, "second.pprof")
	profile, err := startCPUProfile(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := profile.close(); err != nil {
		t.Fatal(err)
	}
}

func TestCPUProfileLifecycle(t *testing.T) {
	for _, early := range []bool{false, true} {
		name := "timer"
		if early {
			name = "early-close"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cpu.pprof")
			duration := 20 * time.Millisecond
			if early {
				duration = 60 * time.Second
			}
			options := cpuProfileOptions{path: path, duration: duration}
			profile, err := startCPUProfile(options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := profile.close(); err != nil {
					t.Error(err)
				}
			})
			if early {
				if err := profile.close(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-profile.done:
			case <-time.After(5 * time.Second):
				t.Fatal("profile worker did not stop")
			}
			if err := profile.close(); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != 0600 || info.Size() == 0 {
				t.Fatal("profile must be private and flushed", info)
			}
			command := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "tool", "pprof", "-top", path)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("parse actual CPU profile: %v\n%s", err, output)
			}
		})
	}
}
