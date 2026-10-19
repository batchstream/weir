package main

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func controlsFixture(t *testing.T) controlsOptions {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"proc/1/cgroup": "0::/\n", "proc/self/cgroup": "0::/\n",
		"proc/self/mountinfo":   "1 0 0:1 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw\n",
		"sys/fs/cgroup/cpu.max": "200000 100000\n", "sys/fs/cgroup/memory.max": "1073741824\n", "sys/fs/cgroup/memory.swap.max": "0\n", "sys/fs/cgroup/pids.max": "96\n",
		"sys/fs/cgroup/memory.current": "123\n", "sys/fs/cgroup/memory.peak": "456\n",
		"sys/fs/cgroup/cpu.stat":      "usage_usec 2\nnr_periods 1\nnr_throttled 0\nthrottled_usec 0\n",
		"sys/fs/cgroup/memory.events": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n",
		"target":                      "known target executable", "observer": "known observer executable",
	}
	fields := make([]string, 20)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0] = "S"
	fields[19] = "1234"
	files["proc/1/stat"] = "1 (process with spaces) " + strings.Join(fields, " ") + "\n"
	for name, raw := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(raw), 0644); err != nil {
			t.Fatal(err)
		}
	}
	for _, ns := range []string{"cgroup", "pid"} {
		a, b := filepath.Join(root, "proc/1/ns", ns), filepath.Join(root, "proc/self/ns", ns)
		if err := os.MkdirAll(filepath.Dir(a), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(b), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(a, []byte("namespace"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(a, b); err != nil {
			t.Fatal(err)
		}
	}
	for target, link := range map[string]string{"target": "proc/1/exe", "observer": "proc/self/exe"} {
		if err := os.Symlink(filepath.Join(root, target), filepath.Join(root, link)); err != nil {
			t.Fatal(err)
		}
	}
	sha := fmt.Sprintf("%x", sha256.Sum256([]byte(files["target"])))
	opts := controlsOptions{Root: root, PID: 1, ExpectedSHA256: sha}
	return opts
}

func TestControlsOnceSnapshot(t *testing.T) {
	opts := controlsFixture(t)
	sample, err := collectControls(opts)
	if err != nil {
		t.Fatal(err)
	}
	if sample.StartTicks != 1234 || sample.PID != 1 || sample.SHA256 != opts.ExpectedSHA256 || sample.Memory != 1<<30 || sample.Swap != 0 || sample.PIDs != 96 || sample.CPU != "200000 100000" || sample.Membership != "0::/" || sample.CPUCounters["usage_usec"] != 2 || sample.ObserverSHA256 == "" {
		t.Fatal("missing actual controls/process snapshot", sample)
	}
	opts.IdentityOnly = true
	opts.ExpectedSHA256 = ""
	sample, err = collectControls(opts)
	if err != nil || sample.SHA256 == "" || sample.CPU != "" {
		t.Fatal("identity-only must establish bytes without control reads", sample, err)
	}
}

func TestControlsOnceRejectsInvalidObservations(t *testing.T) {
	cases := map[string]struct{ Path, Value string }{
		"host PID membership":  {"proc/1/cgroup", "0::/docker/other"},
		"observer membership":  {"proc/self/cgroup", "0::/other"},
		"v1":                   {"proc/1/cgroup", "1:cpu:/"},
		"missing mount":        {"proc/self/mountinfo", "1 0 0:1 / /tmp ro - tmpfs tmpfs rw"},
		"wrong mount root":     {"proc/self/mountinfo", "1 0 0:1 /host /sys/fs/cgroup ro - cgroup2 cgroup rw"},
		"writable mount":       {"proc/self/mountinfo", "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw"},
		"malformed mount":      {"proc/self/mountinfo", "truncated"},
		"unlimited CPU":        {"sys/fs/cgroup/cpu.max", "max 100000"},
		"wrong CPU":            {"sys/fs/cgroup/cpu.max", "100000 100000"},
		"fractional CPU":       {"sys/fs/cgroup/cpu.max", "200001 100000"},
		"zero CPU period":      {"sys/fs/cgroup/cpu.max", "200000 0"},
		"malformed CPU":        {"sys/fs/cgroup/cpu.max", "200000 100000 extra"},
		"unlimited memory":     {"sys/fs/cgroup/memory.max", "max"},
		"negative memory":      {"sys/fs/cgroup/memory.max", "-1"},
		"extra swap":           {"sys/fs/cgroup/memory.swap.max", "1"},
		"unlimited PIDs":       {"sys/fs/cgroup/pids.max", "max"},
		"zero PIDs":            {"sys/fs/cgroup/pids.max", "0"},
		"missing CPU counters": {"sys/fs/cgroup/cpu.stat", "usage_usec 1"},
		"duplicate counters":   {"sys/fs/cgroup/cpu.stat", "usage_usec 1\nusage_usec 1"},
		"malformed counters":   {"sys/fs/cgroup/memory.events", "oom secret value"},
		"negative counters":    {"sys/fs/cgroup/memory.events", "oom -1"},
		"missing OOM":          {"sys/fs/cgroup/memory.events", "low 0"},
		"bad stat":             {"proc/1/stat", "bad"},
		"observation bound":    {"sys/fs/cgroup/cpu.max", strings.Repeat("x", (64<<10)+1)},
	}
	for name, item := range cases {
		t.Run(name, func(t *testing.T) {
			opts := controlsFixture(t)
			if err := os.WriteFile(filepath.Join(opts.Root, item.Path), []byte(item.Value), 0644); err != nil {
				t.Fatal(err)
			}
			if _, err := collectControls(opts); err == nil {
				t.Fatal("accepted invalid observation")
			}
		})
	}
	t.Run("digest", func(t *testing.T) {
		opts := controlsFixture(t)
		opts.ExpectedSHA256 = strings.Repeat("0", 64)
		if _, err := collectControls(opts); err == nil {
			t.Fatal("accepted wrong PID1 bytes")
		}
	})
	t.Run("PID", func(t *testing.T) {
		opts := controlsFixture(t)
		opts.PID = 123
		if _, err := collectControls(opts); err == nil {
			t.Fatal("accepted host PID")
		}
	})
	t.Run("missing", func(t *testing.T) {
		opts := controlsFixture(t)
		if err := os.Remove(filepath.Join(opts.Root, "sys/fs/cgroup/memory.max")); err != nil {
			t.Fatal(err)
		}
		if _, err := collectControls(opts); err == nil {
			t.Fatal("accepted absent control")
		}
	})
	t.Run("namespace", func(t *testing.T) {
		opts := controlsFixture(t)
		path := filepath.Join(opts.Root, "proc/self/ns/cgroup")
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("other"), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := collectControls(opts); err == nil {
			t.Fatal("accepted namespace mismatch")
		}
	})
}
