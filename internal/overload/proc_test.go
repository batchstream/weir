package overload

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestProcParsers(t *testing.T) {
	for _, raw := range []string{"", "+1", "-1", "1 2", "18446744073709551616", "max", "1\x00"} {
		if _, err := parseNumber(raw); err == nil {
			t.Fatal(raw)
		}
	}
	if n, err := parseNumber("18446744073709551615\n"); err != nil || n != ^uint64(0) {
		t.Fatal(n, err)
	}
	if n, err := parseRSS("10 2 1 0 0 0 0\n", 4096); err != nil || n != 8192 {
		t.Fatal(n, err)
	}
	for _, raw := range []string{"1 2", "1 2 3 4 5 6 7 8", "1 18446744073709551615 0 0 0 0 0", "1 -1 0 0 0 0 0", "1 1 x 0 0 0 0"} {
		if _, err := parseRSS(raw, 4096); err == nil {
			t.Fatal(raw)
		}
	}
	mount := "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n"
	for _, leaf := range []string{"/", "/a", "/a/b"} {
		_, names, err := locateCgroup("0::"+leaf+"\n", mount)
		if err != nil || names[len(names)-1] != "." {
			t.Fatal(names, err)
		}
	}
	escaped := "2 0 0:1 /a /owned\\040mount rw shared:1 - cgroup2 cgroup rw\n"
	point, names, err := locateCgroup("0::/a/child", escaped)
	if err != nil || point.mount != "/owned mount" || strings.Join(names, ",") != "child,." {
		t.Fatal(point, names, err)
	}
	for _, member := range []string{"0::/../outside", "0::/a/../b", "0::relative", "0::/a//b", "0::/a (deleted)", "0::/\n0::/", "1:memory:/", "0::/ab", "0::/a/" + strings.Repeat("x/", 32) + "x"} {
		if _, _, err := locateCgroup(member, escaped); err == nil {
			t.Fatal(member)
		}
	}
	for _, raw := range []string{strings.ReplaceAll(mount, "/sys/fs/cgroup", "/bad\\077"), strings.ReplaceAll(mount, "/sys/fs/cgroup", "/../bad"), strings.Repeat(mount, 1025), strings.Repeat("x", maxMountBytes+1)} {
		if _, _, err := locateCgroup("0::/", raw); err == nil {
			t.Fatal("invalid mount accepted")
		}
	}
	// Multiple visible mounts: use the one with the broadest applicable root.
	point, names, err = locateCgroup("0::/a/b", escaped+mount)
	if err != nil || point.mount != "/sys/fs/cgroup" || len(names) != 3 {
		t.Fatal(point, names, err)
	}
}

type procFixture struct {
	proc, mount string
	profile     memoryProfile
}

func fixture(t *testing.T) *procFixture {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("owned POSIX /proc fixture; pure parser tests remain portable")
	}
	root := t.TempDir()
	f := &procFixture{proc: filepath.Join(root, "proc"), mount: filepath.Join(root, "cgroup")}
	for _, dir := range []string{f.proc, filepath.Join(f.mount, "a/leaf")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	f.write(t, filepath.Join(f.proc, "statm"), "100 2 1 0 0 0 0\n")
	f.write(t, filepath.Join(f.proc, "cgroup"), "0::/a/leaf\n")
	f.write(t, filepath.Join(f.proc, "mountinfo"), fmt.Sprintf("1 0 0:1 / %s rw - cgroup2 cgroup rw\n", f.mount))
	f.level(t, "a/leaf", "max", "100")
	f.level(t, "a", "1000", "100")
	// The true hierarchy root has no memory files.
	f.profile.proc = f.proc
	return f
}
func (f *procFixture) write(t *testing.T, name, value string) {
	t.Helper()
	if err := os.WriteFile(name, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
}
func (f *procFixture) level(t *testing.T, name, limit, current string) {
	t.Helper()
	f.write(t, filepath.Join(f.mount, name, "memory.max"), limit)
	f.write(t, filepath.Join(f.mount, name, "memory.current"), current)
}

func TestVisibleAncestorPressureAndRecovery(t *testing.T) {
	f := fixture(t)
	state := Snapshot{Budget: 1 << 20}
	guard := &Guard{state: state, processBudget: state.Budget}
	for _, step := range []struct {
		current string
		latched bool
	}{{"100", false}, {"800", true}, {"750", true}, {"700", false}} {
		f.level(t, "a", "1000", step.current)
		guard.sample(f.profile.observe())
		s := guard.Snapshot()
		if s.Unknown || s.Latched != step.latched || s.Cgroup.Limit != 1000 || s.Cgroup.Scope != "ancestor" || s.Cgroup.Levels != 3 {
			t.Fatal(s)
		}
	}
	// Finite leaf and ancestor must both be low, even if process RSS is tiny.
	f = fixture(t)
	f.level(t, "a/leaf", "500", "400")
	first := f.profile.observe()
	if !first.high || first.cgroup.Scope != "leaf" {
		t.Fatal(first)
	}
	f.level(t, "a/leaf", "500", "100")
	f.level(t, "a", "1000", "750")
	second := f.profile.observe()
	if second.high || second.low || second.cgroup.Current != 750 {
		t.Fatal(second)
	}
}

func TestProfileFailureAndDynamicChanges(t *testing.T) {
	for _, failure := range []string{"permission", "current_missing", "current_malformed", "current_overflow", "max_partial", "rss_missing", "rss_overflow", "oversized", "escape"} {
		t.Run(failure, func(t *testing.T) {
			f := fixture(t)
			f.level(t, "a", "1000", "850")
			state := Snapshot{Budget: 1 << 20}
			guard := &Guard{state: state, processBudget: state.Budget}
			guard.sample(f.profile.observe())
			if !guard.Snapshot().Latched {
				t.Fatal("initial pressure")
			}
			current := filepath.Join(f.mount, "a", "memory.current")
			switch failure {
			case "permission":
				if os.Geteuid() == 0 {
					t.Skip("root bypasses file permissions")
				}
				if err := os.Chmod(current, 0); err != nil {
					t.Fatal(err)
				}
			case "current_missing":
				if err := os.Remove(current); err != nil {
					t.Fatal(err)
				}
			case "current_malformed":
				f.write(t, current, "unknown")
			case "current_overflow":
				f.write(t, current, "18446744073709551616")
			case "max_partial":
				if err := os.Remove(filepath.Join(f.mount, "a", "memory.max")); err != nil {
					t.Fatal(err)
				}
			case "rss_missing":
				if err := os.Remove(filepath.Join(f.proc, "statm")); err != nil {
					t.Fatal(err)
				}
			case "rss_overflow":
				f.write(t, filepath.Join(f.proc, "statm"), "0 18446744073709551615 0 0 0 0 0")
			case "oversized":
				f.write(t, current, strings.Repeat("0", 65))
			case "escape":
				if err := os.Remove(current); err != nil {
					t.Fatal(err)
				}
				outside := filepath.Join(t.TempDir(), "outside")
				f.write(t, outside, "0")
				if err := os.Symlink(outside, current); err != nil {
					t.Fatal(err)
				}
			}
			guard.sample(f.profile.observe())
			if s := guard.Snapshot(); !s.Latched || !s.Unknown {
				t.Fatal(s)
			}
			if failure == "escape" {
				if err := os.Remove(current); err != nil {
					t.Fatal(err)
				}
			}
			if failure == "permission" {
				if err := os.Chmod(current, 0600); err != nil {
					t.Fatal(err)
				}
			}
			f.level(t, "a", "1000", "100")
			f.write(t, filepath.Join(f.proc, "statm"), "100 2 1 0 0 0 0")
			guard.sample(f.profile.observe())
			if s := guard.Snapshot(); s.Latched || s.Unknown {
				t.Fatal("trusted low observation must recover", s)
			}
		})
	}
	for _, change := range []string{"migration", "mount", "limit", "unlimited"} {
		t.Run(change, func(t *testing.T) {
			f := fixture(t)
			before := f.profile.observe()
			if !before.cgroup.Valid {
				t.Fatal(before)
			}
			switch change {
			case "migration":
				f.write(t, filepath.Join(f.proc, "cgroup"), "0::/a\n")
			case "mount":
				f.write(t, filepath.Join(f.proc, "mountinfo"), fmt.Sprintf("2 0 0:1 / %s rw - cgroup2 cgroup rw\n", f.mount))
			case "limit":
				f.level(t, "a", "2000", "0")
			case "unlimited":
				f.level(t, "a", "max", "0")
			}
			for range 2 {
				if o := f.profile.observe(); !o.cgroup.Valid || !o.low {
					t.Fatal(o)
				}
			}
		})
	}
}

func TestStartupUnknownUnlimitedAndZero(t *testing.T) {
	f := fixture(t)
	f.write(t, filepath.Join(f.proc, "statm"), "bad")
	o := f.profile.observe()
	if o.processValid || o.source != "go_sys_minus_released" {
		t.Fatal(o)
	}
	for _, limit := range []string{"max", "0"} {
		f := fixture(t)
		f.level(t, "a", limit, "0")
		o := f.profile.observe()
		if !o.cgroup.Valid || o.high != (limit == "0") || o.low != (limit == "max") || o.cgroup.Finite != (limit == "0") {
			t.Fatal(o)
		}
	}
	f = fixture(t)
	if err := os.Remove(filepath.Join(f.mount, "a/leaf", "memory.max")); err != nil {
		t.Fatal(err)
	}
	if o := f.profile.observe(); o.cgroup.State != "unknown" {
		t.Fatal(o)
	}
	f.level(t, "a/leaf", "max", "0")
	if o := f.profile.observe(); !o.cgroup.Valid {
		t.Fatal("initial transient failure did not recover", o)
	}
}
