package overload

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProfileUnrelatedRecordsAndOrder(t *testing.T) {
	f := fixture(t)
	state := Snapshot{Budget: 1 << 20}
	g := &Guard{state: state}
	mount := fmt.Sprintf("1 0 0:1 / %s rw - cgroup2 cgroup rw\n", f.mount)
	unrelated := "99 0 0:99 / /unrelated rw - tmpfs tmpfs rw\n"
	// Same coverage, different mount point: selection must not depend on line order.
	alternate := fmt.Sprintf("2 0 0:1 / %s-z rw - cgroup2 cgroup rw\n", f.mount)
	narrower := fmt.Sprintf("3 0 0:1 /a %s/a rw - cgroup2 cgroup rw\n", f.mount)
	for _, raw := range []string{mount, unrelated + alternate + mount + narrower, narrower + mount + alternate + unrelated, mount} {
		f.write(t, filepath.Join(f.proc, "mountinfo"), raw)
		f.write(t, filepath.Join(f.proc, "cgroup"), "7:cpu:/unrelated\n0::/a/leaf\n")
		g.sample(f.profile.observe())
		if s := g.Snapshot(); s.Unknown || s.Latched || s.Cgroup.State != "v2" {
			t.Fatal("unrelated records changed profile", s)
		}
	}
	f.write(t, filepath.Join(f.proc, "cgroup"), "0::/a/leaf\n")
	g.sample(f.profile.observe())
	if s := g.Snapshot(); s.Unknown || s.Latched {
		t.Fatal(s)
	}
}

func TestProfileTransientTopologyRecovery(t *testing.T) {
	for _, field := range []string{"mountinfo", "cgroup"} {
		for _, failure := range []string{"malformed", "oversized", "missing", "overflow"} {
			t.Run(field+"/"+failure, func(t *testing.T) {
				f := fixture(t)
				file := filepath.Join(f.proc, field)
				original, err := os.ReadFile(file)
				if err != nil {
					t.Fatal(err)
				}
				state := Snapshot{Budget: 1 << 20}
				g := &Guard{state: state}
				for _, current := range []string{"100", "850", "750"} {
					f.level(t, "a", "1000", current)
					g.sample(f.profile.observe())
				}
				switch failure {
				case "missing":
					if err := os.Remove(file); err != nil {
						t.Fatal(err)
					}
				case "malformed":
					f.write(t, file, "temporarily invalid topology")
				case "oversized":
					f.write(t, file, strings.Repeat("x", maxMountBytes+1))
				case "overflow":
					f.write(t, file, strings.Replace(string(original), "0:", "18446744073709551616:", 1))
				}
				g.sample(f.profile.observe())
				if s := g.Snapshot(); !s.Unknown || !s.Latched || s.Cgroup.State != "unknown" {
					t.Fatal("invalid observation must be transient unknown", s)
				}
				f.write(t, file, string(original))
				g.sample(f.profile.observe())
				if s := g.Snapshot(); s.Unknown || !s.Latched {
					t.Fatal("middle must remain latched", s)
				}
				f.level(t, "a", "1000", "700")
				// Low cgroup alone cannot release an invalid or middle RSS.
				f.write(t, filepath.Join(f.proc, "statm"), "bad")
				g.sample(f.profile.observe())
				if s := g.Snapshot(); !s.Unknown || !s.Latched {
					t.Fatal(s)
				}
				pages := (uint64(1<<20) * 75 / 100) / uint64(os.Getpagesize())
				f.write(t, filepath.Join(f.proc, "statm"), fmt.Sprintf("100 %d 0 0 0 0 0", pages))
				g.sample(f.profile.observe())
				if s := g.Snapshot(); s.Unknown || !s.Latched {
					t.Fatal(s)
				}
				f.write(t, filepath.Join(f.proc, "statm"), "100 2 0 0 0 0 0")
				for range 3 {
					g.sample(f.profile.observe())
				}
				if s := g.Snapshot(); s.Unknown || s.Latched {
					t.Fatal("trusted original profile did not recover", s)
				}
			})
		}
	}
}

func TestProfileValidatedIdentityChange(t *testing.T) {
	for _, change := range []string{"mount_id", "device", "mapping", "migration", "limit"} {
		t.Run(change, func(t *testing.T) {
			f := fixture(t)
			original := fmt.Sprintf("1 0 0:1 / %s rw - cgroup2 cgroup rw\n", f.mount)
			if o := f.profile.observe(); !o.cgroup.Valid {
				t.Fatal(o)
			}
			switch change {
			case "mount_id":
				f.write(t, filepath.Join(f.proc, "mountinfo"), strings.Replace(original, "1 0", "8 0", 1))
			case "device":
				f.write(t, filepath.Join(f.proc, "mountinfo"), strings.Replace(original, "0:1", "0:2", 1))
			case "mapping":
				f.write(t, filepath.Join(f.proc, "mountinfo"), fmt.Sprintf("1 0 0:1 /a %s/a rw - cgroup2 cgroup rw\n", f.mount))
			case "migration":
				f.write(t, filepath.Join(f.proc, "cgroup"), "0::/a\n")
			case "limit":
				f.level(t, "a", "2000", "100")
			}
			// Incomplete observations must not commit a permanent identity change.
			current := filepath.Join(f.mount, "a", "memory.current")
			f.write(t, current, "bad")
			if o := f.profile.observe(); o.cgroup.State != "unknown" {
				t.Fatal(o)
			}
			f.write(t, current, "100")
			if o := f.profile.observe(); o.cgroup.State != "profile_changed" {
				t.Fatal(o)
			}
			f.write(t, filepath.Join(f.proc, "mountinfo"), original)
			f.write(t, filepath.Join(f.proc, "cgroup"), "0::/a/leaf\n")
			f.level(t, "a", "1000", "100")
			if o := f.profile.observe(); o.cgroup.State != "profile_changed" || o.low {
				t.Fatal("confirmed change cannot be undone", o)
			}
		})
	}
}

func TestProfileStartupTopologyUnknown(t *testing.T) {
	f := fixture(t)
	mount := fmt.Sprintf("1 0 0:1 / %s rw - cgroup2 cgroup rw\n", f.mount)
	f.write(t, filepath.Join(f.proc, "mountinfo"), "bad")
	state := Snapshot{Budget: 1 << 20}
	g := &Guard{state: state}
	g.sample(f.profile.observe())
	if s := g.Snapshot(); !s.Unknown || !s.Latched {
		t.Fatal(s)
	}
	f.write(t, filepath.Join(f.proc, "mountinfo"), mount)
	f.level(t, "a", "1000", "750")
	g.sample(f.profile.observe())
	if s := g.Snapshot(); s.Unknown || !s.Latched {
		t.Fatal(s)
	}
	f.level(t, "a", "1000", "700")
	g.sample(f.profile.observe())
	if s := g.Snapshot(); s.Unknown || s.Latched {
		t.Fatal(s)
	}
}

func TestProfileLowMalformedRestored(t *testing.T) {
	f := fixture(t)
	original, err := os.ReadFile(filepath.Join(f.proc, "mountinfo"))
	if err != nil {
		t.Fatal(err)
	}
	state := Snapshot{Budget: 1 << 20}
	g := &Guard{state: state}
	g.sample(f.profile.observe())
	if s := g.Snapshot(); s.Unknown || s.Latched {
		t.Fatal(s)
	}
	f.write(t, filepath.Join(f.proc, "mountinfo"), "temporarily invalid topology")
	g.sample(f.profile.observe())
	if s := g.Snapshot(); !s.Unknown || !s.Latched {
		t.Fatal(s)
	}
	f.write(t, filepath.Join(f.proc, "mountinfo"), string(original))
	for range 3 {
		g.sample(f.profile.observe())
	}
	if s := g.Snapshot(); s.Unknown || s.Latched {
		t.Fatal("original low profile must recover", s)
	}
}

func TestProfileParserBounds(t *testing.T) {
	mount := "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n"
	leaf := "/" + strings.Repeat("a/", maxLevels-2) + "a"
	if _, names, err := locateCgroup("0::"+leaf, mount); err != nil || len(names) != maxLevels {
		t.Fatal(names, err)
	}
	if _, _, err := locateCgroup("0::"+leaf+"/a", mount); err == nil {
		t.Fatal("too deep")
	}
	fields64 := strings.Replace(mount, " - ", " "+strings.Repeat("shared:1 ", 54)+"- ", 1)
	if _, _, err := locateCgroup("0::/", fields64); err != nil {
		t.Fatal(err)
	}
	if _, _, err := locateCgroup("0::/", strings.Replace(fields64, " - ", " shared:1 - ", 1)); err == nil {
		t.Fatal("too many fields")
	}
	for _, raw := range []string{
		strings.Replace(mount, "1 0", "18446744073709551616 0", 1),
		strings.Replace(mount, "1 0", "1 bad", 1),
		strings.Replace(mount, "0:1", "0:18446744073709551616", 1),
		strings.Replace(mount, "0:1", "0:1:2", 1),
		mount + mount,
		mount + strings.Replace(mount, "1 0", "2 1", 1),
	} {
		if _, _, err := locateCgroup("0::/", raw); err == nil {
			t.Fatal("invalid mount accepted", raw)
		}
	}
	membership := "0::/\n" + strings.Repeat("1:cpu:/a\n", 63)
	if _, _, err := locateCgroup(membership, mount); err != nil {
		t.Fatal(err)
	}
	if _, _, err := locateCgroup(membership+"2:io:/b\n", mount); err == nil {
		t.Fatal("too many memberships")
	}
}

func TestProfileMissingTopPairRecovers(t *testing.T) {
	f := fixture(t)
	f.level(t, ".", "2000", "100")
	if o := f.profile.observe(); !o.cgroup.Valid {
		t.Fatal(o)
	}
	for _, name := range []string{"memory.current", "memory.max"} {
		if err := os.Remove(filepath.Join(f.mount, name)); err != nil {
			t.Fatal(err)
		}
	}
	if o := f.profile.observe(); o.cgroup.State != "unknown" {
		t.Fatal("missing pair must be transient", o)
	}
	f.level(t, ".", "2000", "100")
	if o := f.profile.observe(); !o.cgroup.Valid || !o.low {
		t.Fatal(o)
	}
}
