//go:build linux

package main

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func seedPublic(t *testing.T, root string) {
	t.Helper()
	for _, name := range publicFiles {
		if err := os.WriteFile(filepath.Join(root, name), []byte("public test data\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPublicConfig(t *testing.T) {
	for _, kind := range []string{"valid", "missing", "empty", "oversized", "symlink", "directory", "fifo"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			seedPublic(t, root)
			path := filepath.Join(root, publicFiles[0])
			if kind != "valid" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "empty":
				if err := os.WriteFile(path, nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "oversized":
				if err := os.WriteFile(path, make([]byte, (1<<20)+1), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(filepath.Join(root, publicFiles[1]), path); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			}
			files, err := publicConfig(root)
			if kind == "valid" {
				if err != nil || len(files) != 3 {
					t.Fatalf("files=%v err=%v", files, err)
				}
			} else if err == nil {
				t.Fatal("unsafe config accepted")
			}
		})
	}
}

func TestOwnedWritesAndExclusiveCollision(t *testing.T) {
	root := t.TempDir()
	seedPublic(t, root)
	before, err := publicConfig(root)
	if err != nil {
		t.Fatal(err)
	}
	opts := validOptions()
	if err = roundTrip(root, opts); err != nil {
		t.Fatal(err)
	}
	after, err := publicConfig(root)
	if err != nil || !sameFiles(before, after) {
		t.Fatal("public inputs changed")
	}
	file, _, err := newOwned(root, "roundtrip", opts)
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	if err = roundTrip(root, opts); err == nil {
		t.Fatal("owned collision overwritten")
	}
}

func TestNonTmpfsFailsClosed(t *testing.T) {
	// Runner temporary storage may itself be tmpfs; it must not be an exact
	// separately-mounted16MiB restrictive config filesystem at this directory.
	if _, err := filesystem(t.TempDir(), false); err == nil {
		t.Fatal("ordinary temp directory accepted as fixture")
	}
}

func TestMountFacts(t *testing.T) {
	raw := "1 0 0:1 / / ro,relatime - overlay overlay ro\n" +
		"2 1 0:2 / /weir-volume-helper ro,relatime - ext4 /dev/public rw\n" +
		"3 1 0:123 / /held ro,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,size=16384k\n"
	proof := filesystemProof{Mount: "/held"}
	result, err := mountFacts(strings.TrimSpace(raw), true, proof)
	if err != nil || result.Device != "0:123" || !result.RootReadonly || !result.HelperReadonly || !result.NoSharedMemory {
		t.Fatalf("proof=%+v err=%v", result, err)
	}
	cases := []string{
		strings.Replace(raw, "/ / ro,", "/ / rw,", 1),
		strings.Replace(raw, "/weir-volume-helper ro,", "/weir-volume-helper rw,", 1),
		strings.Replace(raw, "ro,nosuid,nodev,noexec", "rw,nosuid,nodev,noexec", 1),
		strings.Replace(raw, "nodev,noexec,", "nodev,", 1),
		strings.Replace(raw, " - tmpfs tmpfs ", " - ext4 /dev/public ", 1),
		raw + "4 1 0:124 / /dev/shm rw,nosuid,nodev,noexec - tmpfs shm rw\n",
		raw + "4 1 0:123 / /held ro,nosuid,nodev,noexec - tmpfs tmpfs rw\n",
		"invalid",
	}
	for i, input := range cases {
		if _, err = mountFacts(strings.TrimSpace(input), true, proof); err == nil {
			t.Errorf("case %d accepted", i)
		}
	}
}

func TestStartTicks(t *testing.T) {
	raw := "1 (name with ) spaces) " + strings.Repeat("0 ", 19) + "123"
	ticks, err := startTicks(raw)
	if err != nil || ticks != 123 {
		t.Fatalf("ticks=%d err=%v", ticks, err)
	}
	for _, input := range []string{"bad", "1 (helper) 0", "1 (helper) " + strings.Repeat("0 ", 19) + "bad"} {
		if _, err := startTicks(input); err == nil {
			t.Fatal("invalid process stat accepted")
		}
	}
}

func TestLoadedSourceAdmission(t *testing.T) {
	file, err := os.Open("/proc/self/exe")
	if err != nil {
		t.Fatal(err)
	}
	sha, _, err := hashFile(file, 128<<20)
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	previous := sourceRevision
	sourceRevision = strings.Repeat("a", 40)
	defer func() { sourceRevision = previous }()
	opts := options{Mode: "identity", Revision: sourceRevision, Helper: sha}
	proof, err := loadedBuild(opts)
	if err != nil || proof.SHA != sha || proof.Revision != sourceRevision {
		t.Fatalf("proof=%+v err=%v", proof, err)
	}
	opts.Revision = strings.Repeat("b", 40)
	if _, err = loadedBuild(opts); err == nil {
		t.Fatal("wrong source admitted")
	}
	opts.Revision = sourceRevision
	opts.Helper = strings.Repeat("c", 64)
	if _, err = loadedBuild(opts); err == nil {
		t.Fatal("wrong loaded bytes admitted")
	}
	opts.Helper = sha
	opts.Owner = "fixture-operation"
	if _, err = loadedBuild(opts); err == nil {
		t.Fatal("identity mode accepted fixture operations")
	}
}
