package testexecution

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// The parent never renames its own image or the go tool's shared output. Each
// child runs a private executable copy in this test's owned temporary directory.
func TestExecutionProfileContractLoadedDriverProcess(t *testing.T) {
	if os.Getenv("WEIR_EXECUTION_LOADED_DRIVER_CHILD") == "1" {
		runLoadedDriverChild(t)
		return
	}
	if !((runtime.GOOS == "linux" && (runtime.GOARCH == "amd64" || runtime.GOARCH == "arm64")) || (runtime.GOOS == "darwin" && runtime.GOARCH == "arm64")) {
		t.Fatal("native loaded-driver positive contract unsupported on this platform")
	}
	current, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	current, err = filepath.EvalSymlinks(current)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := readFile(current, maxBinary)
	if err != nil {
		t.Fatal(err)
	}
	directory := fixtureRoot(t)
	driver := filepath.Join(directory, "owned-driver")
	fixtureWrite(t, driver, raw)
	owner := fmt.Sprintf("weir-loaded-driver-%d-%d", os.Getpid(), time.Now().UnixNano())
	marker := []byte(owner + "\n" + hash(raw))
	if err := os.WriteFile(filepath.Join(directory, ".weir-loaded-driver-owner"), marker, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, driver, "-test.run=^TestExecutionProfileContractLoadedDriverProcess$", "-test.v", "-test.timeout=80s")
	command.Env = append(os.Environ(), "WEIR_EXECUTION_LOADED_DRIVER_CHILD=1", "WEIR_EXECUTION_LOADED_DRIVER_DIRECTORY="+directory, "WEIR_EXECUTION_LOADED_DRIVER_OWNER="+owner)
	output, err := command.CombinedOutput()
	t.Logf("isolated native %s/%s driver regression:\n%s", runtime.GOOS, runtime.GOARCH, output)
	if err != nil {
		t.Fatal("isolated loaded-driver regression failed", err)
	}
	for _, marker := range []string{"baseline_valid=true", "renamed_loaded_inode_valid=true", "same_bytes_new_inode_rejected=true", "different_claimed_hash_rejected=true", "renamed_inode_with_replacement_valid=true", "restored_loaded_inode_valid=true", "unlinked_driver_path_rejected=true"} {
		if !strings.Contains(string(output), marker) {
			t.Fatal("child did not complete assertion", marker)
		}
	}
}
func requireDriverValidation(t *testing.T, profile ExecutionProfile, inputs ProvenanceInputs) {
	t.Helper()
	validated, err := Validate(profile, inputs)
	if err != nil || validated == nil {
		t.Fatal("actual kernel-backed native driver must validate", err)
	}
}
func requireDriverReplacementRejected(t *testing.T, profile ExecutionProfile, inputs ProvenanceInputs) {
	t.Helper()
	validated, err := Validate(profile, inputs)
	if err == nil || validated != nil || !strings.Contains(err.Error(), "kernel-loaded executable backing object") {
		t.Fatal("replacement did not fail at kernel loaded-image binding", err, validated != nil)
	}
}
func runLoadedDriverChild(t *testing.T) {
	t.Helper()
	directory := os.Getenv("WEIR_EXECUTION_LOADED_DRIVER_DIRECTORY")
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(directory) || filepath.Dir(path) != directory || filepath.Base(path) != "owned-driver" {
		t.Fatal("refuse to alter an executable outside this child's owned fixture")
	}
	profile, inputs := fixtureInputs(t)
	if inputs.Driver.Path != path {
		t.Fatal("child driver path does not match owned fixture")
	}
	original, err := readFile(path, maxBinary)
	if err != nil {
		t.Fatal(err)
	}
	owner := os.Getenv("WEIR_EXECUTION_LOADED_DRIVER_OWNER")
	marker, err := readFile(filepath.Join(directory, ".weir-loaded-driver-owner"), 1024)
	if err != nil || owner == "" || string(marker) != owner+"\n"+hash(original) {
		t.Fatal("owned child marker/executable identity mismatch", err)
	}
	requireDriverValidation(t, profile, inputs)
	t.Log("baseline_valid=true")
	retained := filepath.Join(directory, "retained-loaded-driver")
	if err := os.Rename(path, retained); err != nil {
		t.Fatal(err)
	}
	// Best-effort restore on any assertion failure; only these owned files can be
	// touched. A successful final unlink deliberately leaves no pathname to restore.
	retainedPresent := true
	t.Cleanup(func() {
		if retainedPresent {
			_ = os.Remove(path)
			if err := os.Rename(retained, path); err != nil {
				t.Error("cannot restore owned loaded image", err)
			}
		}
	})
	renamed := inputs
	renamed.Driver.Path = retained
	requireDriverValidation(t, profile, renamed)
	t.Log("renamed_loaded_inode_valid=true")
	if value, err := Validate(profile, inputs); err == nil || value != nil {
		t.Fatal("missing old pathname accepted")
	}
	fixtureWrite(t, path, original)
	loadedInfo, err := os.Stat(retained)
	if err != nil {
		t.Fatal(err)
	}
	replacedInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(loadedInfo, replacedInfo) {
		t.Fatal("fixture did not create a different inode")
	}
	requireDriverReplacementRejected(t, profile, inputs)
	t.Log("same_bytes_new_inode_rejected=true")
	replacement := append(append([]byte(nil), original...), []byte("\nUNEXECUTED_OWNED_DRIVER_REPLACEMENT\n")...)
	fixtureWrite(t, path, replacement)
	different := inputs
	different.Driver.SHA256 = hash(replacement)
	if different.Driver.SHA256 == hash(original) {
		t.Fatal("different-byte fixture hash did not change")
	}
	requireDriverReplacementRejected(t, profile, different)
	t.Log("different_claimed_hash_rejected=true")
	requireDriverValidation(t, profile, renamed)
	t.Log("renamed_inode_with_replacement_valid=true")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(retained, path); err != nil {
		t.Fatal(err)
	}
	retainedPresent = false
	requireDriverValidation(t, profile, inputs)
	t.Log("restored_loaded_inode_valid=true")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if value, err := Validate(profile, inputs); err == nil || value != nil {
		t.Fatal("unlinked driver pathname accepted")
	}
	t.Log("unlinked_driver_path_rejected=true")
	t.Logf("kernel-bound driver assertions completed on %s/%s; no executor or service started", runtime.GOOS, runtime.GOARCH)
}
