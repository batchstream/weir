package testexecution

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func fixtureWrite(t *testing.T, path string, raw []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0755); err != nil {
		t.Fatal(err)
	}
}
func testCommand(t *testing.T, dir, name string, args ...string) string {
	t.Helper()
	command := exec.Command(name, args...)
	command.Dir = dir
	command.Env = append(os.Environ(), "GOWORK=off", "GOENV=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off", "GOFLAGS=-mod=readonly", "CGO_ENABLED=0", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	raw, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("%s failed: %v %s", name, err, raw)
	}
	return strings.TrimSpace(string(raw))
}
func fixtureRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}
func fixtureSource(t *testing.T) Source {
	t.Helper()
	root := fixtureRoot(t)
	testCommand(t, root, "git", "init", "--quiet")
	// A tiny synthetic repository, not a commit/change to the developer checkout.
	content := "source fixture\n"
	stream := fmt.Sprintf("blob\nmark :1\ndata %d\n%s\ncommit refs/heads/main\ncommitter Fixture <fixture@example.invalid> 1 +0000\ndata 7\nfixture\nM 100644 :1 fixture.txt\n\ndone\n", len(content), content)
	command := exec.Command("git", "-C", root, "fast-import", "--quiet")
	command.Stdin = strings.NewReader(stream)
	if raw, err := command.CombinedOutput(); err != nil {
		t.Fatalf("fixture import: %v %s", err, raw)
	}
	testCommand(t, root, "git", "symbolic-ref", "HEAD", "refs/heads/main")
	testCommand(t, root, "git", "read-tree", "HEAD")
	testCommand(t, root, "git", "checkout-index", "-a")
	revision := testCommand(t, root, "git", "rev-parse", "HEAD")
	source := Source{Root: root, Revision: revision}
	return source
}
func fixtureCLI(t *testing.T, arch string) Executable {
	t.Helper()
	root := fixtureRoot(t)
	fixtureWrite(t, filepath.Join(root, "go.mod"), []byte("module github.com/batchstream/weir\n\ngo 1.27.1\n"))
	fixtureWrite(t, filepath.Join(root, "cmd", "weir", "main.go"), []byte("package main\nfunc main() {}\n"))
	binary := filepath.Join(fixtureRoot(t), "weir")
	command := exec.Command("go", "build", "-trimpath", "-buildvcs=false", "-o", binary, "./cmd/weir")
	command.Dir = root
	command.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+arch, "CGO_ENABLED=0", "GOWORK=off", "GOENV=off", "GOTOOLCHAIN=local", "GOFLAGS=-mod=readonly", "GOPROXY=off", "GOSUMDB=off")
	if raw, err := command.CombinedOutput(); err != nil {
		t.Fatalf("offline CLI fixture build: %v %s", err, raw)
	}
	raw, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	input := Executable{Role: "cli", Path: binary, SHA256: hash(raw)}
	return input
}
func fixtureInputs(t *testing.T) (ExecutionProfile, ProvenanceInputs) {
	t.Helper()
	profile, err := LegacyProfile("legacy-desktop-load-v1")
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "linux" {
		profile.ID = "linux-native-load-v1"
		profile.Executor = LinuxExecutor
		profile.Intent = "formal"
		profile.Driver = Target{OS: "linux", Arch: runtime.GOARCH}
		profile.CLI = profile.Driver
		for i := range profile.Backends {
			profile.Backends[i].Endpoints = EndpointRoles{Driver: "owned-local", CLI: "owned-local"}
		}
	}
	source := fixtureSource(t)
	cli := fixtureCLI(t, profile.CLI.Arch)
	driverPath, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	driverPath, err = filepath.EvalSymlinks(driverPath)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(driverPath)
	if err != nil {
		t.Fatal(err)
	}
	driver := Executable{Role: "driver", Path: driverPath, SHA256: hash(raw)}
	in := ProvenanceInputs{SchemaVersion: 1, ProfileID: profile.ID, Server: source, Harness: source, CLI: cli, Driver: driver}
	return profile, in
}
func TestExecutionProfileContractStrictDecode(t *testing.T) {
	p, err := LegacyProfile("legacy-desktop-smoke-v1")
	if err != nil {
		t.Fatal(err)
	}
	raw := mustJSON(t, p)
	if _, err := DecodeProfile(raw); err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"missing": nil, "unknown": bytes.Replace(raw, []byte(`"schemaVersion":1`), []byte(`"schemaVersion":99`), 1),
		"duplicate alias":          bytes.Replace(raw, []byte(`"schemaVersion":1`), []byte(`"schemaVersion":1,"SchemaVersion":1`), 1),
		"case alias":               bytes.Replace(raw, []byte(`"schemaVersion":1`), []byte(`"SchemaVersion":1`), 1),
		"formal short measurement": bytes.Replace(raw, []byte(`"intent":"smoke"`), []byte(`"intent":"formal","measurementSeconds":120`), 1),
		"field":                    bytes.Replace(raw, []byte(`"schemaVersion":1`), []byte(`"schemaVersion":1,"passed":true`), 1),
		"duplicate":                bytes.Replace(raw, []byte(`"schemaVersion":1`), []byte(`"schemaVersion":1,"schemaVersion":1`), 1),
		"nested duplicate":         bytes.Replace(raw, []byte(`"os":"darwin"`), []byte(`"os":"darwin","os":"darwin"`), 1),
		"trailing":                 append(append([]byte(nil), raw...), []byte(` {}`)...), "oversize": bytes.Repeat([]byte(" "), maxJSON+1),
		"null": []byte("null"), "tunable policy": bytes.Replace(raw, []byte(`"schemaVersion":1`), []byte(`"schemaVersion":1,"duration":120`), 1),
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeProfile(input); err == nil {
				t.Fatal("accepted invalid profile")
			}
		})
	}
	for _, name := range []string{"passed", "verified", "formalEligible"} {
		input := []byte(`{"schemaVersion":1,"` + name + `":true}`)
		if _, err := DecodeProvenance(input); err == nil {
			t.Fatal("accepted self-attestation", name)
		}
	}
	var validated ValidatedInputs
	if err := json.Unmarshal([]byte(`{}`), &validated); err == nil {
		t.Fatal("decoded validated state")
	}
	if err := validated.RequireExecutable(); err == nil {
		t.Fatal("zero validated state executable")
	}
}
func TestExecutionProfileContractFixedSpecs(t *testing.T) {
	for _, id := range []string{"legacy-desktop-load-v1", "legacy-desktop-smoke-v1", "legacy-desktop-fault-v1"} {
		p, err := LegacyProfile(id)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateProfile(p); err != nil {
			t.Fatal(err)
		}
		if p.CapacityMiB() != LoadMiB && p.Suite != "fault" {
			t.Fatal("load resource policy changed")
		}
		if p.Intent == "smoke" && p.Duration() != SmokeMeasurement {
			t.Fatal("smoke duration changed")
		}
		p.Intent = "formal"
		if err := validateProfile(p); err == nil {
			t.Fatal("legacy promoted to formal")
		}
	}
	p, _ := LegacyProfile("legacy-desktop-load-v1")
	p.Backends[0].Transport = "tls"
	if err := validateProfile(p); err == nil {
		t.Fatal("TLS downgraded to legacy plain")
	}
	in := ProvenanceInputs{}
	in.CLI.Path = "/cli"
	in.CLI.SHA256 = strings.Repeat("a", 64)
	in.Server.Revision = strings.Repeat("b", 40)
	conflicts := map[string]string{"WEIR_QUALIFICATION_BINARY": "/wrong", "WEIR_QUALIFICATION_SOURCE": "wrong", "WEIR_QUALIFICATION_SHA256": "wrong", "WEIR_PRODUCTION_QUALIFICATION_SMOKE": "1", "WEIR_FAULT_EXPLORATORY": "1", "WEIR_MONGO_PROFILE": "plain", "WEIR_SEARCH_INTEGRATION": "opensearch"}
	p, _ = LegacyProfile("legacy-desktop-load-v1")
	for key, value := range conflicts {
		env := map[string]string{key: value}
		if err := CheckLegacyEnv(p, in, env); err == nil {
			t.Fatal("accepted conflicting legacy env", key)
		}
	}
}
func TestExecutionProfileContractDeclaredNotExecutable(t *testing.T) {
	for _, arch := range []string{"amd64", "arm64"} {
		for _, suite := range []string{"load", "fault"} {
			for _, transport := range []string{"plain", "tls"} {
				for _, product := range []string{"elasticsearch", "opensearch"} {
					mongoRoles := EndpointRoles{Driver: "owned-local", CLI: "owned-local"}
					mongo := Backend{Product: "mongodb", Version: "8.0.32", Transport: transport, Endpoints: mongoRoles}
					version := "8.19.22"
					if product == "opensearch" {
						version = "3.3.0"
					}
					search := Backend{Product: product, Version: version, Transport: transport, Endpoints: mongoRoles}
					target := Target{OS: "linux", Arch: arch}
					profile := ExecutionProfile{SchemaVersion: 1, ID: "linux-native-" + suite + "-v1", Suite: suite, Intent: "formal", Policy: PolicyID, Executor: LinuxExecutor, Driver: target, CLI: target, Backends: []Backend{mongo, search}}
					if err := validateProfile(profile); err != nil {
						t.Fatal("valid declaration rejected", err)
					}
					// Private construction is only for exercising dispatch in this package test;
					// outside callers can obtain this state only by real Validate checks.
					inputs := &ValidatedInputs{valid: true, profile: profile}
					callbacks := 0
					if err := inputs.RequireExecutable(); err == nil {
						callbacks++
					}
					want := 0
					if arch == "amd64" && suite == "load" && transport == "plain" && product == "elasticsearch" {
						want = 1
					}
					if callbacks != want {
						t.Fatal("exact native capability dispatch differs", profile, callbacks, want)
					}
				}
			}
		}
	}
}
func TestExecutionProfileContractActualObjects(t *testing.T) {
	p, in := fixtureInputs(t)
	validated, err := Validate(p, in)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "linux" && runtime.GOARCH != "amd64" {
		if err := validated.RequireExecutable(); err == nil {
			t.Fatal("unsupported Linux architecture allowed")
		}
	} else if err := validated.RequireExecutable(); err != nil {
		t.Fatal(err)
	}
	snapshot := validated.Profile()
	snapshot.Backends[0].Transport = "tls"
	if validated.Profile().Backends[0].Transport != "plain" {
		t.Fatal("mutable validated profile")
	}
	tests := map[string]func(*ProvenanceInputs){
		"wrong revision":      func(i *ProvenanceInputs) { i.Server.Revision = strings.Repeat("a", 40) },
		"wrong harness":       func(i *ProvenanceInputs) { i.Harness.Revision = strings.Repeat("a", 40) },
		"wrong hash":          func(i *ProvenanceInputs) { i.CLI.SHA256 = strings.Repeat("a", 64) },
		"role confusion":      func(i *ProvenanceInputs) { i.CLI.Role = "driver" },
		"driver is CLI":       func(i *ProvenanceInputs) { i.Driver.Path = i.CLI.Path; i.Driver.SHA256 = i.CLI.SHA256 },
		"missing server root": func(i *ProvenanceInputs) { i.Server.Root = "" },
		"relative path":       func(i *ProvenanceInputs) { i.CLI.Path = "weir" },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			bad := in
			change(&bad)
			if _, err := Validate(p, bad); err == nil {
				t.Fatal("accepted invalid actual input")
			}
		})
	}
	symlink := filepath.Join(fixtureRoot(t), "linked-cli")
	if err := os.Symlink(in.CLI.Path, symlink); err != nil {
		t.Fatal(err)
	}
	bad := in
	bad.CLI.Path = symlink
	if _, err := Validate(p, bad); err == nil {
		t.Fatal("accepted symlink CLI")
	}
	bad = in
	copied := filepath.Join(fixtureRoot(t), "copied-driver")
	raw, err := os.ReadFile(in.Driver.Path)
	if err != nil {
		t.Fatal(err)
	}
	fixtureWrite(t, copied, raw)
	bad.Driver.Path = copied
	if _, err := Validate(p, bad); err == nil {
		t.Fatal("driver copy is not running executable")
	}
	wrongTarget := p.CLI
	wrongTarget.Arch = "amd64"
	if p.CLI.Arch == "amd64" {
		wrongTarget.Arch = "arm64"
	}
	if _, err := verifyExecutable(in.CLI, wrongTarget); err == nil {
		t.Fatal("accepted wrong native target")
	}
	fixtureWrite(t, filepath.Join(in.Server.Root, "fixture.txt"), []byte("dirty"))
	if _, err := Validate(p, in); err == nil {
		t.Fatal("accepted dirty source")
	}
}
func testArchive(t *testing.T, binary []byte, target string, mutate func(*tar.Header)) []byte {
	t.Helper()
	var raw bytes.Buffer
	zip := gzip.NewWriter(&raw)
	writer := tar.NewWriter(zip)
	members, err := membersForTarget(target)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range members {
		content := []byte("fixture")
		mode := int64(0644)
		if name == "weir" {
			content = binary
			mode = 0755
		}
		header := &tar.Header{Name: name, Mode: mode, Size: int64(len(content)), Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}
		if mutate != nil {
			mutate(header)
		}
		if header.Typeflag != tar.TypeReg {
			header.Size = 0
			content = nil
		}
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zip.Close(); err != nil {
		t.Fatal(err)
	}
	return raw.Bytes()
}
func TestExecutionProfileContractSafeArchive(t *testing.T) {
	good := testArchive(t, []byte("actual bytes"), "linux-arm64", nil)
	if digest, err := archiveBinaryHash(good, "linux-arm64"); err != nil || digest != hash([]byte("actual bytes")) {
		t.Fatal(digest, err)
	}
	changes := map[string]func(*tar.Header){
		"path escape": func(h *tar.Header) { h.Name = "../" + h.Name }, "absolute": func(h *tar.Header) { h.Name = "/" + h.Name },
		"duplicate": func(h *tar.Header) { h.Name = "weir" }, "symlink": func(h *tar.Header) { h.Typeflag = tar.TypeSymlink; h.Linkname = "outside" },
		"hardlink": func(h *tar.Header) { h.Typeflag = tar.TypeLink; h.Linkname = "outside" }, "extra": func(h *tar.Header) { h.Name = "unexpected" },
		"non executable": func(h *tar.Header) { h.Mode = 0644 },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			archive := testArchive(t, []byte("actual bytes"), "linux-arm64", change)
			if _, err := archiveBinaryHash(archive, "linux-arm64"); err == nil {
				t.Fatal("accepted unsafe archive")
			}
		})
	}
	trailing := append(append([]byte(nil), good...), good...)
	if _, err := archiveBinaryHash(trailing, "linux-arm64"); err == nil {
		t.Fatal("accepted concatenated gzip")
	}
	if _, err := archiveBinaryHash(good[:len(good)-5], "linux-arm64"); err == nil {
		t.Fatal("accepted truncated gzip")
	}
}
