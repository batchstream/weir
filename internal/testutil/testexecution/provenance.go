package testexecution

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"debug/elf"
	"debug/macho"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type Source struct {
	Root     string `json:"root"`
	Revision string `json:"revision"`
}
type Executable struct {
	Role   string `json:"role"`
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// CandidateBundle points only to already-present retained delivery bytes. It is
// optional and supplies byte consistency, not an independent origin attestation.
type CandidateBundle struct {
	Directory      string `json:"directory"`
	DeliverySHA256 string `json:"deliverySHA256"`
	ReceiptSHA256  string `json:"receiptSHA256"`
}
type ProvenanceInputs struct {
	SchemaVersion int              `json:"schemaVersion"`
	ProfileID     string           `json:"profileID"`
	Server        Source           `json:"server"`
	Harness       Source           `json:"harness"`
	CLI           Executable       `json:"cli"`
	Driver        Executable       `json:"driver"`
	Candidate     *CandidateBundle `json:"candidate,omitempty"`
}
type ValidatedInputs struct {
	valid   bool
	profile ExecutionProfile
	inputs  ProvenanceInputs
}
type LoadOptions struct {
	ProfilePath    string
	ProvenancePath string
	Suite          string
	LegacyEnv      map[string]string
}

var revisionPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func hash(raw []byte) string { return fmt.Sprintf("%x", sha256.Sum256(raw)) }
func validateProvenanceShape(p ProvenanceInputs) error {
	if p.SchemaVersion != SchemaVersion || p.ProfileID == "" {
		return fmt.Errorf("missing or unknown provenance schema/profile")
	}
	if !revisionPattern.MatchString(p.Server.Revision) || !revisionPattern.MatchString(p.Harness.Revision) || p.Server.Root == "" || p.Harness.Root == "" {
		return fmt.Errorf("distinct server and harness source inputs required")
	}
	if p.CLI.Role != "cli" || p.Driver.Role != "driver" || !digestPattern.MatchString(p.CLI.SHA256) || !digestPattern.MatchString(p.Driver.SHA256) || p.CLI.Path == "" || p.Driver.Path == "" || p.CLI.Path == p.Driver.Path {
		return fmt.Errorf("distinct CLI/driver roles, paths and SHA256 required")
	}
	if p.Candidate != nil && (p.Candidate.Directory == "" || !digestPattern.MatchString(p.Candidate.DeliverySHA256) || !digestPattern.MatchString(p.Candidate.ReceiptSHA256)) {
		return fmt.Errorf("candidate directory and receipt digests required")
	}
	return nil
}
func Load(opts LoadOptions) (*ValidatedInputs, error) {
	raw, err := readFile(opts.ProfilePath, maxJSON)
	if err != nil {
		return nil, fmt.Errorf("profile: %w", err)
	}
	profile, err := DecodeProfile(raw)
	if err != nil {
		return nil, err
	}
	raw, err = readFile(opts.ProvenancePath, maxJSON)
	if err != nil {
		return nil, fmt.Errorf("provenance: %w", err)
	}
	inputs, err := DecodeProvenance(raw)
	if err != nil {
		return nil, err
	}
	if opts.Suite != profile.Suite {
		return nil, fmt.Errorf("profile suite conflicts with consumer")
	}
	if err := CheckLegacyEnv(profile, inputs, opts.LegacyEnv); err != nil {
		return nil, err
	}
	return Validate(profile, inputs)
}
func Validate(profile ExecutionProfile, inputs ProvenanceInputs) (*ValidatedInputs, error) {
	if err := validateProfile(profile); err != nil {
		return nil, err
	}
	if err := validateProvenanceShape(inputs); err != nil {
		return nil, err
	}
	if inputs.ProfileID != profile.ID {
		return nil, fmt.Errorf("profile/provenance ID mismatch")
	}
	if err := verifySource(inputs.Server); err != nil {
		return nil, fmt.Errorf("server source: %w", err)
	}
	if err := verifySource(inputs.Harness); err != nil {
		return nil, fmt.Errorf("harness source: %w", err)
	}
	cli, err := verifyExecutable(inputs.CLI, profile.CLI)
	if err != nil {
		return nil, err
	}
	driver, err := verifyRunningDriver(inputs.Driver, profile.Driver)
	if err != nil {
		return nil, err
	}
	if err := verifyBuildSource(cli, inputs.Server); err != nil {
		return nil, err
	}
	if err := verifyBuildSource(driver, inputs.Harness); err != nil {
		return nil, err
	}
	if inputs.Candidate != nil {
		if err := verifyCandidate(profile, inputs, cli); err != nil {
			return nil, err
		}
	}
	// Snapshot mutable slices/pointers; callers cannot mutate a validated result.
	profile.Backends = append([]Backend(nil), profile.Backends...)
	if inputs.Candidate != nil {
		candidate := *inputs.Candidate
		inputs.Candidate = &candidate
	}
	validated := &ValidatedInputs{valid: true, profile: profile, inputs: inputs}
	return validated, nil
}
func gitOutput(root string, args ...string) (string, error) {
	raw, err := gitBytes(root, args...)
	return strings.TrimSpace(string(raw)), err
}
func gitBytes(root string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	arguments := append([]string{"-C", root}, args...)
	command := exec.CommandContext(ctx, "git", arguments...)
	// Do not inherit GIT_DIR/worktree overrides from the test driver.
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "GIT_") {
			command.Env = append(command.Env, entry)
		}
	}
	command.Env = append(command.Env, "GIT_OPTIONAL_LOCKS=0", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	var out bytes.Buffer
	output := &boundedOutput{buffer: &out, remaining: 4 << 20}
	command.Stdout = output
	command.Stderr = output
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("git check failed: %w", err)
	}
	return out.Bytes(), nil
}

type boundedOutput struct {
	buffer    *bytes.Buffer
	remaining int
}

func (w *boundedOutput) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		return 0, fmt.Errorf("command output exceeds bound")
	}
	w.remaining -= len(p)
	return w.buffer.Write(p)
}
func verifySource(source Source) error {
	if err := canonicalPath(source.Root); err != nil {
		return err
	}
	root, err := gitOutput(source.Root, "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	if root != source.Root {
		return fmt.Errorf("source root is not repository top level")
	}
	revision, err := gitOutput(source.Root, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if revision != source.Revision {
		return fmt.Errorf("source revision mismatch")
	}
	dirty, err := gitOutput(source.Root, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return err
	}
	if dirty != "" {
		return fmt.Errorf("source tree is dirty")
	}
	return nil
}
func verifyBuildSource(raw []byte, source Source) error {
	info, err := buildinfo.Read(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	revision, modified := "", ""
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			revision = setting.Value
		}
		if setting.Key == "vcs.modified" {
			modified = setting.Value
		}
	}
	if (revision != "" && revision != source.Revision) || (modified != "" && modified != "false") {
		return fmt.Errorf("actual buildinfo VCS source conflicts with clean source input")
	}
	// Packaged -buildvcs=false binaries and Go test binaries can omit VCS data.
	// Absence is not independent build-origin proof; runtime text is cross-check only.
	return nil
}
func verifyExecutable(input Executable, target Target) ([]byte, error) {
	raw, err := readFile(input.Path, maxBinary)
	if err != nil {
		return nil, err
	}
	stat, err := os.Stat(input.Path)
	if err != nil {
		return nil, err
	}
	object := executableObject{input: input, target: target, raw: raw, stat: stat}
	return verifyExecutableObject(object)
}

type executableObject struct {
	input  Executable
	target Target
	raw    []byte
	stat   os.FileInfo
}

// Both CLI paths and kernel-bound driver descriptors share exactly these byte,
// build-info, role, target and format checks.
func verifyExecutableObject(object executableObject) ([]byte, error) {
	input, target, raw, stat := object.input, object.target, object.raw, object.stat
	if stat.Mode().Perm()&0111 == 0 || hash(raw) != input.SHA256 {
		return nil, fmt.Errorf("%s executable mode/digest mismatch", input.Role)
	}
	info, err := buildinfo.Read(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("%s lacks actual Go buildinfo: %w", input.Role, err)
	}
	settings := make(map[string]string)
	for _, s := range info.Settings {
		settings[s.Key] = s.Value
	}
	if settings["GOOS"] != target.OS || settings["GOARCH"] != target.Arch {
		return nil, fmt.Errorf("%s build target mismatch", input.Role)
	}
	if input.Role == "cli" {
		if info.Path != "github.com/batchstream/weir/cmd/weir" || settings["CGO_ENABLED"] != "0" {
			return nil, fmt.Errorf("CLI role requires the static Weir command, not a driver")
		}
	} else if !strings.HasSuffix(info.Path, ".test") {
		return nil, fmt.Errorf("driver role requires an actual Go test executable")
	}
	if target.OS == "linux" {
		executable, err := elf.NewFile(bytes.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("actual Linux ELF required: %w", err)
		}
		defer executable.Close()
		machine := elf.EM_AARCH64
		if target.Arch == "amd64" {
			machine = elf.EM_X86_64
		}
		if executable.Class != elf.ELFCLASS64 || executable.Machine != machine {
			return nil, fmt.Errorf("ELF machine mismatch")
		}
	} else if target.OS == "darwin" {
		executable, err := macho.NewFile(bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		defer executable.Close()
		cpu := macho.CpuArm64
		if target.Arch == "amd64" {
			cpu = macho.CpuAmd64
		}
		if executable.Cpu != cpu {
			return nil, fmt.Errorf("Mach-O CPU mismatch")
		}
	} else {
		return nil, fmt.Errorf("unsupported executable format")
	}
	return raw, nil
}

// Old environment variables never select a profile or supply missing inputs.
func CheckLegacyEnv(p ExecutionProfile, in ProvenanceInputs, env map[string]string) error {
	if err := validateProfile(p); err != nil {
		return err
	}
	expected := map[string]string{"WEIR_INTEGRATION": "1", "WEIR_SEARCH_INTEGRATION": p.Backends[1].Product, "WEIR_MONGO_PROFILE": "", "WEIR_QUALIFICATION_BINARY": in.CLI.Path, "WEIR_QUALIFICATION_SOURCE": in.Server.Revision, "WEIR_QUALIFICATION_SHA256": in.CLI.SHA256}
	if p.Backends[0].Transport == "tls" {
		expected["WEIR_MONGO_PROFILE"] = "tls"
	}
	smoke := "0"
	if p.Intent == "smoke" {
		smoke = "1"
	}
	expected["WEIR_PRODUCTION_QUALIFICATION_SMOKE"] = smoke
	expected["WEIR_FAULT_EXPLORATORY"] = "0"
	for key, value := range env {
		want, ok := expected[key]
		if !ok {
			return fmt.Errorf("unrecognized legacy environment %s", key)
		}
		if value != want {
			return fmt.Errorf("legacy environment conflicts with profile: %s", key)
		}
	}
	return nil
}

// CheckVersion only cross-checks linker-controlled text against verified bytes
// and local source inputs. It cannot prove where those bytes were built.
func (v *ValidatedInputs) CheckVersion(raw []byte) error {
	if v == nil || !v.valid {
		return fmt.Errorf("validated inputs required")
	}
	var identity struct {
		Product  string `json:"product"`
		Version  string `json:"version"`
		Revision string `json:"revision"`
		State    string `json:"state"`
		Dirty    string `json:"dirty"`
		Target   string `json:"target"`
		Go       string `json:"go"`
	}
	if err := decode(raw, &identity); err != nil {
		return err
	}
	cli, err := verifyExecutable(v.inputs.CLI, v.profile.CLI)
	if err != nil {
		return err
	}
	build, err := buildinfo.Read(bytes.NewReader(cli))
	if err != nil {
		return err
	}
	if identity.Go != build.GoVersion || identity.Version != "local-"+v.inputs.Server.Revision || identity.Product != "weir" || identity.Revision != v.inputs.Server.Revision || identity.Target != v.profile.CLI.String() || identity.State != "clean-commit" || identity.Dirty != "false" {
		return fmt.Errorf("CLI version cross-check mismatch")
	}
	return nil
}

// RecheckCLI closes the ordinary validation-to-mount gap; execution must still
// use an owned local fixture, not a caller-selected remote endpoint.
func (v *ValidatedInputs) RecheckCLI() error {
	if err := v.RequireExecutable(); err != nil {
		return err
	}
	_, err := verifyExecutable(v.inputs.CLI, v.profile.CLI)
	return err
}
func (v *ValidatedInputs) CheckHarnessRoot(root string) error {
	actual, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	if actual != v.inputs.Harness.Root {
		return fmt.Errorf("consumer source root does not match harness input")
	}
	return verifySource(v.inputs.Harness)
}
