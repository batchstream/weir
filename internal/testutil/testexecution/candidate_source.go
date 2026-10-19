package testexecution

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

const grpcSourceRoot = "third_party/grpc/"
const grpcUpstreamModule = "google.golang.org/grpc"
const grpcUpstreamVersion = "v1.83.2"
const grpcUpstreamGoSum = "h1:EManeRomTObA0BU7I8vXgg/78uE5MJ9M8B39EX2WscU="

type grpcPatchedFile struct {
	Path           string `json:"path"`
	UpstreamSHA256 string `json:"upstream_sha256"`
	SHA256         string `json:"sha256"`
}
type grpcNewFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}
type grpcSourceManifest struct {
	Module        string            `json:"module"`
	Version       string            `json:"version"`
	UpstreamGoSum string            `json:"upstreamGoSum"`
	Files         map[string]string `json:"files"`
	PatchedFiles  []grpcPatchedFile `json:"patchedFiles"`
	NewFiles      []grpcNewFile     `json:"newFiles"`
	CopyPolicy    string            `json:"copyPolicy"`
}
type candidateSourceReader struct {
	source     Source
	inputs     map[string]string
	tracked    map[string]bool
	publicGRPC map[string]string
}

func verifyCandidateSourceInputs(source Source, inputs map[string]string) error {
	if sensitivePath(filepath.ToSlash(source.Root)) {
		return fmt.Errorf("sensitive source root forbidden")
	}
	if err := canonicalPath(source.Root); err != nil {
		return err
	}
	if !revisionPattern.MatchString(source.Revision) {
		return fmt.Errorf("source revision required")
	}
	records, err := gitOutput(source.Root, "ls-files", "--stage", "-z")
	if err != nil {
		return err
	}
	reader := &candidateSourceReader{source: source, inputs: inputs, tracked: make(map[string]bool)}
	for _, record := range strings.Split(records, "\x00") {
		metadata, name, ok := strings.Cut(record, "\t")
		if !ok {
			continue
		}
		fields := strings.Fields(metadata)
		if len(fields) == 3 && (fields[0] == "100644" || fields[0] == "100755") && fields[2] == "0" {
			reader.tracked[name] = true
		}
	}
	names := make([]string, 0, len(inputs))
	for name := range inputs {
		names = append(names, name)
	}
	sort.Strings(names)
	total := 0
	for _, name := range names {
		digest := inputs[name]
		if !safeRelative(name) || !reader.tracked[name] || !digestPattern.MatchString(digest) {
			return fmt.Errorf("unsafe candidate source input %q", name)
		}
		var raw []byte
		if sensitivePath(name) {
			if !publicGRPCSourceName(name) {
				return fmt.Errorf("unsafe candidate source input %q", name)
			}
			if reader.publicGRPC == nil {
				if err := reader.loadPublicGRPC(); err != nil {
					return err
				}
			}
			if reader.publicGRPC[name] != digest {
				return fmt.Errorf("unlisted or conflicting public gRPC source %q", name)
			}
			// This is the only sensitive-name exception. Metadata grants this exact
			// tracked public member and digest, never a directory-wide read permission.
			raw, err = readRegularFile(filepath.Join(source.Root, filepath.FromSlash(name)), 16<<20)
		} else {
			raw, err = readFile(filepath.Join(source.Root, filepath.FromSlash(name)), 16<<20)
		}
		if err != nil {
			return err
		}
		total += len(raw)
		if total > 256<<20 || hash(raw) != digest {
			return fmt.Errorf("candidate source input digest/bound mismatch: %q", name)
		}
	}
	return nil
}

// Only the three known public package roots may contain a credentials segment.
// A second such segment, assets, test source, or any secret-style descendant is
// not an exception, even when a caller-controlled receipt happens to list it.
func publicGRPCSourceName(name string) bool {
	if !safeRelative(name) || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
		return false
	}
	relative := strings.TrimPrefix(name, grpcSourceRoot)
	if relative == name {
		return false
	}
	for _, prefix := range []string{"credentials/", "internal/credentials/", "experimental/credentials/"} {
		if strings.HasPrefix(relative, prefix) {
			return !sensitivePath(strings.TrimPrefix(relative, prefix))
		}
	}
	return false
}

func (reader *candidateSourceReader) trustedMetadata(name string) ([]byte, error) {
	if !reader.tracked[name] || !digestPattern.MatchString(reader.inputs[name]) {
		return nil, fmt.Errorf("public gRPC source requires tracked receipt metadata %q", name)
	}
	raw, err := readFile(filepath.Join(reader.source.Root, filepath.FromSlash(name)), maxJSON)
	if err != nil {
		return nil, err
	}
	if hash(raw) != reader.inputs[name] {
		return nil, fmt.Errorf("public gRPC metadata digest mismatch: %q", name)
	}
	committed, err := gitBytes(reader.source.Root, "show", reader.source.Revision+":"+name)
	if err != nil {
		return nil, err
	}
	if hash(raw) != hash(committed) {
		return nil, fmt.Errorf("public gRPC metadata differs from declared source revision: %q", name)
	}
	return raw, nil
}
func (reader *candidateSourceReader) loadPublicGRPC() error {
	raw, err := reader.trustedMetadata(grpcSourceRoot + "WEIR_UPSTREAM.json")
	if err != nil {
		return err
	}
	var manifest grpcSourceManifest
	if err := decode(raw, &manifest); err != nil {
		return err
	}
	if manifest.Module != grpcUpstreamModule || manifest.Version != grpcUpstreamVersion || manifest.UpstreamGoSum != grpcUpstreamGoSum || manifest.CopyPolicy == "" || len(manifest.Files) == 0 || len(manifest.Files) > 4096 || len(manifest.PatchedFiles) > 4096 || len(manifest.NewFiles) > 4096 {
		return fmt.Errorf("public gRPC upstream module/version/checksum identity mismatch")
	}
	expected := make(map[string]string, len(manifest.Files))
	changed := make(map[string]bool)
	for name, digest := range manifest.Files {
		if !safeRelative(name) || !digestPattern.MatchString(digest) {
			return fmt.Errorf("invalid gRPC upstream manifest member")
		}
		expected[name] = digest
	}
	for _, record := range manifest.PatchedFiles {
		if changed[record.Path] || manifest.Files[record.Path] == "" || manifest.Files[record.Path] != record.UpstreamSHA256 || !digestPattern.MatchString(record.SHA256) {
			return fmt.Errorf("gRPC patch upstream/member identity mismatch")
		}
		expected[record.Path] = record.SHA256
		changed[record.Path] = true
	}
	for _, record := range manifest.NewFiles {
		if !safeRelative(record.Path) || expected[record.Path] != "" || !digestPattern.MatchString(record.SHA256) {
			return fmt.Errorf("gRPC new patch member identity mismatch")
		}
		expected[record.Path] = record.SHA256
	}
	for _, name := range []string{"go.mod", "WEIR_PATCH.diff", "WEIR_PATCH.md"} {
		if expected[name] == "" {
			return fmt.Errorf("gRPC patch/module metadata missing: %q", name)
		}
		content, err := reader.trustedMetadata(grpcSourceRoot + name)
		if err != nil {
			return err
		}
		if hash(content) != expected[name] {
			return fmt.Errorf("gRPC patch/module metadata differs from manifest: %q", name)
		}
		if name == "go.mod" {
			line, _, _ := strings.Cut(string(content), "\n")
			if strings.TrimSpace(line) != "module "+grpcUpstreamModule {
				return fmt.Errorf("public gRPC go.mod module mismatch")
			}
		}
	}
	reader.publicGRPC = make(map[string]string)
	for name, digest := range expected {
		fullName := grpcSourceRoot + name
		if publicGRPCSourceName(fullName) {
			reader.publicGRPC[fullName] = digest
		}
	}
	return nil
}
