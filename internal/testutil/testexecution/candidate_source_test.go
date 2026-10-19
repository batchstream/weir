package testexecution

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The binary is read into bounded memory only after the production archive
// checker accepts every member. Nothing is extracted or executed.
func readArchiveCLIFixture(t *testing.T, raw []byte, target string) []byte {
	t.Helper()
	if _, err := archiveBinaryHash(raw, target); err != nil {
		t.Fatal(err)
	}
	compressed, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()
	reader := tar.NewReader(compressed)
	for {
		member, err := reader.Next()
		if err != nil {
			t.Fatal("checked archive lacks CLI", err)
		}
		if member.Name == "weir" {
			content, err := io.ReadAll(io.LimitReader(reader, maxBinary+1))
			if err != nil || int64(len(content)) != member.Size || len(content) > maxBinary {
				t.Fatal("bounded fixture CLI read", err)
			}
			return content
		}
	}
}
func fixtureSourceTree(t *testing.T, files map[string][]byte) Source {
	t.Helper()
	root := fixtureRoot(t)
	testCommand(t, root, "git", "init", "--quiet")
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	var stream strings.Builder
	for i, name := range names {
		content := files[name]
		fmt.Fprintf(&stream, "blob\nmark :%d\ndata %d\n%s\n", i+1, len(content), content)
	}
	stream.WriteString("commit refs/heads/main\ncommitter Fixture <fixture@example.invalid> 1 +0000\ndata 7\nfixture\n")
	for i, name := range names {
		fmt.Fprintf(&stream, "M 100644 :%d %s\n", i+1, name)
	}
	stream.WriteString("\ndone\n")
	command := exec.Command("git", "-C", root, "fast-import", "--quiet")
	command.Stdin = strings.NewReader(stream.String())
	if raw, err := command.CombinedOutput(); err != nil {
		t.Fatalf("fixture source import: %v %s", err, raw)
	}
	testCommand(t, root, "git", "symbolic-ref", "HEAD", "refs/heads/main")
	testCommand(t, root, "git", "read-tree", "HEAD")
	testCommand(t, root, "git", "checkout-index", "-a")
	source := Source{Root: root, Revision: testCommand(t, root, "git", "rev-parse", "HEAD")}
	return source
}
func fixturePublicGRPCSource(t *testing.T, change func(*grpcSourceManifest)) (Source, map[string]string) {
	t.Helper()
	goMod := []byte("module google.golang.org/grpc\n\ngo 1.27.1\n")
	original := []byte("package credentials\n// Public upstream fixture source.\n")
	patched := []byte("package credentials\n// Explicitly recorded patched fixture source.\n")
	added := []byte("package credentials\n// Explicitly recorded new fixture source.\n")
	patch := []byte("public fixture patch\n")
	description := []byte("public fixture patch description\n")
	upstream := map[string]string{"go.mod": hash(goMod), "credentials/credentials.go": hash(original), "internal/credentials/credentials.go": hash([]byte("original public upstream"))}
	changed := grpcPatchedFile{Path: "internal/credentials/credentials.go", UpstreamSHA256: upstream["internal/credentials/credentials.go"], SHA256: hash(patched)}
	diff := grpcNewFile{Path: "WEIR_PATCH.diff", SHA256: hash(patch)}
	doc := grpcNewFile{Path: "WEIR_PATCH.md", SHA256: hash(description)}
	newMember := grpcNewFile{Path: "experimental/credentials/tls.go", SHA256: hash(added)}
	manifest := grpcSourceManifest{Module: grpcUpstreamModule, Version: grpcUpstreamVersion, UpstreamGoSum: grpcUpstreamGoSum, Files: upstream, PatchedFiles: []grpcPatchedFile{changed}, NewFiles: []grpcNewFile{diff, doc, newMember}, CopyPolicy: "Public non-test Go source fixture only"}
	if change != nil {
		change(&manifest)
	}
	files := map[string][]byte{"fixture.txt": []byte("source fixture\n"), grpcSourceRoot + "go.mod": goMod, grpcSourceRoot + "WEIR_PATCH.diff": patch, grpcSourceRoot + "WEIR_PATCH.md": description, grpcSourceRoot + "WEIR_UPSTREAM.json": mustJSON(t, manifest), grpcSourceRoot + "credentials/credentials.go": original, grpcSourceRoot + "internal/credentials/credentials.go": patched, grpcSourceRoot + "experimental/credentials/tls.go": added, grpcSourceRoot + "credentials/unlisted.go": []byte("package credentials\n")}
	inputs := make(map[string]string)
	for name, raw := range files {
		if !strings.HasSuffix(name, "/unlisted.go") {
			inputs[name] = hash(raw)
		}
	}
	return fixtureSourceTree(t, files), inputs
}
func rewriteCandidateSourceInputs(t *testing.T, bundle CandidateBundle, inputs map[string]string) CandidateBundle {
	t.Helper()
	receiptPath := filepath.Join(bundle.Directory, "weir-first", "receipt.json")
	deliveryPath := filepath.Join(bundle.Directory, "delivery.json")
	receiptRaw, err := readFile(receiptPath, maxJSON)
	if err != nil {
		t.Fatal(err)
	}
	deliveryRaw, err := readFile(deliveryPath, maxJSON)
	if err != nil {
		t.Fatal(err)
	}
	var receipt candidateReceipt
	var delivery candidateDelivery
	if err := json.Unmarshal(receiptRaw, &receipt); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(deliveryRaw, &delivery); err != nil {
		t.Fatal(err)
	}
	receipt.Inputs = inputs
	inputHash, err := candidateInputHash(inputs)
	if err != nil {
		t.Fatal(err)
	}
	image := delivery.Images["weir"]
	image.InputSHA256 = inputHash
	delivery.Images["weir"] = image
	receiptRaw, deliveryRaw = mustJSON(t, receipt), mustJSON(t, delivery)
	fixtureWrite(t, receiptPath, receiptRaw)
	fixtureWrite(t, deliveryPath, deliveryRaw)
	sumsPath := filepath.Join(bundle.Directory, "SHA256SUMS")
	sums, err := readFile(sumsPath, maxJSON)
	if err != nil {
		t.Fatal(err)
	}
	var updated strings.Builder
	for _, line := range strings.Split(strings.TrimSuffix(string(sums), "\n"), "\n") {
		digest, name, _ := strings.Cut(line, "  ")
		if name == "delivery.json" {
			digest = hash(deliveryRaw)
		}
		if name == "weir-first/receipt.json" {
			digest = hash(receiptRaw)
		}
		updated.WriteString(digest + "  " + name + "\n")
	}
	fixtureWrite(t, sumsPath, []byte(updated.String()))
	bundle.DeliverySHA256, bundle.ReceiptSHA256 = hash(deliveryRaw), hash(receiptRaw)
	return bundle
}
func TestExecutionProfileContractPublicGRPCSources(t *testing.T) {
	source, inputs := fixturePublicGRPCSource(t, nil)
	if err := verifyCandidateSourceInputs(source, inputs); err != nil {
		t.Fatal("manifest-qualified upstream/patched/new public Go source", err)
	}
	publicPath := filepath.Join(source.Root, "third_party", "grpc", "credentials", "credentials.go")
	if _, err := readFile(publicPath, maxJSON); err == nil {
		t.Fatal("generic reader gained credentials-directory exception")
	}
	profile, in := fixtureInputs(t)
	in.Server, in.Harness = source, source
	bundle := fixtureCandidate(t, profile, in)
	bundle = rewriteCandidateSourceInputs(t, bundle, inputs)
	in.Candidate = &bundle
	cli, err := readFile(in.CLI.Path, maxBinary)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyCandidate(profile, in, cli); err != nil {
		t.Fatal("complete deterministic public-source candidate", err)
	}
	unlisted := make(map[string]string)
	for name, digest := range inputs {
		unlisted[name] = digest
	}
	unlisted[grpcSourceRoot+"credentials/unlisted.go"] = hash([]byte("package credentials\n"))
	if err := verifyCandidateSourceInputs(source, unlisted); err == nil || !strings.Contains(err.Error(), "unlisted") {
		t.Fatal("unlisted tracked public source allowed", err)
	}
	for _, name := range []string{"third_party/grpc/credentials/.env.go", "third_party/grpc/internal/credentials/.ssh/client.go", "third_party/grpc/experimental/credentials/client.pem", "third_party/grpc/credentials/keyfile/client.go", "third_party/grpc/credentials/credentials/nested.go", "third_party/grpc/credentials/member_test.go", "other/credentials/member.go"} {
		if publicGRPCSourceName(name) {
			t.Fatal("secret/test/unknown descendant got public-source exemption", name)
		}
		// Put only an index entry in this owned synthetic repository. No sensitive
		// path is created or opened; classification must reject it before a read.
		oid := testCommand(t, source.Root, "git", "rev-parse", "HEAD:fixture.txt")
		testCommand(t, source.Root, "git", "update-index", "--add", "--cacheinfo", "100644,"+oid+","+name)
		selected := map[string]string{name: hash([]byte("source fixture\n"))}
		if err := verifyCandidateSourceInputs(source, selected); err == nil {
			t.Fatal("tracked secret/test/unknown path allowed", name)
		}
	}
	if err := os.Remove(publicPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(source.Root, "fixture.txt"), publicPath); err != nil {
		t.Fatal(err)
	}
	if err := verifyCandidateSourceInputs(source, inputs); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatal("listed public source symlink allowed", err)
	}
}
func TestExecutionProfileContractPublicGRPCMetadata(t *testing.T) {
	changes := map[string]func(*grpcSourceManifest){
		"module": func(m *grpcSourceManifest) { m.Module = "untrusted.example/grpc" }, "version": func(m *grpcSourceManifest) { m.Version = "v0.0.0" }, "upstream checksum": func(m *grpcSourceManifest) { m.UpstreamGoSum = "h1:wrong" },
		"patch upstream": func(m *grpcSourceManifest) { m.PatchedFiles[0].UpstreamSHA256 = strings.Repeat("f", 64) }, "patch hash": func(m *grpcSourceManifest) { m.NewFiles[0].SHA256 = strings.Repeat("f", 64) },
		"member hash": func(m *grpcSourceManifest) { m.Files["credentials/credentials.go"] = strings.Repeat("f", 64) },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			source, inputs := fixturePublicGRPCSource(t, change)
			if err := verifyCandidateSourceInputs(source, inputs); err == nil {
				t.Fatal("invalid public module/patch/member identity accepted")
			}
		})
	}
	source, inputs := fixturePublicGRPCSource(t, nil)
	manifestName := grpcSourceRoot + "WEIR_UPSTREAM.json"
	manifestPath := filepath.Join(source.Root, filepath.FromSlash(manifestName))
	raw, err := readFile(manifestPath, maxJSON)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, ' ')
	fixtureWrite(t, manifestPath, raw)
	inputs[manifestName] = hash(raw)
	if err := verifyCandidateSourceInputs(source, inputs); err == nil || !strings.Contains(err.Error(), "declared source revision") {
		t.Fatal("working metadata plus receipt self-assertion bypassed Git snapshot", err)
	}
	source, inputs = fixturePublicGRPCSource(t, nil)
	delete(inputs, manifestName)
	if err := verifyCandidateSourceInputs(source, inputs); err == nil {
		t.Fatal("public member without receipt metadata allowed")
	}
}
