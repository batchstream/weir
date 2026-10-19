package testexecution

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixtureCandidate(t *testing.T, p ExecutionProfile, in ProvenanceInputs) CandidateBundle {
	t.Helper()
	directory := fixtureRoot(t)
	binaries := make(map[string]string)
	archives := make(map[string]string)
	cli, err := os.ReadFile(in.CLI.Path)
	if err != nil {
		t.Fatal(err)
	}
	sums := make(map[string]string)
	for _, target := range candidateTargets {
		content := []byte("other target fixture")
		if target == p.CLI.OS+"-"+p.CLI.Arch {
			content = cli
		}
		archive := testArchive(t, content, target, nil)
		name := "weir-" + target + ".tar.gz"
		fixtureWrite(t, filepath.Join(directory, "weir-first", name), archive)
		binaries[target] = hash(content)
		archives[name] = hash(archive)
		sums["weir-first/"+name] = hash(archive)
		linked := []byte(`{"fixture":"linked metadata is a byte input, not an attestation"}`)
		fixtureWrite(t, filepath.Join(directory, "weir-first", target+"-linked.json"), linked)
		sums["weir-first/"+target+"-linked.json"] = hash(linked)
	}
	source, err := os.ReadFile(filepath.Join(in.Server.Root, "fixture.txt"))
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	ociDigest := "sha256:" + digest
	base := candidateBase{Image: "example.invalid/base", Index: ociDigest, Platforms: map[string]string{"amd64": ociDigest, "arm64": ociDigest}}
	platforms := make(map[string]candidatePlatform)
	for _, arch := range []string{"amd64", "arm64"} {
		platform := candidatePlatform{BinarySHA256: binaries["linux-"+arch], Config: ociDigest, Manifest: ociDigest, Layers: []string{ociDigest, ociDigest}}
		platforms[arch] = platform
	}
	oci := candidateOCI{Index: ociDigest, OuterIndexSHA256: digest, Images: platforms}
	receipt := candidateReceipt{Source: in.Server.Revision, Archives: archives, Binaries: binaries, Inputs: map[string]string{"fixture.txt": hash(source)}, Base: base, Epoch: 1, Go: "go1.27.1", Flags: []string{"-buildvcs=false"}, OCI: oci}
	inputHash, err := candidateInputHash(receipt.Inputs)
	if err != nil {
		t.Fatal(err)
	}
	image := candidateImage{Binaries: binaries, InputSHA256: inputHash, OCI: oci}
	delivery := candidateDelivery{Source: in.Server.Revision, Images: map[string]candidateImage{"weir": image}, Base: base, BaseLayers: map[string][]string{"amd64": {ociDigest}, "arm64": {ociDigest}}}
	receiptRaw, deliveryRaw := mustJSON(t, receipt), mustJSON(t, delivery)
	fixtureWrite(t, filepath.Join(directory, "delivery.json"), deliveryRaw)
	sums["delivery.json"] = hash(deliveryRaw)
	fixtureWrite(t, filepath.Join(directory, "weir-first", "receipt.json"), receiptRaw)
	sums["weir-first/receipt.json"] = hash(receiptRaw)
	graph := []byte(`{"fixture":"graph"}`)
	fixtureWrite(t, filepath.Join(directory, "weir-first", "module-graph.json"), graph)
	sums["weir-first/module-graph.json"] = hash(graph)
	var checksum strings.Builder
	for name, digest := range sums {
		checksum.WriteString(digest + "  " + name + "\n")
	}
	fixtureWrite(t, filepath.Join(directory, "SHA256SUMS"), []byte(checksum.String()))
	bundle := CandidateBundle{Directory: directory, DeliverySHA256: hash(deliveryRaw), ReceiptSHA256: hash(receiptRaw)}
	return bundle
}
func TestExecutionProfileContractCandidateBytes(t *testing.T) {
	if example := os.Getenv("WEIR_EXECUTION_CANDIDATE_EXAMPLE"); example != "" {
		t.Run("external complete candidate source and bytes", func(t *testing.T) { checkRetainedExample(t, example) })
	}
	p, in := fixtureInputs(t)
	bundle := fixtureCandidate(t, p, in)
	in.Candidate = &bundle
	if _, err := Validate(p, in); err != nil {
		t.Fatal(err)
	}
	// A supplied delivery index never enables an executor or proves registry bytes.
	raw, err := os.ReadFile(in.CLI.Path)
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*ProvenanceInputs){
		"wrong delivery hash":    func(i *ProvenanceInputs) { i.Candidate.DeliverySHA256 = strings.Repeat("a", 64) },
		"wrong receipt hash":     func(i *ProvenanceInputs) { i.Candidate.ReceiptSHA256 = strings.Repeat("a", 64) },
		"wrong source":           func(i *ProvenanceInputs) { i.Server.Revision = strings.Repeat("b", 40) },
		"wrong target CLI bytes": func(i *ProvenanceInputs) { i.CLI.SHA256 = strings.Repeat("c", 64) },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			bad := in
			candidate := bundle
			bad.Candidate = &candidate
			change(&bad)
			if err := verifyCandidate(p, bad, raw); err == nil {
				t.Fatal("accepted tampered candidate")
			}
		})
	}
	archive := filepath.Join(bundle.Directory, "weir-first", "weir-"+p.CLI.OS+"-"+p.CLI.Arch+".tar.gz")
	original, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	fixtureWrite(t, archive, append(original, 0))
	if err := verifyCandidate(p, in, raw); err == nil {
		t.Fatal("accepted modified archive bytes")
	}
	fixtureWrite(t, archive, original)
	// Rehashing the archive/envelope must not conceal a changed selected member.
	memberChanged := testArchive(t, []byte("tampered CLI member"), p.CLI.OS+"-"+p.CLI.Arch, nil)
	fixtureWrite(t, archive, memberChanged)
	path := filepath.Join(bundle.Directory, "weir-first", "receipt.json")
	receiptRaw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var receipt candidateReceipt
	if err := json.Unmarshal(receiptRaw, &receipt); err != nil {
		t.Fatal(err)
	}
	archiveName := "weir-" + p.CLI.OS + "-" + p.CLI.Arch + ".tar.gz"
	receipt.Archives[archiveName] = hash(memberChanged)
	memberReceipt := mustJSON(t, receipt)
	fixtureWrite(t, path, memberReceipt)
	memberBundle := bundle
	memberBundle.ReceiptSHA256 = hash(memberReceipt)
	memberInput := in
	memberInput.Candidate = &memberBundle
	if err := verifyCandidate(p, memberInput, raw); err == nil || !strings.Contains(err.Error(), "archived CLI bytes differ") {
		t.Fatal("member tampering not rejected at byte comparison", err)
	}
	fixtureWrite(t, archive, original)
	if err := json.Unmarshal(receiptRaw, &receipt); err != nil {
		t.Fatal(err)
	}
	receipt.Inputs = map[string]string{"../escape": strings.Repeat("a", 64)}
	changed := mustJSON(t, receipt)
	fixtureWrite(t, path, changed)
	badBundle := bundle
	badBundle.ReceiptSHA256 = hash(changed)
	bad := in
	bad.Candidate = &badBundle
	if err := verifyCandidate(p, bad, raw); err == nil {
		t.Fatal("accepted escaped candidate source input")
	}
	fixtureWrite(t, path, receiptRaw)
	link := filepath.Join(fixtureRoot(t), "candidate")
	if err := os.Symlink(bundle.Directory, link); err != nil {
		t.Fatal(err)
	}
	badBundle = bundle
	badBundle.Directory = link
	bad.Candidate = &badBundle
	if err := verifyCandidate(p, bad, raw); err == nil {
		t.Fatal("accepted symlink candidate root")
	}
}
func checkRetainedExample(t *testing.T, directory string) {
	t.Helper()
	deliveryRaw, err := readFile(filepath.Join(directory, "delivery.json"), maxJSON)
	if err != nil {
		t.Fatal(err)
	}
	receiptRaw, err := readFile(filepath.Join(directory, "weir-first", "receipt.json"), maxJSON)
	if err != nil {
		t.Fatal(err)
	}
	var receipt candidateReceipt
	if err := decode(receiptRaw, &receipt); err != nil {
		t.Fatal(err)
	}
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	repository, err := gitOutput(working, "rev-parse", "--show-toplevel")
	if err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(fixtureRoot(t), "source")
	testCommand(t, working, "git", "clone", "--quiet", "--no-checkout", "--shared", repository, snapshot)
	testCommand(t, snapshot, "git", "checkout", "--quiet", "--detach", receipt.Source)
	source := Source{Root: snapshot, Revision: receipt.Source}
	if err := verifySource(source); err != nil {
		t.Fatal(err)
	}
	profile, err := LegacyProfile("legacy-desktop-load-v1")
	if err != nil {
		t.Fatal(err)
	}
	archive, err := readFile(filepath.Join(directory, "weir-first", "weir-linux-arm64.tar.gz"), maxBinary)
	if err != nil {
		t.Fatal(err)
	}
	cliBytes := readArchiveCLIFixture(t, archive, "linux-arm64")
	cli := Executable{Role: "cli", SHA256: hash(cliBytes)}
	bundle := CandidateBundle{Directory: directory, DeliverySHA256: hash(deliveryRaw), ReceiptSHA256: hash(receiptRaw)}
	inputs := ProvenanceInputs{Server: source, CLI: cli, Candidate: &bundle}
	if err := verifyCandidate(profile, inputs, cliBytes); err != nil {
		t.Fatal("real candidate complete source/metadata/archive path", err)
	}
	t.Logf("actual verifyCandidate source-input path accepted %d inputs at declared source snapshot; no running CLI, ValidatedInputs, registry/layer retrieval, origin proof or runtime qualification", len(receipt.Inputs))
}
func TestExecutionProfileContractLoadBoundary(t *testing.T) {
	p, in := fixtureInputs(t)
	root := fixtureRoot(t)
	profilePath, provenancePath := filepath.Join(root, "profile.json"), filepath.Join(root, "provenance.json")
	fixtureWrite(t, profilePath, mustJSON(t, p))
	fixtureWrite(t, provenancePath, mustJSON(t, in))
	opts := LoadOptions{ProfilePath: profilePath, ProvenancePath: provenancePath, Suite: p.Suite}
	validated, err := Load(opts)
	if err != nil {
		t.Fatal(err)
	}
	identity := map[string]string{"product": "weir", "version": "local-" + in.Server.Revision, "revision": in.Server.Revision, "state": "clean-commit", "dirty": "false", "target": p.CLI.String(), "go": "go1.27.1"}
	if err := validated.CheckVersion(mustJSON(t, identity)); err != nil {
		t.Fatal(err)
	}
	identity["revision"] = strings.Repeat("a", 40)
	if err := validated.CheckVersion(mustJSON(t, identity)); err == nil {
		t.Fatal("accepted version source mismatch")
	}
	var callbacks int
	createOnlyIfExecutable := func(opts LoadOptions) error {
		v, err := Load(opts)
		if err != nil {
			return err
		}
		if err := v.RequireExecutable(); err != nil {
			return err
		}
		callbacks++
		return nil
	}
	cases := []LoadOptions{opts, opts, opts, opts}
	cases[0].ProfilePath = ""
	cases[1].ProvenancePath = ""
	cases[2].Suite = "wrong"
	cases[3].LegacyEnv = map[string]string{"WEIR_QUALIFICATION_BINARY": "/wrong"}
	for _, bad := range cases {
		if err := createOnlyIfExecutable(bad); err == nil {
			t.Fatal("accepted missing/conflicting boundary input")
		}
	}
	if callbacks != 0 {
		t.Fatal("invalid inputs reached fixture callback")
	}
	if p.Executor == LinuxExecutor {
		if err := createOnlyIfExecutable(opts); err == nil || callbacks != 0 {
			t.Fatal("unsupported Linux executor reached fixture callback")
		}
	}
	// Reject direct self-attestation even with otherwise complete provenance.
	claimed := bytes.Replace(mustJSON(t, in), []byte(`"schemaVersion":1`), []byte(`"schemaVersion":1,"verified":true`), 1)
	fixtureWrite(t, provenancePath, claimed)
	if _, err := Load(opts); err == nil {
		t.Fatal("accepted complete self-attested provenance")
	}
}
