package testexecution

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"unicode/utf16"
)

type candidateBase struct {
	Image     string            `json:"image"`
	Index     string            `json:"index"`
	Platforms map[string]string `json:"platforms"`
}
type candidatePlatform struct {
	BinarySHA256 string   `json:"binary_sha256"`
	Config       string   `json:"config"`
	Layers       []string `json:"layers"`
	Manifest     string   `json:"manifest"`
}
type candidateOCI struct {
	Images           map[string]candidatePlatform `json:"images"`
	Index            string                       `json:"index"`
	OuterIndexSHA256 string                       `json:"outer_index_sha256,omitempty"`
}
type candidateImage struct {
	Binaries    map[string]string `json:"binaries"`
	InputSHA256 string            `json:"input_sha256"`
	OCI         candidateOCI      `json:"oci"`
}
type candidateDelivery struct {
	Base       candidateBase             `json:"base"`
	BaseLayers map[string][]string       `json:"base_layers"`
	Images     map[string]candidateImage `json:"images"`
	Source     string                    `json:"source"`
}
type candidateReceipt struct {
	Archives map[string]string `json:"archives"`
	Base     candidateBase     `json:"base"`
	Binaries map[string]string `json:"binaries"`
	Epoch    int64             `json:"epoch"`
	Flags    []string          `json:"flags"`
	Go       string            `json:"go"`
	Inputs   map[string]string `json:"inputs"`
	OCI      candidateOCI      `json:"oci"`
	Source   string            `json:"source"`
}

var candidateTargets = []string{"darwin-amd64", "darwin-arm64", "linux-amd64", "linux-arm64"}
var archiveMembers = []string{"README.md", "config/routes.reference.yaml", "config/weir.reference.yaml", "licenses/golua/LICENSE", "licenses/golua/WEIR_PATCH.diff", "licenses/golua/WEIR_PATCH.md", "licenses/golua/WEIR_UPSTREAM.json", "licenses/grpc/AUTHORS", "licenses/grpc/LICENSE", "licenses/grpc/WEIR_PATCH.diff", "licenses/grpc/WEIR_PATCH.md", "licenses/grpc/WEIR_UPSTREAM.json", "weir"}

func verifyCandidate(profile ExecutionProfile, in ProvenanceInputs, cli []byte) error {
	bundle := in.Candidate
	if err := canonicalPath(bundle.Directory); err != nil {
		return err
	}
	if err := checkCandidateFiles(bundle.Directory); err != nil {
		return err
	}
	deliveryRaw, err := readFile(filepath.Join(bundle.Directory, "delivery.json"), maxJSON)
	if err != nil {
		return err
	}
	receiptRaw, err := readFile(filepath.Join(bundle.Directory, "weir-first", "receipt.json"), maxJSON)
	if err != nil {
		return err
	}
	if hash(deliveryRaw) != bundle.DeliverySHA256 || hash(receiptRaw) != bundle.ReceiptSHA256 {
		return fmt.Errorf("candidate delivery/receipt byte digest mismatch")
	}
	var delivery candidateDelivery
	var receipt candidateReceipt
	if err := decode(deliveryRaw, &delivery); err != nil {
		return err
	}
	if err := decode(receiptRaw, &receipt); err != nil {
		return err
	}
	image, ok := delivery.Images["weir"]
	if !ok || len(delivery.Images) != 1 || delivery.Source != in.Server.Revision || receipt.Source != delivery.Source || !reflect.DeepEqual(delivery.Base, receipt.Base) || !reflect.DeepEqual(image.Binaries, receipt.Binaries) || !reflect.DeepEqual(image.OCI, receipt.OCI) {
		return fmt.Errorf("candidate source/binary/receipt mapping mismatch")
	}
	inputHash, err := candidateInputHash(receipt.Inputs)
	if err != nil || image.InputSHA256 != inputHash {
		return fmt.Errorf("candidate delivery source-input map hash mismatch")
	}
	if err := validateCandidateMapping(delivery, receipt); err != nil {
		return err
	}
	if len(receipt.Archives) != 4 || len(receipt.Binaries) != 4 || len(receipt.Inputs) == 0 || len(receipt.Inputs) > 4096 {
		return fmt.Errorf("candidate file set incomplete or exceeds bound")
	}
	if err := verifyCandidateSourceInputs(in.Server, receipt.Inputs); err != nil {
		return err
	}
	sums, err := readFile(filepath.Join(bundle.Directory, "SHA256SUMS"), maxJSON)
	if err != nil {
		return err
	}
	expectedSums := map[string]string{"delivery.json": hash(deliveryRaw), "weir-first/receipt.json": hash(receiptRaw)}
	graph, err := readFile(filepath.Join(bundle.Directory, "weir-first", "module-graph.json"), maxJSON)
	if err != nil {
		return err
	}
	expectedSums["weir-first/module-graph.json"] = hash(graph)
	target := profile.CLI.OS + "-" + profile.CLI.Arch
	for _, platform := range candidateTargets {
		archiveName := "weir-" + platform + ".tar.gz"
		archiveDigest, archiveOK := receipt.Archives[archiveName]
		binaryDigest, binaryOK := receipt.Binaries[platform]
		if !archiveOK || !binaryOK || !digestPattern.MatchString(archiveDigest) || !digestPattern.MatchString(binaryDigest) {
			return fmt.Errorf("candidate target/archive missing")
		}
		raw, err := readFile(filepath.Join(bundle.Directory, "weir-first", archiveName), maxBinary)
		if err != nil {
			return err
		}
		if hash(raw) != archiveDigest {
			return fmt.Errorf("candidate archive hash mismatch")
		}
		actual, err := archiveBinaryHash(raw, platform)
		if err != nil {
			return err
		}
		if actual != binaryDigest || (platform == target && actual != hash(cli)) {
			return fmt.Errorf("candidate archived CLI bytes differ")
		}
		expectedSums["weir-first/"+archiveName] = archiveDigest
		linked, err := readFile(filepath.Join(bundle.Directory, "weir-first", platform+"-linked.json"), maxJSON)
		if err != nil {
			return err
		}
		expectedSums["weir-first/"+platform+"-linked.json"] = hash(linked)
	}
	if receipt.Binaries[target] != in.CLI.SHA256 {
		return fmt.Errorf("selected candidate target differs from actual CLI")
	}
	seen := make(map[string]bool)
	for _, line := range strings.Split(strings.TrimSuffix(string(sums), "\n"), "\n") {
		fields := strings.Split(line, "  ")
		if len(fields) != 2 || seen[fields[1]] || expectedSums[fields[1]] != fields[0] {
			return fmt.Errorf("candidate checksum file mismatch/duplicate")
		}
		seen[fields[1]] = true
	}
	if len(seen) != len(expectedSums) {
		return fmt.Errorf("incomplete candidate checksum file")
	}
	// OCI index, platform manifests, image configs, and layers are receipt claims
	// only. No OCI bytes/registry are available here; none confer runtime proof.
	return nil
}

// Match the retained delivery's json.dumps(inputs, sort_keys=True) encoding.
// This is a layout checksum, not a signature or trusted-origin assertion.
func candidateInputHash(inputs map[string]string) (string, error) {
	keys := make([]string, 0, len(inputs))
	for key := range inputs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var result strings.Builder
	result.WriteByte('{')
	encode := func(value string) (string, error) {
		var raw bytes.Buffer
		encoder := json.NewEncoder(&raw)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(value); err != nil {
			return "", err
		}
		var ascii strings.Builder
		for _, char := range strings.TrimSuffix(raw.String(), "\n") {
			if char < 128 {
				ascii.WriteRune(char)
			} else if char <= 0xffff {
				fmt.Fprintf(&ascii, `\u%04x`, char)
			} else {
				hi, lo := utf16.EncodeRune(char)
				fmt.Fprintf(&ascii, `\u%04x\u%04x`, hi, lo)
			}
		}
		return ascii.String(), nil
	}
	for i, key := range keys {
		if i > 0 {
			result.WriteString(", ")
		}
		name, err := encode(key)
		if err != nil {
			return "", err
		}
		value, err := encode(inputs[key])
		if err != nil {
			return "", err
		}
		result.WriteString(name + ": " + value)
	}
	result.WriteByte('}')
	return hash([]byte(result.String())), nil
}
func validateCandidateMapping(delivery candidateDelivery, receipt candidateReceipt) error {
	image := delivery.Images["weir"]
	ociDigest := func(value string) bool {
		return strings.HasPrefix(value, "sha256:") && digestPattern.MatchString(strings.TrimPrefix(value, "sha256:"))
	}
	if receipt.Epoch <= 0 || receipt.Go == "" || len(receipt.Flags) == 0 || receipt.Base.Image == "" || !ociDigest(receipt.Base.Index) || !digestPattern.MatchString(image.InputSHA256) || !ociDigest(image.OCI.Index) || !digestPattern.MatchString(image.OCI.OuterIndexSHA256) || len(image.OCI.Images) != 2 || len(delivery.BaseLayers) != 2 || len(receipt.Base.Platforms) != 2 {
		return fmt.Errorf("candidate required build/OCI metadata missing or invalid")
	}
	for _, arch := range []string{"amd64", "arm64"} {
		platform, ok := image.OCI.Images[arch]
		layers := delivery.BaseLayers[arch]
		if !ok || !ociDigest(receipt.Base.Platforms[arch]) || platform.BinarySHA256 != receipt.Binaries["linux-"+arch] || !ociDigest(platform.Manifest) || !ociDigest(platform.Config) || len(layers) == 0 || len(layers) > 64 || len(platform.Layers) != len(layers)+1 {
			return fmt.Errorf("candidate platform identity mapping incomplete")
		}
		for i, layer := range platform.Layers {
			if !ociDigest(layer) || (i < len(layers) && layer != layers[i]) {
				return fmt.Errorf("candidate layer receipt mapping mismatch")
			}
		}
	}
	return nil
}
func checkCandidateFiles(directory string) error {
	rootExpected := map[string]bool{"delivery.json": false, "SHA256SUMS": false, "weir-first": true}
	productExpected := map[string]bool{"receipt.json": false, "module-graph.json": false}
	for _, target := range candidateTargets {
		productExpected["weir-"+target+".tar.gz"] = false
		productExpected[target+"-linked.json"] = false
	}
	if err := checkDirectory(directory, rootExpected); err != nil {
		return err
	}
	return checkDirectory(filepath.Join(directory, "weir-first"), productExpected)
}
func checkDirectory(directory string, expected map[string]bool) error {
	if err := canonicalPath(directory); err != nil {
		return err
	}
	folder, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer folder.Close()
	entries, err := folder.ReadDir(len(expected) + 1)
	if err != nil && err != io.EOF {
		return err
	}
	if len(entries) != len(expected) {
		return fmt.Errorf("candidate directory file set mismatch")
	}
	for _, entry := range entries {
		directory, ok := expected[entry.Name()]
		if !ok || entry.IsDir() != directory || entry.Type()&os.ModeSymlink != 0 || (!directory && !entry.Type().IsRegular()) {
			return fmt.Errorf("unexpected candidate file/directory/link %q", entry.Name())
		}
	}
	return nil
}
func safeRelative(name string) bool {
	return name != "" && name != "." && !strings.Contains(name, "\\") && path.Clean(name) == name && !strings.HasPrefix(name, "/") && name != ".." && !strings.HasPrefix(name, "../")
}
func sensitivePath(name string) bool {
	for _, part := range strings.Split(name, "/") {
		part = strings.ToLower(part)
		if strings.HasPrefix(part, ".env") || part == ".ssh" || part == "credentials" || part == "keyfile" || strings.HasSuffix(part, ".pem") || strings.HasSuffix(part, ".key") {
			return true
		}
	}
	return false
}
func membersForTarget(target string) ([]string, error) {
	members := append([]string(nil), archiveMembers...)
	switch target {
	case "linux-amd64", "linux-arm64":
	case "darwin-amd64", "darwin-arm64":
		members = append(members, "licenses/purego-Apache-2.0.txt", "licenses/purego-Go-BSD-3-Clause.txt", "licenses/purego-NOTICE.txt")
	default:
		return nil, fmt.Errorf("unknown candidate archive target")
	}
	return members, nil
}
func archiveBinaryHash(raw []byte, target string) (string, error) {
	compressed := bytes.NewReader(raw)
	zip, err := gzip.NewReader(compressed)
	if err != nil {
		return "", err
	}
	defer zip.Close()
	zip.Multistream(false)
	limited := &io.LimitedReader{R: zip, N: 192 << 20}
	reader := tar.NewReader(limited)
	seen := make(map[string]bool)
	allowed := make(map[string]bool)
	members, err := membersForTarget(target)
	if err != nil {
		return "", err
	}
	for _, name := range members {
		allowed[name] = true
	}
	binaryHash := ""
	for {
		member, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		if !safeRelative(member.Name) || !allowed[member.Name] || seen[member.Name] || member.Typeflag != tar.TypeReg || member.Linkname != "" || member.Format != tar.FormatUSTAR || member.Size < 0 || member.Size > maxBinary || len(member.PAXRecords) != 0 {
			return "", fmt.Errorf("unsafe, duplicate or unsupported candidate tar member %q", member.Name)
		}
		seen[member.Name] = true
		digest := sha256.New()
		n, err := io.Copy(digest, reader)
		if err != nil || n != member.Size {
			return "", fmt.Errorf("truncated candidate member")
		}
		if member.Name == "weir" {
			if member.Mode&0111 == 0 {
				return "", fmt.Errorf("archive CLI is not executable")
			}
			binaryHash = hex.EncodeToString(digest.Sum(nil))
		}
	}
	if len(seen) != len(allowed) {
		return "", fmt.Errorf("candidate archive file set incomplete")
	}
	// tar EOF is not gzip EOF. Drain bounded zero padding to check gzip CRC and
	// reject concatenated gzip streams, hidden payloads, and decompression bombs.
	padding, err := io.ReadAll(io.LimitReader(limited, (1<<20)+1))
	if err != nil {
		return "", err
	}
	if len(padding) > 1<<20 || limited.N == 0 || len(bytes.Trim(padding, "\x00")) != 0 || compressed.Len() != 0 {
		return "", fmt.Errorf("candidate archive trailing data or size bound")
	}
	return binaryHash, nil
}
