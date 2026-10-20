// testvolume proves only the owned config-volume filesystem prerequisite.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var sourceRevision string
var identityPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var ownerPattern = regexp.MustCompile(`^weir-volume-[0-9a-f]{32}$`)

const configPath = "/usr/share/opensearch/config"
const capacity = 16 << 20

var publicFiles = []string{"opensearch.yml", "jvm.options", "log4j2.properties"}

type options struct {
	Mode     string
	Role     string
	Owner    string
	Revision string
	Helper   string
	Image    string
	TTL      time.Duration
}

type buildProof struct {
	Schema   int    `json:"schema"`
	Stage    string `json:"stage"`
	Revision string `json:"revision"`
	SHA      string `json:"helper_sha256"`
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	Go       string `json:"go"`
}

type fileProof struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	SHA  string `json:"sha256"`
}

type filesystemProof struct {
	RootReadonly   bool     `json:"root_readonly"`
	HelperReadonly bool     `json:"helper_readonly"`
	NoSharedMemory bool     `json:"no_shared_memory"`
	Magic          int64    `json:"magic"`
	Capacity       uint64   `json:"capacity"`
	Mount          string   `json:"mount"`
	Device         string   `json:"device"`
	Options        []string `json:"options"`
}

type processProof struct {
	PID        int    `json:"pid"`
	Start      uint64 `json:"start_ticks"`
	SHA        string `json:"sha256"`
	Executable string `json:"executable"`
	UID        int    `json:"uid"`
	GID        int    `json:"gid"`
	Membership string `json:"membership"`
	CPU        string `json:"cpu_max"`
	Memory     uint64 `json:"memory_max"`
	Swap       uint64 `json:"memory_swap_max"`
	PIDs       uint64 `json:"pids_max"`
	CapEff     string `json:"cap_eff"`
	NoNewPrivs string `json:"no_new_privs"`
}

type record struct {
	Schema        int             `json:"schema"`
	Stage         string          `json:"stage"`
	Role          string          `json:"role"`
	Owner         string          `json:"owner"`
	Revision      string          `json:"revision"`
	Helper        string          `json:"helper_sha256"`
	Image         string          `json:"image"`
	Arch          string          `json:"arch"`
	Process       processProof    `json:"process"`
	Filesystem    filesystemProof `json:"filesystem"`
	Files         []fileProof     `json:"files"`
	Written       int             `json:"fill_bytes"`
	Attempts      int             `json:"fill_attempts"`
	ENOSPC        bool            `json:"enospc"`
	RoundTrip     bool            `json:"round_trip"`
	Preserved     bool            `json:"preserved"`
	Qualification bool            `json:"qualification"`
}

func boundedRead(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if len(raw) > 64<<10 {
		return "", errors.New("observation exceeds64KiB")
	}
	return strings.TrimSpace(string(raw)), err
}

func hashFile(file *os.File, limit int64) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(file, limit+1))
	if n > limit {
		return "", n, errors.New("file exceeds bound")
	}
	return hex.EncodeToString(h.Sum(nil)), n, err
}

func publicConfig(root string) ([]fileProof, error) {
	files := make([]fileProof, 0, len(publicFiles))
	for _, name := range publicFiles {
		file, err := openPublic(root + "/" + name)
		if err != nil {
			return nil, fmt.Errorf("public config %s: %w", name, err)
		}
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, err
		}
		if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 1<<20 {
			file.Close()
			return nil, errors.New("public config is not a bounded nonempty regular file")
		}
		sha, size, err := hashFile(file, 1<<20)
		file.Close()
		if err != nil || size != info.Size() {
			return nil, errors.New("public config changed or unreadable")
		}
		proof := fileProof{Name: name, Size: size, SHA: sha}
		files = append(files, proof)
	}
	return files, nil
}

func newOwned(root, suffix string, opts options) (*os.File, string, error) {
	path := root + "/." + opts.Owner + "-" + suffix
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	return file, path, err
}

func roundTrip(root string, opts options) error {
	file, path, err := newOwned(root, "roundtrip", opts)
	if err != nil {
		return err
	}
	defer file.Close()
	defer os.Remove(path)
	payload := []byte(opts.Owner)
	if _, err = file.Write(payload); err != nil {
		return err
	}
	if _, err = file.Seek(0, 0); err != nil {
		return err
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(len(payload)+1)))
	if err != nil || string(data) != string(payload) {
		return errors.New("owned write/read mismatch")
	}
	// Reserve the destination exclusively too; rename can replace only our file.
	destination, renamed, err := newOwned(root, "renamed", opts)
	if err != nil {
		return err
	}
	destination.Close()
	defer os.Remove(renamed)
	if err = os.Rename(path, renamed); err != nil {
		return err
	}
	if err = os.Remove(renamed); err != nil {
		return err
	}
	return nil
}

func fill(root string, opts options) (int, int, error) {
	file, path, err := newOwned(root, "fill", opts)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()
	defer os.Remove(path)
	total, attempts, err := boundedFill(file)
	if err != nil {
		return total, attempts, err
	}
	if err = file.Close(); err != nil {
		return total, attempts, err
	}
	if err = os.Remove(path); err != nil {
		return total, attempts, err
	}
	return total, attempts, nil
}

func boundedFill(writer io.Writer) (int, int, error) {
	chunk := make([]byte, 64<<10)
	total := 0
	for attempt := 1; attempt <= 273; attempt++ {
		n, writeErr := writer.Write(chunk)
		total += n
		if errors.Is(writeErr, syscall.ENOSPC) {
			if total <= 0 || total > capacity {
				return total, attempt, errors.New("invalid ENOSPC bound")
			}
			return total, attempt, nil
		}
		if writeErr != nil {
			return total, attempt, writeErr
		}
		if n != len(chunk) {
			return total, attempt, io.ErrShortWrite
		}
	}
	return total, 273, errors.New("bounded fill did not reach ENOSPC")
}

func validate(opts options) error {
	if opts.Mode != "holder" && opts.Mode != "probe" && opts.Mode != "observe" && opts.Mode != "copy" {
		return errors.New("invalid helper mode")
	}
	if opts.Role != "holder" && opts.Role != "probe" {
		return errors.New("invalid helper role")
	}
	if opts.Mode != "observe" && !(opts.Mode == "copy" && opts.Role == "holder") && opts.Mode != opts.Role {
		return errors.New("mode/role mismatch")
	}
	if !ownerPattern.MatchString(opts.Owner) || !identityPattern.MatchString(opts.Helper) || !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(opts.Revision) || opts.Revision != sourceRevision {
		return errors.New("invalid owner/source/helper identity")
	}
	if !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(opts.Image) {
		return errors.New("invalid image identity")
	}
	if opts.TTL <= 0 || opts.TTL > 300*time.Second || opts.Role == "probe" && opts.TTL > 55*time.Second {
		return errors.New("invalid finite TTL")
	}
	return nil
}

func waitForStop(ttl time.Duration, stop <-chan os.Signal) error {
	timer := time.NewTimer(ttl)
	defer timer.Stop()
	select {
	case <-stop:
		return nil
	case <-timer.C:
		return errors.New("finite helper TTL expired without controlled shutdown")
	}
}

func loadedBuild(opts options) (buildProof, error) {
	result := buildProof{Schema: 1, Stage: "identity", Revision: sourceRevision, OS: runtime.GOOS, Arch: runtime.GOARCH, Go: runtime.Version()}
	if opts.Revision != sourceRevision || !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(opts.Revision) || !identityPattern.MatchString(opts.Helper) {
		return result, errors.New("embedded source/helper identity mismatch")
	}
	if opts.Role != "" || opts.Owner != "" || opts.Image != "" || opts.TTL != 0 {
		return result, errors.New("identity mode cannot perform fixture operations")
	}
	file, err := os.Open("/proc/self/exe")
	if err != nil {
		return result, err
	}
	defer file.Close()
	result.SHA, _, err = hashFile(file, 128<<20)
	if err != nil || result.SHA != opts.Helper {
		return result, errors.New("loaded admission executable SHA mismatch")
	}
	return result, nil
}

func execute(opts options) error {
	// This check precedes all filesystem access on unsupported OSes.
	if err := supported(); err != nil {
		return err
	}
	if opts.Mode == "identity" {
		proof, err := loadedBuild(opts)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(proof)
	}
	if err := validate(opts); err != nil {
		return err
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(stop)
	started := time.Now()
	process, err := observeProcess()
	if err != nil {
		return err
	}
	if process.SHA != opts.Helper || process.Executable != "/weir-volume-helper" || process.UID != 1000 || process.GID != 1000 {
		return errors.New("actual loaded helper identity mismatch")
	}
	root := configPath
	if opts.Role == "holder" {
		root = "/held"
	}
	fs, err := filesystem(root, opts.Role == "holder")
	if err != nil {
		return err
	}
	result := record{Schema: 1, Stage: opts.Mode, Role: opts.Role, Owner: opts.Owner, Revision: opts.Revision, Helper: opts.Helper, Image: opts.Image, Arch: runtime.GOARCH, Process: process, Filesystem: fs, Files: make([]fileProof, 0)}
	if opts.Mode == "holder" {
		entries, readErr := os.ReadDir(root)
		if readErr != nil || len(entries) != 0 {
			return errors.New("holder requires empty readonly mount before copy-up")
		}
	}
	if opts.Mode == "copy" {
		result.Files, err = publicConfig(root)
		if err != nil {
			return err
		}
	}
	if opts.Mode == "probe" {
		result.Files, err = publicConfig(root)
		if err != nil {
			return err
		}
		if err = roundTrip(root, opts); err != nil {
			return err
		}
		result.RoundTrip = true
		result.Written, result.Attempts, err = fill(root, opts)
		if err != nil {
			return err
		}
		result.ENOSPC = true
		after, readErr := publicConfig(root)
		if readErr != nil || !sameFiles(result.Files, after) {
			return errors.New("public inputs changed during owned writes")
		}
		result.Preserved = true
	}
	if err = json.NewEncoder(os.Stdout).Encode(result); err != nil {
		return err
	}
	if opts.Mode == "observe" || opts.Mode == "copy" {
		return nil
	}
	remaining := opts.TTL - time.Since(started)
	if remaining <= 0 {
		return errors.New("finite helper deadline exceeded")
	}
	return waitForStop(remaining, stop)
}

func sameFiles(a, b []fileProof) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func main() {
	var opts options
	flag.StringVar(&opts.Mode, "mode", "", "identity, holder, copy, probe or observe")
	flag.StringVar(&opts.Role, "role", "", "holder or probe")
	flag.StringVar(&opts.Owner, "owner", "", "owned run identity")
	flag.StringVar(&opts.Revision, "revision", "", "exact clean source revision")
	flag.StringVar(&opts.Helper, "helper-sha256", "", "known helper SHA256")
	flag.StringVar(&opts.Image, "image", "", "local pinned image config identity")
	flag.DurationVar(&opts.TTL, "ttl", 0, "finite lifetime")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected helper arguments")
		os.Exit(1)
	}
	if err := execute(opts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func scalar(path string) (uint64, error) {
	raw, err := boundedRead(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(raw, 10, 64)
}
