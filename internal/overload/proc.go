package overload

import (
	"errors"
	"io"
	"math/bits"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

const maxLevels = 32
const maxMountBytes = 128 << 10
const maxCgroupBytes = 16 << 10

var errProfile = errors.New("unsupported memory profile")

type memoryLevel struct {
	name           string
	limit          uint64
	finite, absent bool
}

// Linux parsing is kept portable so offline race tests can exercise real owned
// files on every development platform. Only memory_linux.go selects /proc.
// After discovery, topology and limits are static. Changes latch until restart.
type memoryProfile struct {
	proc               string
	membership, mounts string
	mount              string
	levels             []memoryLevel
	changed            bool
}

func (p *memoryProfile) observe() observation {
	cg := CgroupSnapshot{State: "not_applicable", Scope: "none"}
	o := observation{source: "go_sys_minus_released", processValid: true, cgroup: cg, low: true}
	if p.proc == "" {
		o.bytes = goBytes()
		return o
	}
	o.processValid = false
	raw, err := readFile(filepath.Join(p.proc, "statm"), 256)
	if err == nil {
		n, err := parseRSS(raw, uint64(os.Getpagesize()))
		if err == nil {
			o.bytes, o.source, o.processValid = n, "linux_rss", true
		}
	}
	if !o.processValid {
		o.bytes = goBytes()
	}
	o.cgroup, o.high, o.low = p.cgroupObservation()
	return o
}

func (p *memoryProfile) cgroupObservation() (CgroupSnapshot, bool, bool) {
	snapshot := CgroupSnapshot{State: "unknown", Scope: "none", Levels: len(p.levels)}
	if p.changed {
		snapshot.State = "profile_changed"
		return snapshot, false, false
	}
	membership, err := readFile(filepath.Join(p.proc, "cgroup"), maxCgroupBytes)
	if err != nil {
		return snapshot, false, false
	}
	mounts, err := readFile(filepath.Join(p.proc, "mountinfo"), maxMountBytes)
	if err != nil {
		return snapshot, false, false
	}
	if p.levels != nil && (membership != p.membership || mounts != p.mounts) {
		p.changed = true
		snapshot.State = "profile_changed"
		return snapshot, false, false
	}
	if p.levels == nil {
		mount, names, err := locateCgroup(membership, mounts)
		if err != nil {
			return snapshot, false, false
		}
		p.mount = mount
		root, err := os.OpenRoot(mount)
		if err != nil {
			return snapshot, false, false
		}
		defer root.Close()
		levels := make([]memoryLevel, 0, len(names))
		for i, name := range names {
			level, _, err := readLevel(root, name)
			// The actual hierarchy root may have no memory controller interface.
			// Only omit a visible top ancestor, never a leaf or a partially missing pair.
			if err != nil || level.absent && (i == 0 || name != ".") {
				return snapshot, false, false
			}
			levels = append(levels, level)
		}
		p.levels, p.membership, p.mounts = levels, membership, mounts
	}
	root, err := os.OpenRoot(p.mount)
	if err != nil {
		return snapshot, false, false
	}
	defer root.Close()
	snapshot.Levels = len(p.levels)
	high, low := false, true
	for i, expected := range p.levels {
		level, current, err := readLevel(root, expected.name)
		if err != nil {
			return snapshot, false, false
		}
		if level != expected {
			p.changed = true
			snapshot.State = "profile_changed"
			return snapshot, false, false
		}
		if i == 0 {
			snapshot.Current, snapshot.Scope = current, "leaf"
		}
		if !level.finite {
			continue
		}
		high = high || level.limit == 0 || current >= watermark(level.limit, 80, true)
		low = low && level.limit != 0 && current <= watermark(level.limit, 70, false)
		if !snapshot.Finite || pressureGreater(current, level.limit, snapshot) {
			snapshot.Current, snapshot.Limit, snapshot.Finite = current, level.limit, true
			snapshot.Scope = "leaf"
			if i != 0 {
				snapshot.Scope = "ancestor"
			}
		}
	}
	snapshot.State, snapshot.Valid = "v2", true
	return snapshot, high, low
}

func pressureGreater(current, limit uint64, previous CgroupSnapshot) bool {
	if previous.Limit == 0 {
		return false
	}
	if limit == 0 {
		return true
	}
	a, b := bits.Mul64(current, previous.Limit)
	c, d := bits.Mul64(previous.Current, limit)
	return a > c || a == c && b > d
}

func readLevel(root *os.Root, name string) (memoryLevel, uint64, error) {
	level := memoryLevel{name: name}
	rawMax, maxErr := readRootFile(root, path.Join(name, "memory.max"))
	rawCurrent, currentErr := readRootFile(root, path.Join(name, "memory.current"))
	if errors.Is(maxErr, os.ErrNotExist) && errors.Is(currentErr, os.ErrNotExist) {
		level.absent = true
		return level, 0, nil
	}
	if maxErr != nil || currentErr != nil {
		return level, 0, errProfile
	}
	if strings.TrimSpace(rawMax) != "max" {
		n, err := parseNumber(rawMax)
		if err != nil {
			return level, 0, err
		}
		level.limit, level.finite = n, true
	}
	current, err := parseNumber(rawCurrent)
	return level, current, err
}

func readRootFile(root *os.Root, name string) (string, error) {
	file, err := root.Open(name)
	if err != nil {
		return "", err
	}
	defer file.Close()
	return readBounded(file, 64)
}

func readFile(name string, limit int64) (string, error) {
	file, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer file.Close()
	return readBounded(file, limit)
}

func readBounded(reader io.Reader, limit int64) (string, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return "", err
	}
	if int64(len(raw)) > limit {
		return "", errProfile
	}
	return string(raw), nil
}

func parseNumber(raw string) (uint64, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, errProfile
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return 0, errProfile
		}
	}
	return strconv.ParseUint(value, 10, 64)
}

func parseRSS(raw string, pageSize uint64) (uint64, error) {
	fields := strings.Fields(raw)
	if len(fields) != 7 || len(raw) > 256 || pageSize == 0 {
		return 0, errProfile
	}
	var resident uint64
	for i, field := range fields {
		n, err := parseNumber(field)
		if err != nil {
			return 0, err
		}
		if i == 1 {
			resident = n
		}
	}
	if resident > ^uint64(0)/pageSize {
		return 0, errProfile
	}
	return resident * pageSize, nil
}

func locateCgroup(membership, mounts string) (string, []string, error) {
	if len(membership) > maxCgroupBytes || len(mounts) > maxMountBytes {
		return "", nil, errProfile
	}
	lines := strings.Split(strings.TrimSpace(membership), "\n")
	if len(lines) > 64 {
		return "", nil, errProfile
	}
	leaf := ""
	for _, line := range lines {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			return "", nil, errProfile
		}
		if parts[0] == "0" && parts[1] == "" {
			if leaf != "" || !validPath(parts[2]) {
				return "", nil, errProfile
			}
			leaf = parts[2]
		}
	}
	if leaf == "" {
		return "", nil, errProfile
	}
	lines = strings.Split(strings.TrimSpace(mounts), "\n")
	if len(lines) > 1024 {
		return "", nil, errProfile
	}
	mount, mountRoot := "", ""
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 10 || len(fields) > 64 {
			return "", nil, errProfile
		}
		separator := -1
		for i := 6; i < len(fields); i++ {
			if fields[i] == "-" {
				separator = i
				break
			}
		}
		if separator < 0 || separator+4 != len(fields) {
			return "", nil, errProfile
		}
		if fields[separator+1] != "cgroup2" {
			continue
		}
		root, err := mountPath(fields[3])
		if err != nil {
			return "", nil, err
		}
		point, err := mountPath(fields[4])
		if err != nil {
			return "", nil, err
		}
		if leaf != root && !strings.HasPrefix(leaf, strings.TrimSuffix(root, "/")+"/") {
			continue
		}
		// Prefer the mount exposing the most ancestors. Never escape its root.
		if mount == "" || len(root) < len(mountRoot) {
			mount, mountRoot = point, root
		}
	}
	if mount == "" {
		return "", nil, errProfile
	}
	relative := strings.TrimPrefix(strings.TrimPrefix(leaf, mountRoot), "/")
	if relative == "" {
		relative = "."
	}
	var names []string
	for {
		if len(names) == maxLevels {
			return "", nil, errProfile
		}
		names = append(names, relative)
		if relative == "." {
			break
		}
		relative = path.Dir(relative)
	}
	return mount, names, nil
}

func validPath(value string) bool {
	if len(value) == 0 || len(value) > 4096 || !strings.HasPrefix(value, "/") || path.Clean(value) != value {
		return false
	}
	for _, c := range value {
		if c < 32 || c == 127 {
			return false
		}
	}
	return !strings.HasSuffix(value, " (deleted)")
}

func mountPath(raw string) (string, error) {
	var output strings.Builder
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			output.WriteByte(raw[i])
			continue
		}
		if i+4 > len(raw) {
			return "", errProfile
		}
		switch raw[i : i+4] {
		case "\\040":
			output.WriteByte(' ')
		case "\\134":
			output.WriteByte('\\')
		default:
			return "", errProfile
		}
		i += 3
	}
	value := output.String()
	if !validPath(value) {
		return "", errProfile
	}
	return value, nil
}
