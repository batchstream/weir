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
// Each complete observation discovers the current hierarchy and memory limits.
type memoryProfile struct {
	proc     string
	identity cgroupIdentity
	levels   []memoryLevel
}

// Mount ID and device distinguish replacement mounts at the same path. The
// mapping and membership determine the visible relative hierarchy.
type cgroupIdentity struct {
	leaf, root, mount string
	id, major, minor  uint64
}

func (p *memoryProfile) observe(limits Limits) observation {
	if p.proc == "" {
		return processObservation()
	}
	cg := CgroupSnapshot{State: "not_applicable", Scope: "none"}
	o := observation{source: "go_sys_minus_released", cgroup: cg}
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
	o.cgroup, o.high, o.low = p.cgroupObservation(limits)
	return o
}

func (p *memoryProfile) cgroupObservation(limits Limits) (CgroupSnapshot, bool, bool) {
	snapshot := CgroupSnapshot{State: "unknown", Scope: "none", Levels: len(p.levels)}
	membership, err := readFile(filepath.Join(p.proc, "cgroup"), maxCgroupBytes)
	if err != nil {
		return snapshot, false, false
	}
	mounts, err := readFile(filepath.Join(p.proc, "mountinfo"), maxMountBytes)
	if err != nil {
		return snapshot, false, false
	}
	identity, names, err := locateCgroup(membership, mounts)
	if err != nil {
		return snapshot, false, false
	}
	root, err := os.OpenRoot(identity.mount)
	if err != nil {
		return snapshot, false, false
	}
	defer root.Close()
	levels := make([]memoryLevel, 0, len(names))
	snapshot.Levels = len(names)
	high, low := false, true
	for i, name := range names {
		level, current, err := readLevel(root, name)
		// Only the visible top ancestor may lack both memory interfaces.
		if err != nil || level.absent && (i == 0 || name != ".") {
			return snapshot, false, false
		}
		// A previously readable top pair disappearing is missing evidence, not a
		// confirmed unlimited/controller change. Preserve the trusted profile.
		if level.absent && p.levels != nil && identity == p.identity && i < len(p.levels) && !p.levels[i].absent {
			return snapshot, false, false
		}
		levels = append(levels, level)
		if i == 0 {
			snapshot.Current, snapshot.Scope = current, "leaf"
		}
		if !level.finite {
			continue
		}
		snapshot.Capacity = smallerBudget(snapshot.Capacity, level.limit)
		high = high || level.limit == 0 || current >= watermark(level.limit, limits.HighWatermark, true)
		low = low && level.limit != 0 && current <= watermark(level.limit, limits.LowWatermark, false)
		if !snapshot.Finite || pressureGreater(current, level.limit, snapshot) {
			snapshot.Current, snapshot.Limit, snapshot.Finite = current, level.limit, true
			snapshot.Scope = "leaf"
			if i != 0 {
				snapshot.Scope = "ancestor"
			}
		}
	}
	// Replace the profile only after the entire hierarchy validates.
	p.identity, p.levels = identity, levels
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

func locateCgroup(membership, mounts string) (cgroupIdentity, []string, error) {
	var selected cgroupIdentity
	if len(membership) > maxCgroupBytes || len(mounts) > maxMountBytes {
		return selected, nil, errProfile
	}
	lines := strings.Split(strings.TrimSpace(membership), "\n")
	if len(lines) > 64 {
		return selected, nil, errProfile
	}
	leaf := ""
	for _, line := range lines {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			return selected, nil, errProfile
		}
		hierarchy, err := parseNumber(parts[0])
		if err != nil || !validPath(parts[2]) || (hierarchy == 0) != (parts[1] == "") {
			return selected, nil, errProfile
		}
		if hierarchy == 0 {
			if leaf != "" {
				return selected, nil, errProfile
			}
			leaf = parts[2]
		}
	}
	if leaf == "" {
		return selected, nil, errProfile
	}
	lines = strings.Split(strings.TrimSpace(mounts), "\n")
	if len(lines) > 1024 {
		return selected, nil, errProfile
	}
	ids := make(map[uint64]bool)
	points := make(map[string]bool)
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 10 || len(fields) > 64 {
			return selected, nil, errProfile
		}
		separator := -1
		for i := 6; i < len(fields); i++ {
			if fields[i] == "-" {
				separator = i
				break
			}
		}
		if separator < 0 || separator+4 != len(fields) {
			return selected, nil, errProfile
		}
		id, err := parseNumber(fields[0])
		if err != nil || id == 0 || ids[id] {
			return selected, nil, errProfile
		}
		ids[id] = true
		if _, err := parseNumber(fields[1]); err != nil {
			return selected, nil, errProfile
		}
		device := strings.Split(fields[2], ":")
		if len(device) != 2 {
			return selected, nil, errProfile
		}
		major, err := parseNumber(device[0])
		if err != nil {
			return selected, nil, err
		}
		minor, err := parseNumber(device[1])
		if err != nil {
			return selected, nil, err
		}
		if fields[separator+1] != "cgroup2" {
			continue
		}
		root, err := mountPath(fields[3])
		if err != nil {
			return selected, nil, err
		}
		point, err := mountPath(fields[4])
		if err != nil {
			return selected, nil, err
		}
		if leaf != root && !strings.HasPrefix(leaf, strings.TrimSuffix(root, "/")+"/") {
			continue
		}
		// Stacked applicable mounts at one path are outside this static profile.
		if points[point] {
			return selected, nil, errProfile
		}
		points[point] = true
		// Prefer greatest ancestor visibility, then lexical mount point. Text order,
		// optional propagation fields and unrelated records do not define identity.
		if selected.mount == "" || len(root) < len(selected.root) || len(root) == len(selected.root) && point < selected.mount {
			selected = cgroupIdentity{leaf: leaf, root: root, mount: point, id: id, major: major, minor: minor}
		}
	}
	if selected.mount == "" {
		return selected, nil, errProfile
	}
	relative := strings.TrimPrefix(strings.TrimPrefix(leaf, selected.root), "/")
	if relative == "" {
		relative = "."
	}
	var names []string
	for {
		if len(names) == maxLevels {
			return selected, nil, errProfile
		}
		names = append(names, relative)
		if relative == "." {
			break
		}
		relative = path.Dir(relative)
	}
	return selected, names, nil
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
