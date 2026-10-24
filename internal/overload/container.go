package overload

import (
	"bufio"
	"bytes"
	"io/fs"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
)

// SetContainerMemoryLimit gives the Go runtime a soft memory limit of 90% of
// the process's cgroup v2 memory.max, leaving room for memory outside the Go
// heap. An explicit GOMEMLIMIT, including off, takes precedence. It returns the
// limit it set, or zero when it set none.
func SetContainerMemoryLimit() uint64 {
	if _, set := os.LookupEnv("GOMEMLIMIT"); set {
		return 0
	}
	limit := containerMemoryLimit(os.DirFS("/")) / 10 * 9
	if limit == 0 {
		return 0
	}
	debug.SetMemoryLimit(int64(limit))
	return limit
}

// containerMemoryLimit reads the memory.max of the process's own cgroup v2
// group. It returns zero when the limit is unset, unreadable or on cgroup v1.
func containerMemoryLimit(root fs.FS) uint64 {
	membership, err := fs.ReadFile(root, "proc/self/cgroup")
	if err != nil {
		return 0
	}
	group := ""
	scanner := bufio.NewScanner(bytes.NewReader(membership))
	for scanner.Scan() {
		if name, found := strings.CutPrefix(scanner.Text(), "0::"); found {
			group = name
			break
		}
	}
	if !strings.HasPrefix(group, "/") {
		return 0
	}
	raw, err := fs.ReadFile(root, path.Join("sys/fs/cgroup", group, "memory.max"))
	if err != nil {
		return 0
	}
	limit, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 63)
	if err != nil {
		return 0
	}
	return limit
}
