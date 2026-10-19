package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// controlsSnapshot deliberately excludes metrics, IO and sampler timing. The
// exec probe runs inside the observed cgroup and is charged to that container.
type controlsSnapshot struct {
	PID            int              `json:"pid"`
	StartTicks     int64            `json:"start_ticks"`
	Executable     string           `json:"executable"`
	SHA256         string           `json:"sha256"`
	ObserverSHA256 string           `json:"observer_sha256"`
	Membership     string           `json:"membership"`
	MountRoot      string           `json:"mount_root"`
	CPU            string           `json:"cpu_max"`
	Memory         int64            `json:"memory_max"`
	Swap           int64            `json:"memory_swap_max"`
	PIDs           int64            `json:"pids_max"`
	CPUCounters    map[string]int64 `json:"cpu"`
	MemoryCurrent  int64            `json:"memory_current"`
	MemoryPeak     int64            `json:"memory_peak"`
	MemoryEvents   map[string]int64 `json:"memory_events"`
}

type controlsOptions struct {
	Root, ExpectedSHA256 string
	PID                  int
	IdentityOnly         bool
}

func executableHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, (128<<20)+1))
	if err != nil || n > 128<<20 {
		return "", fmt.Errorf("executable read bound: %v", err)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func controlMapping(target, observer, mounts string) error {
	if target != "0::/" || observer != target {
		return fmt.Errorf("target/observer must share private cgroup root")
	}
	found := false
	for _, line := range strings.Split(mounts, "\n") {
		fields := strings.Fields(line)
		split := -1
		for i, field := range fields {
			if field == "-" {
				split = i
				break
			}
		}
		if split < 6 || split+3 >= len(fields) {
			return fmt.Errorf("malformed mountinfo")
		}
		if fields[split+1] != "cgroup2" {
			continue
		}
		if found || fields[3] != "/" || fields[4] != "/sys/fs/cgroup" || !strings.Contains(","+fields[5]+",", ",ro,") {
			return fmt.Errorf("cgroup mount mapping is not private read-only root")
		}
		found = true
	}
	if !found {
		return fmt.Errorf("missing unified cgroup mount")
	}
	return nil
}

func collectControls(opts controlsOptions) (controlsSnapshot, error) {
	sample := controlsSnapshot{PID: opts.PID, MountRoot: "/"}
	if opts.PID != 1 || (!opts.IdentityOnly && len(opts.ExpectedSHA256) != 64) || (opts.IdentityOnly && opts.ExpectedSHA256 != "") {
		return sample, fmt.Errorf("once requires PID1 and expected executable SHA256 (or identity-only baseline)")
	}
	proc := filepath.Join(opts.Root, "proc")
	target := filepath.Join(proc, "1")
	member, err := readFile(filepath.Join(target, "cgroup"))
	if err != nil {
		return sample, err
	}
	self, err := readFile(filepath.Join(proc, "self/cgroup"))
	if err != nil {
		return sample, err
	}
	mounts, err := readFile(filepath.Join(proc, "self/mountinfo"))
	if err != nil {
		return sample, err
	}
	if err := controlMapping(member, self, mounts); err != nil {
		return sample, err
	}
	sample.Membership = member
	// Namespace identity and the private-root mapping must both hold. A host PID
	// alone cannot establish which controls are visible to the observer.
	for _, ns := range []string{"cgroup", "pid"} {
		a, err := os.Stat(filepath.Join(target, "ns", ns))
		if err != nil {
			return sample, err
		}
		b, err := os.Stat(filepath.Join(proc, "self/ns", ns))
		if err != nil || !os.SameFile(a, b) {
			return sample, fmt.Errorf("target and observer namespace mismatch")
		}
	}
	sample.Executable, err = os.Readlink(filepath.Join(target, "exe"))
	if err != nil {
		return sample, err
	}
	sample.SHA256, err = executableHash(filepath.Join(target, "exe"))
	if err != nil || (!opts.IdentityOnly && sample.SHA256 != opts.ExpectedSHA256) {
		return sample, fmt.Errorf("PID1 executable digest mismatch: %v", err)
	}
	sample.ObserverSHA256, err = executableHash(filepath.Join(proc, "self/exe"))
	if err != nil {
		return sample, err
	}
	before, err := readFile(filepath.Join(target, "stat"))
	if err != nil {
		return sample, err
	}
	fields := strings.Fields(before[strings.LastIndex(before, ")")+1:])
	if len(fields) < 20 {
		return sample, fmt.Errorf("malformed PID1 stat")
	}
	sample.StartTicks, err = strconv.ParseInt(fields[19], 10, 64)
	if err != nil || sample.StartTicks <= 0 {
		return sample, fmt.Errorf("invalid process start identity")
	}
	if opts.IdentityOnly {
		return sample, nil
	}
	cg := filepath.Join(opts.Root, "sys/fs/cgroup")
	sample.CPU, err = readFile(filepath.Join(cg, "cpu.max"))
	if err != nil {
		return sample, err
	}
	cpu := strings.Fields(sample.CPU)
	if len(cpu) != 2 {
		return sample, fmt.Errorf("malformed CPU limit")
	}
	quota, err := strconv.ParseInt(cpu[0], 10, 64)
	if err != nil || quota <= 0 {
		return sample, fmt.Errorf("unlimited/invalid CPU quota")
	}
	period, err := strconv.ParseInt(cpu[1], 10, 64)
	if err != nil || period <= 0 || period > 1000000 || quota/period != 2 || quota%period != 0 {
		return sample, fmt.Errorf("expected exactly two CPUs")
	}
	for name, destination := range map[string]*int64{"memory.max": &sample.Memory, "memory.swap.max": &sample.Swap, "pids.max": &sample.PIDs, "memory.current": &sample.MemoryCurrent, "memory.peak": &sample.MemoryPeak} {
		*destination, err = scalar(filepath.Join(cg, name))
		if err != nil || *destination < 0 {
			return sample, fmt.Errorf("invalid bounded control %s", name)
		}
	}
	if sample.Memory <= 0 || sample.PIDs <= 0 || sample.Swap != 0 {
		return sample, fmt.Errorf("unlimited controls or extra swap")
	}
	sample.CPUCounters, err = counters(filepath.Join(cg, "cpu.stat"))
	if err != nil {
		return sample, err
	}
	sample.MemoryEvents, err = counters(filepath.Join(cg, "memory.events"))
	if err != nil {
		return sample, err
	}
	for _, item := range []struct {
		Values map[string]int64
		Names  []string
	}{
		{sample.CPUCounters, []string{"usage_usec", "nr_periods", "nr_throttled", "throttled_usec"}},
		{sample.MemoryEvents, []string{"low", "high", "max", "oom", "oom_kill"}},
	} {
		for _, name := range item.Names {
			value, ok := item.Values[name]
			if !ok || value < 0 {
				return sample, fmt.Errorf("missing/invalid counter %s", name)
			}
		}
	}
	after, err := readFile(filepath.Join(target, "stat"))
	if err != nil || before != after {
		// CPU fields can legitimately change, but the start identity must not.
		f := strings.Fields(after[strings.LastIndex(after, ")")+1:])
		if err != nil || len(f) < 20 || f[19] != fields[19] {
			return sample, fmt.Errorf("PID1 identity changed during observation")
		}
	}
	digest, err := executableHash(filepath.Join(target, "exe"))
	if err != nil || digest != sample.SHA256 {
		return sample, fmt.Errorf("PID1 executable changed during observation")
	}
	return sample, nil
}
