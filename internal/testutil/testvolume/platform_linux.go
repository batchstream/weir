//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

func supported() error { return nil }

func openPublic(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

func filesystem(root string, readonly bool) (filesystemProof, error) {
	result := filesystemProof{Mount: root}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(root, &stat); err != nil {
		return result, err
	}
	result.Magic = stat.Type
	result.Capacity = stat.Blocks * uint64(stat.Bsize)
	if result.Magic != 0x01021994 || result.Capacity != capacity {
		return result, errors.New("expected exact16MiB tmpfs")
	}
	raw, err := boundedRead("/proc/self/mountinfo")
	if err != nil {
		return result, err
	}
	return mountFacts(raw, readonly, result)
}

func mountFacts(raw string, readonly bool, result filesystemProof) (filesystemProof, error) {
	count := 0
	root := result.Mount
	result.NoSharedMemory = true
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 10 {
			return result, errors.New("malformed mountinfo")
		}
		mountOptions := "," + fields[5] + ","
		if fields[4] == "/" {
			result.RootReadonly = strings.Contains(mountOptions, ",ro,")
		}
		if fields[4] == "/weir-volume-helper" {
			result.HelperReadonly = strings.Contains(mountOptions, ",ro,")
		}
		if fields[4] == "/dev/shm" {
			result.NoSharedMemory = false
		}
		if fields[4] != root {
			continue
		}
		count++
		separator := -1
		for i, field := range fields {
			if field == "-" {
				separator = i
				break
			}
		}
		if separator < 6 || len(fields) < separator+4 || fields[separator+1] != "tmpfs" {
			return result, errors.New("mount is not tmpfs")
		}
		result.Device = fields[2]
		result.Options = strings.Split(fields[5], ",")
		options := "," + fields[5] + ","
		for _, option := range []string{"noexec", "nosuid", "nodev"} {
			if !strings.Contains(options, ","+option+",") {
				return result, errors.New("missing restrictive mount flag")
			}
		}
		access := "rw"
		if readonly {
			access = "ro"
		}
		if !strings.Contains(options, ","+access+",") {
			return result, errors.New("mount access mismatch")
		}
	}
	if count != 1 || !result.RootReadonly || !result.HelperReadonly || !result.NoSharedMemory {
		return result, errors.New("expected one exact config mount")
	}
	return result, nil
}

func startTicks(raw string) (uint64, error) {
	end := strings.LastIndex(raw, ")")
	if end < 0 {
		return 0, errors.New("invalid process stat")
	}
	fields := strings.Fields(raw[end+1:])
	if len(fields) <= 19 {
		return 0, errors.New("short process stat")
	}
	return strconv.ParseUint(fields[19], 10, 64)
}

func observeProcess() (processProof, error) {
	result := processProof{PID: 1, UID: os.Getuid(), GID: os.Getgid()}
	before, err := boundedRead("/proc/1/stat")
	if err != nil {
		return result, err
	}
	result.Start, err = startTicks(before)
	if err != nil {
		return result, err
	}
	result.Executable, err = os.Readlink("/proc/1/exe")
	if err != nil {
		return result, err
	}
	file, err := os.Open("/proc/1/exe")
	if err != nil {
		return result, err
	}
	result.SHA, _, err = hashFile(file, 128<<20)
	file.Close()
	if err != nil {
		return result, err
	}
	self, err := os.Open("/proc/self/exe")
	if err != nil {
		return result, err
	}
	selfSHA, _, err := hashFile(self, 128<<20)
	self.Close()
	if err != nil || selfSHA != result.SHA {
		return result, errors.New("observer is not known PID1 helper")
	}
	result.Membership, err = boundedRead("/proc/1/cgroup")
	if err != nil || result.Membership != "0::/" {
		return result, errors.New("PID1 private cgroup mapping required")
	}
	membership, err := boundedRead("/proc/self/cgroup")
	if err != nil || membership != result.Membership {
		return result, errors.New("observer/PID1 cgroups differ")
	}
	mounts, err := boundedRead("/proc/self/mountinfo")
	if err != nil {
		return result, err
	}
	mapped := false
	for _, line := range strings.Split(mounts, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 10 && fields[3] == "/" && fields[4] == "/sys/fs/cgroup" && strings.Contains(line, " - cgroup2 ") {
			mapped = true
		}
	}
	if !mapped {
		return result, errors.New("private unified cgroup root required")
	}
	result.CPU, err = boundedRead("/sys/fs/cgroup/cpu.max")
	if err != nil {
		return result, err
	}
	result.Memory, err = scalar("/sys/fs/cgroup/memory.max")
	if err != nil {
		return result, err
	}
	result.Swap, err = scalar("/sys/fs/cgroup/memory.swap.max")
	if err != nil {
		return result, err
	}
	result.PIDs, err = scalar("/sys/fs/cgroup/pids.max")
	if err != nil {
		return result, err
	}
	status, err := boundedRead("/proc/1/status")
	if err != nil {
		return result, err
	}
	identities := 0
	for _, line := range strings.Split(status, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "CapEff:":
			result.CapEff = fields[1]
		case "NoNewPrivs:":
			result.NoNewPrivs = fields[1]
		case "Uid:", "Gid:":
			identities++
			if strings.Join(fields[1:], ",") != "1000,1000,1000,1000" {
				return result, errors.New("PID1 nonroot identity mismatch")
			}
		}
	}
	if identities != 2 {
		return result, errors.New("PID1 UID/GID status missing")
	}
	after, err := boundedRead("/proc/1/stat")
	if err != nil {
		return result, err
	}
	ticks, err := startTicks(after)
	if err != nil || ticks != result.Start {
		return result, fmt.Errorf("PID1 replaced during observation")
	}
	return result, nil
}
