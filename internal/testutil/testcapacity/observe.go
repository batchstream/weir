package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var observationEpoch = time.Now()

type Sample struct {
	Role        string            `json:"role"`
	MonotonicNS int64             `json:"monotonic_ns"`
	DurationNS  int64             `json:"duration_ns"`
	GOMAXPROCS  int               `json:"gomaxprocs,omitempty"`
	Time        time.Time         `json:"time"`
	Files       map[string]string `json:"files"`
	FD          int               `json:"fd"`
	Goroutines  int               `json:"goroutines,omitempty"`
	RSS         uint64            `json:"rss_bytes"`
	Metrics     string            `json:"metrics,omitempty"`
	DB          string            `json:"db,omitempty"`
	Errors      []string          `json:"errors,omitempty"`
}

func boundedFile(name string) (string, error) {
	f, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (256<<10)+1))
	if len(b) > 256<<10 {
		return "", fmt.Errorf("file bound")
	}
	return string(b), err
}
func sample(pid string) Sample {
	s := Sample{Time: time.Now(), Files: map[string]string{}}
	s.MonotonicNS = time.Since(observationEpoch).Nanoseconds()
	s.Role = "weir"
	if pid != "1" {
		s.Role = "es"
	}
	proc := "/proc/" + pid
	cg := proc + "/root/sys/fs/cgroup"
	for _, name := range []string{"memory.current", "memory.max", "memory.swap.max", "memory.events", "cpu.max", "cpu.stat", "cpuset.cpus.effective", "io.stat", "pids.current", "pids.max"} {
		value, err := boundedFile(filepath.Join(cg, name))
		if err != nil {
			s.Errors = append(s.Errors, name+":"+err.Error())
		} else {
			s.Files[name] = value
		}
	}
	for _, name := range []string{"status", "limits", "net/tcp", "net/tcp6"} {
		value, err := boundedFile(filepath.Join(proc, name))
		if err != nil {
			s.Errors = append(s.Errors, name+":"+err.Error())
		} else {
			s.Files[name] = value
		}
	}
	for _, line := range strings.Split(s.Files["status"], "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			n, _ := strconv.ParseUint(fields[1], 10, 64)
			s.RSS = n * 1024
		}
	}
	entries, err := os.ReadDir(proc + "/fd")
	if err != nil {
		s.Errors = append(s.Errors, err.Error())
	} else {
		s.FD = len(entries)
	}
	if pid == "self" {
		s.Goroutines = runtime.NumGoroutine()
		s.GOMAXPROCS = runtime.GOMAXPROCS(0)
		s.Role = "client"
	}
	s.DurationNS = time.Since(s.Time).Nanoseconds()
	return s
}
func executableHash(pid string) (string, error) {
	f, err := os.Open("/proc/" + pid + "/exe")
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	_, err = io.Copy(h, io.LimitReader(f, 128<<20))
	return hex.EncodeToString(h.Sum(nil)), err
}
func observeHTTP(ctx context.Context, c *Client, s *Sample) {
	code, raw, err := c.request(ctx, "GET", "/metrics", nil)
	if err != nil || code != 200 {
		s.Errors = append(s.Errors, fmt.Sprintf("metrics %d %v", code, err))
	} else {
		s.Metrics = string(raw)
	}
}
